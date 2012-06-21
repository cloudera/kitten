/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.kitten.appmaster.service;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.Records;

import com.cloudera.kitten.ContainerLaunchContextFactory;
import com.cloudera.kitten.ContainerLaunchParameters;
import com.cloudera.kitten.MasterConnectionFactory;
import com.cloudera.kitten.appmaster.ApplicationMasterParameters;
import com.cloudera.kitten.appmaster.ApplicationMasterService;
import com.cloudera.kitten.appmaster.ContainerManagerConnectionFactory;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractScheduledService;

/**
 *
 */
public class ApplicationMasterServiceImpl extends
    AbstractScheduledService implements ApplicationMasterService {

  private static final Log LOG = LogFactory.getLog(ApplicationMasterServiceImpl.class);

  private final ApplicationMasterParameters parameters;
  private final MasterConnectionFactory<AMRMProtocol> resourceManagerFactory;
  private final ContainerManagerConnectionFactory containerManagerFactory;
  private final List<ContainerTracker> containerTrackers;
  private final AtomicInteger newRequestId = new AtomicInteger();
  private final AtomicInteger totalFailures = new AtomicInteger();
  
  private AMRMProtocol resourceManager;
  private ContainerLaunchContextFactory containerLaunchContextFactory;
  
  public ApplicationMasterServiceImpl(ApplicationMasterParameters params) {
    this(params, new ResourceManagerConnectionFactory(params.getConfiguration()),
        new ContainerManagerConnectionFactoryImpl(params.getConfiguration()));
  }
  
  public ApplicationMasterServiceImpl(ApplicationMasterParameters parameters,
      MasterConnectionFactory<AMRMProtocol> resourceManagerFactory,
      ContainerManagerConnectionFactory containerManagerFactory) {
    this.parameters = Preconditions.checkNotNull(parameters);
    this.resourceManagerFactory = resourceManagerFactory;
    this.containerManagerFactory = containerManagerFactory;
    this.containerTrackers = Lists.newArrayList();
  }

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    return parameters.getApplicationAttemptId();
  }
  
  @Override
  protected void startUp() {
    this.resourceManager = resourceManagerFactory.connect();
    
    RegisterApplicationMasterResponse registration = null;
    try {
      registration = resourceManager.registerApplicationMaster(
          createRegistrationRequest());
    } catch (YarnRemoteException e) {
      LOG.error("Exception thrown registering application master", e);
      stop();
      return;
    }
    
    this.containerLaunchContextFactory = new ContainerLaunchContextFactory(
        registration.getMinimumResourceCapability(), registration.getMaximumResourceCapability());
    
    List<ContainerLaunchParameters> containerParameters = parameters.getContainerLaunchParameters();
    if (containerParameters.isEmpty()) {
      LOG.warn("No container configurations specified");
      stop();
      return;
    }
    for (int i = 0; i < containerParameters.size(); i++) {
      ContainerLaunchParameters clp = containerParameters.get(i);
      containerTrackers.add(new ContainerTracker(clp));
    }
  }
  
  private AMResponse allocate(int id, ResourceRequest request, List<ContainerId> releases) {
    AllocateRequest req = Records.newRecord(AllocateRequest.class);
    req.setResponseId(id);
    req.setApplicationAttemptId(parameters.getApplicationAttemptId());
    req.addAsk(request);
    req.addAllReleases(releases);
    try {
      return resourceManager.allocate(req).getAMResponse();
    } catch (YarnRemoteException e) {
      LOG.warn("Exception thrown during resource request", e);
      return Records.newRecord(AllocateResponse.class).getAMResponse();
    }
  }
  
  private RegisterApplicationMasterRequest createRegistrationRequest() {
    RegisterApplicationMasterRequest req = Records.newRecord(
        RegisterApplicationMasterRequest.class);
    req.setApplicationAttemptId(parameters.getApplicationAttemptId());
    req.setHost(parameters.getHostname());
    req.setRpcPort(parameters.getClientPort());
    req.setTrackingUrl(parameters.getTrackingUrl());
    return req;
  }
  
  @Override
  protected void shutDown() {
    // Stop the containers in the case that we're finishing because of a timeout.
    LOG.info("Stopping trackers");
    for (ContainerTracker tracker : containerTrackers) {
      tracker.stopServices();
    }
    
    FinishApplicationMasterRequest finishReq = Records.newRecord(
        FinishApplicationMasterRequest.class);
    finishReq.setAppAttemptId(getApplicationAttemptId());
    if (state() == State.FAILED || totalFailures.incrementAndGet() > parameters.getAllowedFailures()) {
      //TODO: diagnostics
      finishReq.setFinishApplicationStatus(FinalApplicationStatus.FAILED);
    } else {
      finishReq.setFinishApplicationStatus(FinalApplicationStatus.SUCCEEDED);
    }
    LOG.info("Sending finish request with status = " + finishReq.getFinalApplicationStatus());
    try {
      resourceManager.finishApplicationMaster(finishReq);
    } catch (YarnRemoteException e) {
      LOG.error("Error finishing application master", e);
    }
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(0, 1, TimeUnit.SECONDS);
  }
  
  @Override
  protected void runOneIteration() throws Exception {
    if (totalFailures.get() > parameters.getAllowedFailures()) {
      stop();
      return;
    }
    
    boolean moreWork = false;
    for (ContainerTracker tracker : containerTrackers) {
      moreWork |= tracker.doWork();
    }
    if (!moreWork) {
      stop();
    }
  }
  
  private class ContainerTracker {
    private final ContainerLaunchParameters parameters;
    private final int needed;
    private final Map<ContainerId, ContainerService> services;
    private final Map<ContainerId, ContainerStatus> amStatus;
    
    private int requested = 0;
    private int completed = 0;
    private boolean stopping = false;
    
    public ContainerTracker(ContainerLaunchParameters parameters) {
      this.parameters = parameters;
      this.needed = parameters.getNumInstances();
      this.services = Maps.newHashMapWithExpectedSize(needed);
      this.amStatus = Maps.newHashMapWithExpectedSize(needed);
    }
    
    public boolean doWork() {
      if (shouldWork()) {
        AMResponse resp = allocate(newRequestId.incrementAndGet(), createRequest(), getReleases()); 
        handleAllocation(resp);
        completed = checkContainerStatuses(resp);
      }
      return shouldWork();
    }
    
    private boolean shouldWork() {
      return !stopping && completed < needed;
    }
    
    private ResourceRequest createRequest() {
      ResourceRequest req = containerLaunchContextFactory.createResourceRequest(parameters);
      req.setNumContainers(needed - requested);
      if (requested < needed) {
        requested = needed;
      }
      return req;
    }
    
    private List<ContainerId> getReleases() {
      // TODO: empty for now, but perhaps something for the future.
      return ImmutableList.of();
    }
    
    private void handleAllocation(AMResponse resp) {
      List<Container> newContainers = resp.getAllocatedContainers();
      for (Container container : newContainers) {
        if (!services.containsKey(container.getId())) {
          ContainerService cs = new ContainerService(container, parameters);
          services.put(container.getId(), cs);
          cs.start();
        } else {
          LOG.warn("Already have running service for container: " + container.getId().getId());
        }
      }
    }

    private int checkContainerStatuses(AMResponse resp) {
      for (ContainerStatus status : resp.getCompletedContainersStatuses()) {
        amStatus.put(status.getContainerId(), status);
      }

      int complete = 0;
      Set<ContainerId> failed = Sets.newHashSet();
      for (ContainerId containerId : services.keySet()) {
        if (amStatus.containsKey(containerId)) {
          int exitStatus = amStatus.get(containerId).getExitStatus();
          if (exitStatus == 0) {
            LOG.debug("Container completed cleanly");
            complete++;
          } else {
            LOG.info("Container failed with exit code = " + exitStatus);
            failed.add(containerId);
          }
        }
      }
      
      if (!failed.isEmpty()) {
        totalFailures.addAndGet(failed.size());
        requested -= failed.size();
        for (ContainerId failedId : failed) {
          // Placeholder value so we don't attempt to duplicate a new job in
          // this container as part of a response from the AM.
          services.put(failedId, null);
        }
      }
      return complete;
    }
    
    public void stopServices() {
      this.stopping = true;
      for (ContainerService service : services.values()) {
        if (service != null) {
          service.stop();
        }
      }
    }
  }
  
  private class ContainerService extends AbstractScheduledService {

    private final Container container;
    private final ContainerLaunchParameters params;
    
    private ContainerManager containerManager;
    private ContainerStatus status;
    private int failedStatusChecks;
    
    public ContainerService(Container container, ContainerLaunchParameters params) {
      this.container = container;
      this.params = params;
    }
    
    @Override
    protected void startUp() throws Exception {
      ContainerLaunchContext ctxt = containerLaunchContextFactory.create(params);
      ctxt.setContainerId(container.getId());
      ctxt.setResource(container.getResource());

      this.containerManager = containerManagerFactory.connect(container);
      if (containerManager == null) {
        LOG.error("Could not connect to container manager for : " + container);
        stop();
      }
      
      StartContainerRequest startReq = Records.newRecord(StartContainerRequest.class);
      startReq.setContainerLaunchContext(ctxt);
      LOG.info("Starting container: " + container.getId());
      try {
        containerManager.startContainer(startReq);
      } catch (YarnRemoteException e) {
        LOG.error("Exception starting container: " + container, e);
        stop();
      }
    }

    @Override
    protected Scheduler scheduler() {
      return Scheduler.newFixedRateSchedule(10, 10, TimeUnit.SECONDS);
    }

    @Override
    protected void runOneIteration() throws Exception {
      GetContainerStatusRequest req = Records.newRecord(GetContainerStatusRequest.class);
      req.setContainerId(container.getId());
      try {
        GetContainerStatusResponse resp = containerManager.getContainerStatus(req);
        this.status = resp.getStatus();
      } catch (YarnRemoteException e) {
        LOG.warn("Exception getting container status", e);
        failedStatusChecks++;
        // TODO: configure this
        if (status == null || failedStatusChecks == 3) {
          stop();
          return;
        }
      }
      
      if (status != null) {
        LOG.info("Current container status: " + status.getState());
      }
      if (status != null && status.getState() == ContainerState.COMPLETE) {
        stop();
      }
    }

    @Override
    protected void shutDown() throws Exception {
      if (status != null && status.getState() != ContainerState.COMPLETE) {
        // We need to explicitly release the container.
        LOG.info("Stopping container: " + container.getId());
        StopContainerRequest req = Records.newRecord(StopContainerRequest.class);
        req.setContainerId(container.getId());
        try {
          containerManager.stopContainer(req);
        } catch (YarnRemoteException e) {
          LOG.warn("Exception thrown stopping container: " + container, e);
        }
      }
    }
  }
}
