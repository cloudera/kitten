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
package com.cloudera.kitten.client.service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.cloudera.kitten.ContainerLaunchContextFactory;
import com.cloudera.kitten.ContainerLaunchParameters;
import com.cloudera.kitten.MasterConnectionFactory;
import com.cloudera.kitten.client.YarnClientParameters;
import com.cloudera.kitten.client.YarnClientService;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.AbstractScheduledService;

/**
 * A basic implementation of a YARN Client service.
 */
public class YarnClientServiceImpl extends AbstractScheduledService
    implements YarnClientService {

  private static final Set<YarnApplicationState> DONE = EnumSet.of(
      YarnApplicationState.FAILED, YarnApplicationState.FINISHED,
      YarnApplicationState.KILLED);
  
  private static final Log LOG = LogFactory.getLog(YarnClientServiceImpl.class);
  
  private final YarnClientParameters parameters;
  private final MasterConnectionFactory<YarnClient> yarnClientFactory;
  private final Stopwatch stopwatch;
  
  private YarnClient yarnClient;
  private ApplicationId applicationId;
  private ApplicationReport finalReport;
  private boolean timeout = false;
  
  public YarnClientServiceImpl(YarnClientParameters params) {
    this(params, new YarnClientFactory(params.getConfiguration()),
        new Stopwatch());
  }
  
  public YarnClientServiceImpl(YarnClientParameters parameters,
      MasterConnectionFactory<YarnClient> yarnClientFactory,
      Stopwatch stopwatch) {
    this.parameters = Preconditions.checkNotNull(parameters);
    this.yarnClientFactory = yarnClientFactory;
    this.stopwatch = stopwatch;
  }
  
  @Override
  protected void startUp() throws IOException {
    ByteBuffer serializedTokens = null;
    if (UserGroupInformation.isSecurityEnabled()) {
      Configuration conf = this.yarnClientFactory.getConfig();
      FileSystem fs = FileSystem.get(conf);
      Credentials credentials = new Credentials();
      String tokenRenewer = this.yarnClientFactory.getConfig().get(YarnConfiguration.RM_PRINCIPAL);
      if (tokenRenewer == null || tokenRenewer.length() == 0) {
        throw new IOException("Can't get Master Kerberos principal for the RM to use as renewer");
      }
      // For now, only getting tokens for the default file-system.
      final Token<?> tokens[] = fs.addDelegationTokens(tokenRenewer, credentials);
      if (tokens != null) {
        for (Token<?> token : tokens) {
          LOG.info("Got delegation token for " + fs.getUri() + "; " + token);
        }
      }
      DataOutputBuffer dob = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(dob);
      serializedTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    }

    this.yarnClient = yarnClientFactory.connect();
    YarnClientApplication clientApp = getNewApplication();
    GetNewApplicationResponse newApp = clientApp.getNewApplicationResponse();
    ContainerLaunchContextFactory clcFactory = new ContainerLaunchContextFactory(
        newApp.getMaximumResourceCapability(),
        serializedTokens);
    
    ApplicationSubmissionContext appContext = clientApp.getApplicationSubmissionContext();
    this.applicationId = appContext.getApplicationId();
    appContext.setApplicationName(parameters.getApplicationName());

    // Setup the container for the application master.
    ContainerLaunchParameters appMasterParams = parameters.getApplicationMasterParameters(applicationId);
    ContainerLaunchContext clc = clcFactory.create(appMasterParams);
    LOG.debug("Master context: " + clc);
    appContext.setResource(clcFactory.createResource(appMasterParams));
    appContext.setQueue(parameters.getQueue());
    appContext.setPriority(clcFactory.createPriority(appMasterParams.getPriority()));
    appContext.setAMContainerSpec(clc);
    submitApplication(appContext);
    
    // Make sure we stop the application in the case that it isn't done already.
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        if (YarnClientServiceImpl.this.isRunning()) {
          YarnClientServiceImpl.this.stop();
        }
      }
    });
    
    stopwatch.start();
  }
  
  private void submitApplication(ApplicationSubmissionContext appContext) {
    LOG.info("Submitting application to the applications manager");
    try {
      yarnClient.submitApplication(appContext);
    } catch (YarnException e) {
      LOG.error("Exception thrown submitting application", e);
      stop();
    } catch (IOException e) {
      LOG.error("IOException thrown submitting application", e);
      stop();
    }
  }
  
  private YarnClientApplication getNewApplication() {
    try {
      return yarnClient.createApplication();
    } catch (YarnException e) {
      LOG.error("Exception thrown getting new application", e);
      stop();
      return null;
    } catch (IOException e) {
      stop();
      return null;
    }
  }
  
  @Override
  protected void shutDown() {
    if (finalReport != null) {
      YarnApplicationState state = finalReport.getYarnApplicationState();
      FinalApplicationStatus status = finalReport.getFinalApplicationStatus();
      String diagnostics = finalReport.getDiagnostics();
      if (YarnApplicationState.FINISHED == state) {
        if (FinalApplicationStatus.SUCCEEDED == status) {
          LOG.info("Application completed successfully.");
        }
        else {
          LOG.info("Application finished unsuccessfully."
              + " State = " + state.toString() + ", FinalStatus = " + status.toString());
        }             
      }
      else if (YarnApplicationState.KILLED == state 
          || YarnApplicationState.FAILED == state) {
        LOG.info("Application did not complete successfully."
            + " State = " + state.toString() + ", FinalStatus = " + status.toString());
        if (diagnostics != null) {
          LOG.info("Diagnostics = " + diagnostics);
        }
      }
    } else {
      // Otherwise, we need to kill the application, if it was created.
      if (applicationId != null) {
        LOG.info("Killing application id = " + applicationId);
        try {
          yarnClient.killApplication(applicationId);
        } catch (YarnException e) {
          LOG.error("Exception thrown killing application", e);
        } catch (IOException e) {
          LOG.error("IOException thrown killing application", e);
        }
        LOG.info("Application was killed.");
      }
    }
  }
  
  @Override
  public YarnClientParameters getParameters() {
    return parameters;
  }
  
  @Override
  public ApplicationId getApplicationId() {
    return applicationId;
  }

  @Override
  public boolean isApplicationFinished() {
    return timeout || finalReport != null;
  }
  
  @Override
  public ApplicationReport getFinalReport() {
    if (!timeout && finalReport == null) {
      finalReport = getApplicationReport();
    }
    return finalReport;
  }
  
  @Override
  public ApplicationReport getApplicationReport() {
    try {
      return yarnClient.getApplicationReport(applicationId);
    } catch (YarnException e) {
      LOG.error("Exception occurred requesting application report", e);
      return null;
    } catch (IOException e) {
      LOG.error("IOException occurred requesting application report", e);
      return null;
    }
  }
  
  @Override
  protected void runOneIteration() throws Exception {
    if (isApplicationFinished()) {
      LOG.info("Nothing to do, application is finished");
      return;
    }

    ApplicationReport report = getApplicationReport();
    if (report == null) {
      LOG.error("No application report received");
    } else if (DONE.contains(report.getYarnApplicationState()) ||
        report.getFinalApplicationStatus() != FinalApplicationStatus.UNDEFINED) {
      finalReport = report;
      stop();
    }
    
    // Ensure that we haven't been running for all that long.
    if (parameters.getClientTimeoutMillis() > 0 &&
        stopwatch.elapsedMillis() > parameters.getClientTimeoutMillis()) {
      LOG.warn("Stopping application due to timeout.");
      timeout = true;
      stop();
    }
  }

  @Override
  protected Scheduler scheduler() {
    // TODO: make this configurable
    return Scheduler.newFixedRateSchedule(0, 1, TimeUnit.SECONDS);
  }

}
