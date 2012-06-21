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
package com.cloudera.kitten;

import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.util.Records;

/**
 * Functions for constructing YARN objects from the parameter values.
 */
public class ContainerLaunchContextFactory {

  private final Resource clusterMin;
  private final Resource clusterMax;
  
  public ContainerLaunchContextFactory(Resource clusterMin, Resource clusterMax) {
    this.clusterMin = clusterMin;
    this.clusterMax = clusterMax;
  }
  
  public ContainerLaunchContext create(ContainerLaunchParameters parameters) {
    ContainerLaunchContext clc = Records.newRecord(ContainerLaunchContext.class);
    clc.setCommands(parameters.getCommands());
    clc.setEnvironment(parameters.getEnvironment());
    clc.setLocalResources(parameters.getLocalResources());
    clc.setResource(parameters.getContainerResource(clusterMin, clusterMax));
    clc.setUser(parameters.getUser());
    return clc;
  }
  
  public ResourceRequest createResourceRequest(ContainerLaunchParameters parameters) {
    ResourceRequest req = Records.newRecord(ResourceRequest.class);
    req.setCapability(parameters.getContainerResource(clusterMin, clusterMax));
    req.setPriority(createPriority(parameters.getPriority()));
    req.setNumContainers(parameters.getNumInstances());
    req.setHostName("*"); // TODO: get smarter about this.
    return req;
  }
  
  public static Priority createPriority(int priority) {
    Priority p = Records.newRecord(Priority.class);
    p.setPriority(priority);
    return p;
  }
}
