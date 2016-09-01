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

import java.nio.ByteBuffer;

import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

/**
 * Functions for constructing YARN objects from the parameter values.
 */
public class ContainerLaunchContextFactory {

  private final Resource clusterMax;
  private final ByteBuffer allTokens;

  public ContainerLaunchContextFactory(Resource clusterMax, ByteBuffer allTokens) {
    this.clusterMax = clusterMax;
    this.allTokens = allTokens;
  }
  
  public ContainerLaunchContext create(ContainerLaunchParameters parameters) {
    ContainerLaunchContext clc = Records.newRecord(ContainerLaunchContext.class);
    clc.setCommands(parameters.getCommands());
    clc.setEnvironment(parameters.getEnvironment());
    clc.setLocalResources(parameters.getLocalResources());
    if (allTokens != null) {
      clc.setTokens(allTokens.duplicate());
    }
    return clc;
  }

  public String getNodeLabelExpression(ContainerLaunchParameters parameters) {
    return parameters.getNodeLabelsExpression();
  }
  
  public String getNodeLabelExpression(ContainerLaunchParameters parameters) {
	  return parameters.getNodeLabelsExpression();
  }
  
  public Resource createResource(ContainerLaunchParameters parameters) {
    return parameters.getContainerResource(clusterMax);
  }
  
  public Priority createPriority(int priority) {
    Priority p = Records.newRecord(Priority.class);
    p.setPriority(priority);
    return p;
  }
}
