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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * The parameters that are common to launching both application masters and node tasks via
 * a {@code ContainerLaunchContext} instance.
 */
public interface ContainerLaunchParameters {
  /**
   * The user to run the container as.
   */
  String getUser();

  /**
   * The requested memory for this container in megabytes.
   */
  int getMemory();
  
  /**
   * Returns the resources needed for this job, using the cluster min and max as bounds.
   */
  Resource getContainerResource(Resource clusterMin, Resource clusterMax);
  
  /**
   * The requested priority for the container.
   */
  int getPriority();
  
  /**
   * The number of instances of this container to launch.
   */
  int getNumInstances();
  
  /**
   * The local resources for the application in the container.
   */
  Map<String, LocalResource> getLocalResources();
  
  /**
   * The environment variables for the container.
   */
  Map<String, String> getEnvironment();

  /**
   * The commands to execute that start the application within the container.
   */
  List<String> getCommands();
}
