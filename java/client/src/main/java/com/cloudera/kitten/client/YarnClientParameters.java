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
package com.cloudera.kitten.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.cloudera.kitten.ContainerLaunchParameters;

/**
 * The information that the {@code YarnClientService} needs to know in order to manage
 * setting up a YARN application from the client.
 */
public interface YarnClientParameters {

  /**
   * The name of this YARN application.
   */
  String getApplicationName();
  
  /**
   * The queue the application master is assigned to run in.
   */
  String getQueue();
  
  /**
   * Returns the container configuration info for launching the application master.
   */
  ContainerLaunchParameters getApplicationMasterParameters(ApplicationId applicationId);
  
  /**
   * The maximum length of time the client will allow the application to run, in milliseconds.
   * If < 0, there is no limit on the application's run time.
   */
  long getClientTimeoutMillis();
  
  /**
   * Return the configuration object used in constructing these parameters.
   * @return
   */
  Configuration getConfiguration();
}
