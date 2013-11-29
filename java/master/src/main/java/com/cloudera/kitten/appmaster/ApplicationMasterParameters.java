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
package com.cloudera.kitten.appmaster;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.kitten.ContainerLaunchParameters;

/**
 * The information that the {@code ApplicationMasterService} needs to know in order to
 * setup and manage a YARN application.
 */
public interface ApplicationMasterParameters {

  /**
   * Returns the {@code Configuration} instance that should be used for this run.
   */
  Configuration getConfiguration();
  
  /**
   * Returns the parameters that will be used to launch the child containers for
   * this application.
   */
  List<ContainerLaunchParameters> getContainerLaunchParameters();
  
  /**
   * Returns the number of containers that are allowed to fail before this
   * application shuts itself down automatically.
   */
  int getAllowedFailures();

  /**
   * Returns the hostname that was set for this application master.
   */
  String getHostname();

  /**
   * TODO
   * @param clientPort
   */
  void setClientPort(int clientPort);

  /**
   * Returns the client port that was set for this application master.
   */
  int getClientPort();

  /**
   * TODO
   * @param trackingUrl
   */
  void setTrackingUrl(String trackingUrl);

  /**
   * Returns the tracking URL that was set for this application master.
   */
  String getTrackingUrl();  
}
