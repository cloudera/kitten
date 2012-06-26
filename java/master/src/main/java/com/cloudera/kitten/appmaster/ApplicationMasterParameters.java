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
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;

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
   * Returns the attempt ID for this application.
   */
  ApplicationAttemptId getApplicationAttemptId();
  
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
   * Sets the hostname the master is running on. This information is communicated to the
   * resource manager and is then passed along to the client by YARN.
   */
  ApplicationMasterParameters setHostname(String hostname);
  
  /**
   * Returns the hostname that was set for this application master.
   */
  String getHostname();
  
  /**
   * Sets the port the master is listening on for client requests. This information is
   * communicated to the resource manager and is then passed along to the client by YARN.
   */
  ApplicationMasterParameters setClientPort(int port);
  
  /**
   * Returns the client port that was set for this application master.
   */
  int getClientPort();
  
  /**
   * Sets a tracking URL for the client. If it is not specified, the combination of the
   * hostname and the client port will be used.
   */
  ApplicationMasterParameters setTrackingUrl(String url);

  /**
   * Returns the tracking URL that was set for this application master.
   */
  String getTrackingUrl();  
}
