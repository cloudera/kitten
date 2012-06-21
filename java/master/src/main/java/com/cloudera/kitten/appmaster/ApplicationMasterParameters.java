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
 * The information that the {@link ApplicationMasterService} needs to know in order to
 * setup and manage a YARN application.
 */
public interface ApplicationMasterParameters {

  Configuration getConfiguration();
  
  ApplicationAttemptId getApplicationAttemptId();
  
  List<ContainerLaunchParameters> getContainerLaunchParameters();
  
  int getAllowedFailures();

  ApplicationMasterParameters setHostname(String hostname);
  
  String getHostname();
  
  ApplicationMasterParameters setClientPort(int port);
  
  int getClientPort();
  
  ApplicationMasterParameters setTrackingUrl(String url);
  
  String getTrackingUrl();  
}
