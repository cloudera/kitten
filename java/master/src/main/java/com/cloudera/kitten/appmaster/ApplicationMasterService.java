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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;

import com.google.common.util.concurrent.Service;

/**
 * A service that handles the common cluster-side logic for running an application on YARN. It
 * should be included in the same JAR file that contains the application's business logic.
 */
public interface ApplicationMasterService extends Service {
  
  /**
   * Returns the parameters used to configure this service.
   */
  ApplicationMasterParameters getParameters();
  
  /**
   * Returns the application attempt ID.
   */
  ApplicationAttemptId getApplicationAttemptId();
  
  /**
   * Returns true if there are containers that this application master service is
   * monitoring.
   */
  boolean hasRunningContainers();
}
