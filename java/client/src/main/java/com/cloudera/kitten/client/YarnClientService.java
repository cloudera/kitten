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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;

import com.google.common.util.concurrent.Service;

/**
 * A {@code Service} that handles the client-side logic for the lifecycle of a typical
 * YARN application.
 */
public interface YarnClientService extends Service {

  /**
   * Returns the parameters used to configure this service.
   */
  YarnClientParameters getParameters();
  
  /**
   * Returns the ID of the application once it has been submitted. Only valid
   * while the service is in the RUNNING state.
   */
  ApplicationId getApplicationId();
  
  /**
   * Queries the YARN resource manager for the current state of the application on
   * the cluster and returns the result.
   */
  ApplicationReport getApplicationReport();
  
  /**
   * Check to see whether or not the application is still executing on the
   * cluster.
   */
  boolean isApplicationFinished();
  
  /**
   * Returns the final application report for this job.
   */
  ApplicationReport getFinalReport();
}
