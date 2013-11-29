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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.cloudera.kitten.MasterConnectionFactory;
import com.google.common.base.Preconditions;

public class YarnClientFactory implements MasterConnectionFactory<YarnClient> {

  private static final Log LOG = LogFactory.getLog(YarnClientFactory.class);
  
  private final Configuration conf;

  public YarnClientFactory(Configuration conf) {
    this.conf = Preconditions.checkNotNull(conf);
  }
  
  @Override
  public YarnClient connect() {
    YarnClient client = YarnClient.createYarnClient();
    client.init(new YarnConfiguration(conf));
    client.start();
    return client;
  }
}
