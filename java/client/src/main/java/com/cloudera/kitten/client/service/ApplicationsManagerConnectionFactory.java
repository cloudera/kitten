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

import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import com.cloudera.kitten.MasterConnectionFactory;
import com.google.common.base.Preconditions;

public class ApplicationsManagerConnectionFactory implements
    MasterConnectionFactory<ClientRMProtocol> {

  private static final Log LOG = LogFactory.getLog(ApplicationsManagerConnectionFactory.class);
  
  private final Configuration conf;
  private final YarnRPC rpc;
  
  public ApplicationsManagerConnectionFactory(Configuration conf) {
    this.conf = Preconditions.checkNotNull(conf);
    this.rpc = YarnRPC.create(conf);
  }
  
  @Override
  public ClientRMProtocol connect() {
    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    InetSocketAddress rmAddress = NetUtils.createSocketAddr(yarnConf.get(
        YarnConfiguration.RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADDRESS));     
    LOG.info("Connecting to ResourceManager at: " + rmAddress);
    return ((ClientRMProtocol) rpc.getProxy(ClientRMProtocol.class, rmAddress, conf));
  }
}
