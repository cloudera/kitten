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
package com.cloudera.kitten.appmaster.service;

import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import com.cloudera.kitten.appmaster.ContainerManagerConnectionFactory;
import com.google.common.collect.Maps;

public class ContainerManagerConnectionFactoryImpl implements
    ContainerManagerConnectionFactory {

  private static final Log LOG = LogFactory.getLog(ContainerManagerConnectionFactoryImpl.class);
  
  private final Configuration conf;
  private final YarnRPC rpc;
  private final Map<String, ContainerManager> containerManagers;
  
  public ContainerManagerConnectionFactoryImpl(Configuration conf) {
    this.conf = conf;
    this.rpc = YarnRPC.create(conf);
    this.containerManagers = Maps.newHashMap();
  }
  
  @Override
  public synchronized ContainerManager connect(Container container) {
    NodeId nodeId = container.getNodeId();
    String containerIpPort = String.format("%s:%d", nodeId.getHost(), nodeId.getPort());
    if (!containerManagers.containsKey(containerIpPort)) {
      LOG.info("Connecting to ContainerManager at: " + containerIpPort);
      InetSocketAddress addr = NetUtils.createSocketAddr(containerIpPort);
      ContainerManager cm = (ContainerManager) rpc.getProxy(ContainerManager.class,
          addr, conf);
      containerManagers.put(containerIpPort, cm);
      return cm;
    }
    return containerManagers.get(containerIpPort);
  }

}
