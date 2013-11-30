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
package com.cloudera.kitten.appmaster.params.lua;

import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.kitten.ContainerLaunchParameters;
import com.cloudera.kitten.appmaster.ApplicationMasterParameters;
import com.cloudera.kitten.lua.LuaContainerLaunchParameters;
import com.cloudera.kitten.lua.LuaFields;
import com.cloudera.kitten.lua.LuaPair;
import com.cloudera.kitten.lua.LuaWrapper;
import com.cloudera.kitten.util.LocalDataHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.net.NetUtils;

public class LuaApplicationMasterParameters implements ApplicationMasterParameters {
  
  private final LuaWrapper env;
  private final Configuration conf;
  private final Map<String, URI> localToUris;
  private final String hostname;

  private int clientPort = 0;
  private String trackingUrl = "";

  public LuaApplicationMasterParameters(Configuration conf) {
    this(LuaFields.KITTEN_LUA_CONFIG_FILE, System.getenv(LuaFields.KITTEN_JOB_NAME), conf);
  }

  public LuaApplicationMasterParameters(Configuration conf, Map<String, Object> extras) {
    this(LuaFields.KITTEN_LUA_CONFIG_FILE, System.getenv(LuaFields.KITTEN_JOB_NAME), conf, extras);
  }

  public LuaApplicationMasterParameters(String script, String jobName, Configuration conf) {
    this(script, jobName, conf, ImmutableMap.<String, Object>of());
  }
  
  public LuaApplicationMasterParameters(String script, String jobName,
      Configuration conf, Map<String, Object> extras) {
    this(script, jobName, conf, extras, loadLocalToUris());
  }
  
  public LuaApplicationMasterParameters(String script, String jobName,
      Configuration conf,
      Map<String, Object> extras,
      Map<String, URI> localToUris) {
    this.env = new LuaWrapper(script, loadExtras(extras)).getTable(jobName);
    this.conf = conf;
    this.localToUris = localToUris;
    this.hostname = NetUtils.getHostname();
  }
  
  private static Map<String, URI> loadLocalToUris() {
    Map<String, String> e = System.getenv();
    if (e.containsKey(LuaFields.KITTEN_LOCAL_FILE_TO_URI)) {
      return LocalDataHelper.deserialize(e.get(LuaFields.KITTEN_LOCAL_FILE_TO_URI));
    }
    return ImmutableMap.of();
  }
  
  private static Map<String, Object> loadExtras(Map<String, Object> masterExtras) {
    Map<String, String> e = System.getenv();
    if (e.containsKey(LuaFields.KITTEN_EXTRA_LUA_VALUES)) {
      Map<String, Object> extras = Maps.newHashMap(LocalDataHelper.deserialize(
          e.get(LuaFields.KITTEN_EXTRA_LUA_VALUES)));
      extras.putAll(masterExtras);
      return extras;
    }
    return masterExtras;
  }
  
  @Override
  public Configuration getConfiguration() {
    return conf;
  }
  
  @Override
  public String getHostname() {
    return hostname;
  }

  @Override
  public void setClientPort(int clientPort) {
    this.clientPort = clientPort;
  }

  @Override
  public int getClientPort() {
    return clientPort;
  }

  @Override
  public void setTrackingUrl(String trackingUrl) {
    this.trackingUrl = trackingUrl;
  }

  @Override
  public String getTrackingUrl() {
    return trackingUrl;
  }

  @Override
  public int getAllowedFailures() {
    if (env.isNil(LuaFields.TOLERATED_FAILURES)) {
      return 4; // TODO: kind of arbitrary, no? :)
    } else {
      return env.getInteger(LuaFields.TOLERATED_FAILURES);
    }
  }
  
  @Override
  public List<ContainerLaunchParameters> getContainerLaunchParameters() {
    if (!env.isNil(LuaFields.CONTAINERS)) {
     List<ContainerLaunchParameters> clp = Lists.newArrayList();
      Iterator<LuaPair> iter = env.getTable(LuaFields.CONTAINERS).arrayIterator();
      while (iter.hasNext()) {
        clp.add(new LuaContainerLaunchParameters(iter.next().value, conf, localToUris));
      }
      return clp;
    } else if (!env.isNil(LuaFields.CONTAINER)) {
      return ImmutableList.<ContainerLaunchParameters>of(
          new LuaContainerLaunchParameters(env.getTable(LuaFields.CONTAINER), conf, localToUris));
    }
    return ImmutableList.of();
  }
}
