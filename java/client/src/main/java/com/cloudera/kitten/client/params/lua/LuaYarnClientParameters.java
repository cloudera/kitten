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
package com.cloudera.kitten.client.params.lua;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.cloudera.kitten.ContainerLaunchParameters;
import com.cloudera.kitten.client.YarnClientParameters;
import com.cloudera.kitten.lua.LuaContainerLaunchParameters;
import com.cloudera.kitten.lua.LuaFields;
import com.cloudera.kitten.lua.LuaPair;
import com.cloudera.kitten.lua.LuaWrapper;
import com.cloudera.kitten.util.Extras;
import com.cloudera.kitten.util.LocalDataHelper;
import com.google.common.collect.ImmutableMap;

public class LuaYarnClientParameters implements YarnClientParameters {

  private static final Log LOG = LogFactory.getLog(LuaYarnClientParameters.class);
  
  private final LuaWrapper env;
  private final Configuration conf;
  private final Extras extras;
  
  public LuaYarnClientParameters(String script, String jobName, Configuration conf) {
    this(script, jobName, conf, ImmutableMap.<String, Object>of());
  }
  
  public LuaYarnClientParameters(String script, String jobName, Configuration conf,
      Map<String, Object> extraLuaValues) {
    this(script, jobName, conf, extraLuaValues, ImmutableMap.<String, String>of());
  }
  
  public LuaYarnClientParameters(String script, String jobName, Configuration conf,
      Map<String, Object> extraLuaValues, Map<String, String> resources) {
    this.env = new LuaWrapper(script, extraLuaValues).getTable(jobName);
    this.conf = initConf(env, conf);
    this.extras = new Extras();
    this.extras.putEnv(LuaFields.KITTEN_JOB_NAME, jobName);
    this.extras.putResource(LuaFields.KITTEN_LUA_CONFIG_FILE, script);
    this.extras.putAllResources(resources);
    if (extraLuaValues != null && !extraLuaValues.isEmpty()) {
      this.extras.putEnv(LuaFields.KITTEN_EXTRA_LUA_VALUES,
          LocalDataHelper.serialize(extraLuaValues));
    }
  }
  
  private static Configuration initConf(LuaWrapper lv, Configuration conf) {
    if (!lv.isNil(LuaFields.CONF)) {
      // Add the given settings to the conf before we start the run.
      for (LuaPair lp : lv.getTable(LuaFields.CONF)) {
        if (lp.key.isint()) {
          if (lp.value.isstring()) {
            String[] pieces = lp.value.tojstring().split("=");
            if (pieces.length == 2) {
              conf.set(pieces[0], pieces[1]);
            } else {
              LOG.warn("Invalid field in container conf: " + lp.value.tojstring());
            }
          } else {
            LOG.warn("Non-string value in container conf: " + lp.value);
          }
        } else {
          conf.set(lp.key.tojstring(), lp.value.tojstring());
        }
      }
    }
    return conf;
  }
  
  @Override
  public Configuration getConfiguration() {
    return conf;
  }
    
  @Override
  public String getApplicationName() {
    return env.getString(LuaFields.APP_NAME);
  }

  @Override
  public String getQueue() {
    return env.isNil(LuaFields.QUEUE) ? "" : env.getString(LuaFields.QUEUE);
  }

  @Override
  public ContainerLaunchParameters getApplicationMasterParameters(ApplicationId applicationId) {
    Map<String, URI> localToUris = mapLocalFiles(applicationId);
    extras.putEnv(LuaFields.KITTEN_LOCAL_FILE_TO_URI, LocalDataHelper.serialize(localToUris));
    return new LuaContainerLaunchParameters(env.getTable(LuaFields.MASTER), conf, localToUris, extras);
  }

  private Map<String, URI> mapLocalFiles(ApplicationId applicationId) {
    LocalDataHelper lfh = new LocalDataHelper(applicationId, conf);
    
    // Map the configuration object as an XML file.
    try {
      lfh.copyConfiguration(LuaFields.KITTEN_JOB_XML_FILE, conf);
    } catch (IOException e) {
      LOG.error("Error copying configuration object", e);
    }
    
    // Map the files that were specified by the framework itself.
    for (String localFileName : extras.getResources().values()) {
      try {
        lfh.copyToHdfs(localFileName);
      } catch (IOException e) {
        LOG.error("Error copying local file " + localFileName + " to hdfs", e);
      }
    }
    
    // Map all of the local files that the appmaster will need.
    mapLocalFiles(env.getTable(LuaFields.MASTER), lfh);
    
    // Map all of the files that the containers will need.   
    if (!env.isNil(LuaFields.CONTAINERS)) {
      Iterator<LuaPair> iter = env.getTable(LuaFields.CONTAINERS).arrayIterator();
      while (iter.hasNext()) {
        mapLocalFiles(new LuaWrapper(iter.next().value.checktable()), lfh);
      }
    } else if (!env.isNil(LuaFields.CONTAINER)) {
      mapLocalFiles(env.getTable(LuaFields.CONTAINER), lfh);
    }
    return lfh.getFileMapping();
  }
  
  private void mapLocalFiles(LuaWrapper entity, LocalDataHelper localFileHelper) {
    if (!entity.isNil(LuaFields.RESOURCES)) {
      LuaWrapper lrsrcs = entity.getTable(LuaFields.RESOURCES);
      for (LuaPair lp : lrsrcs) {
        LuaWrapper rsrc = new LuaWrapper(lp.value.checktable());
        if (!rsrc.isNil(LuaFields.LOCAL_RESOURCE_LOCAL_FILE)) {
          String localFileName = rsrc.getString(LuaFields.LOCAL_RESOURCE_LOCAL_FILE);
          try {
            localFileHelper.copyToHdfs(localFileName);
          } catch (IOException e) {
            LOG.error("Error copying local file " + localFileName + " to hdfs", e);
          }
        }
      }
    }
  }
  
  @Override
  public long getClientTimeoutMillis() {
    return env.isNil(LuaFields.TIMEOUT) ? -1 : env.getLong(LuaFields.TIMEOUT);
  }

}
