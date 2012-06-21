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
package com.cloudera.kitten.lua;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.luaj.vm2.LuaValue;

import com.cloudera.kitten.ContainerLaunchParameters;
import com.cloudera.kitten.util.Extras;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class LuaContainerLaunchParameters implements ContainerLaunchParameters {

  private static final Log LOG = LogFactory.getLog(LuaContainerLaunchParameters.class);
  
  private final LuaWrapper lv;
  private final Configuration conf;
  private final Map<String, URI> localFileUris;
  private final Extras extras;
  
  public LuaContainerLaunchParameters(LuaValue lv, Configuration conf, Map<String, URI> localFileUris) {
    this(new LuaWrapper(lv.checktable()), conf, localFileUris, new Extras());
  }
  
  public LuaContainerLaunchParameters(LuaWrapper lv, Configuration conf, Map<String, URI> localFileUris) {
    this(lv, conf, localFileUris, new Extras());
  }
  
  public LuaContainerLaunchParameters(LuaValue lv, Configuration conf,
      Map<String, URI> localFileUris, Extras extras) {
    this(new LuaWrapper(lv.checktable()), conf, localFileUris, extras);
  }
  
  public LuaContainerLaunchParameters(LuaWrapper lv, Configuration conf,
      Map<String, URI> localFileUris, Extras extras) {
    this.lv = lv;
    this.conf = conf;
    this.localFileUris = localFileUris;
    this.extras = extras;
  }

  
  @Override
  public String getUser() {
    if (!lv.isNil(LuaFields.USER)) {
      String user = lv.getString(LuaFields.USER);
      if (!user.isEmpty()) {
        return user;
      }
    }
    
    try {
      return UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int getMemory() {
    return lv.getInteger(LuaFields.MEMORY);
  }

  @Override
  public Resource getContainerResource(Resource clusterMin, Resource clusterMax) {
    Resource rsrc = Records.newRecord(Resource.class);
    rsrc.setMemory(Math.min(clusterMax.getMemory(),
        Math.max(clusterMin.getMemory(), getMemory())));
    return rsrc;
  }

  @Override
  public int getPriority() {
    return lv.isNil(LuaFields.PRIORITY) ? 0 : lv.getInteger(LuaFields.PRIORITY);
  }

  @Override
  public int getNumInstances() {
    return lv.isNil(LuaFields.INSTANCES) ? 1 : lv.getInteger(LuaFields.INSTANCES);
  }
  
  @Override
  public Map<String, LocalResource> getLocalResources() {
    Map<String, LocalResource> localResources = Maps.newHashMap();  
    if (!lv.isNil(LuaFields.RESOURCES)) {
      LuaWrapper lr = lv.getTable(LuaFields.RESOURCES);
      for (LuaPair lp : lr) {
        try {
          NamedLocalResource nlr = constructResource(lp);
          localResources.put(nlr.name, nlr.resource);
        } catch (IOException e) {
          LOG.error("Error constructing local resource: " + lp.key, e);
        }
      }
    }
    for (Map.Entry<String, String> elr : extras.getResources().entrySet()) {
      LocalResource rsrc = constructExtraResource(elr.getValue());
      if (rsrc != null) {
        localResources.put(elr.getKey(), rsrc);
      }
    }
    
    // Get a local resource for the configuration object.
    LocalResource confRsrc = constructExtraResource(LuaFields.KITTEN_JOB_XML_FILE);
    if (confRsrc != null) {
      localResources.put(LuaFields.KITTEN_JOB_XML_FILE, confRsrc);
    }
    
    return localResources;
  }

  private LocalResource constructExtraResource(String key) {
    LocalResource rsrc = Records.newRecord(LocalResource.class);
    rsrc.setType(LocalResourceType.FILE);
    rsrc.setVisibility(LocalResourceVisibility.APPLICATION);
    try {
      Path path = new Path(localFileUris.get(key));
      configureLocalResourceForPath(rsrc, path);
    } catch (IOException e) {
      LOG.error("Error constructing extra local resource: " + key, e);
      return null;
    }
    return rsrc;
  }
  
  private static class NamedLocalResource {
    public final String name;
    public final LocalResource resource;
    
    public NamedLocalResource(String name, LocalResource resource) {
      this.name = name;
      this.resource = resource;
    }
  }
  
  private NamedLocalResource constructResource(LuaPair lp) throws IOException {
    LocalResource rsrc = Records.newRecord(LocalResource.class);
    LuaWrapper value = new LuaWrapper(lp.value.checktable());
    String name = lp.key.isint() ? "" : lp.key.tojstring();
    if (value.isNil(LuaFields.LOCAL_RESOURCE_TYPE)) {
      rsrc.setType(LocalResourceType.FILE);
    } else {
      rsrc.setType(LocalResourceType.valueOf(
          value.getString(LuaFields.LOCAL_RESOURCE_TYPE).toUpperCase()));
    }
    if (value.isNil(LuaFields.LOCAL_RESOURCE_VISIBILITY)) {
      rsrc.setVisibility(LocalResourceVisibility.APPLICATION);
    } else {
      rsrc.setVisibility(LocalResourceVisibility.valueOf(
          value.getString(LuaFields.LOCAL_RESOURCE_VISIBILITY).toUpperCase()));
    }
    if (!value.isNil(LuaFields.LOCAL_RESOURCE_URL)) {
      URI uri = URI.create(value.getString(LuaFields.LOCAL_RESOURCE_URL));
      rsrc.setResource(ConverterUtils.getYarnUrlFromURI(uri));
      if (name.isEmpty()) {
        name = (new File(uri.getPath())).getName();
      }
    } else if (!value.isNil(LuaFields.LOCAL_RESOURCE_HDFS_FILE)) {
      Path path = new Path(value.getString(LuaFields.LOCAL_RESOURCE_HDFS_FILE));
      configureLocalResourceForPath(rsrc, path);
      if (name.isEmpty()) {
        name = path.getName();
      }
    } else if (!value.isNil(LuaFields.LOCAL_RESOURCE_LOCAL_FILE)) {
      String src = value.getString(LuaFields.LOCAL_RESOURCE_LOCAL_FILE);
      Path path = new Path(localFileUris.get(src));
      configureLocalResourceForPath(rsrc, path);
      if (name.isEmpty()) {
        name = new Path(src).getName();
      }
    } else {
      throw new IllegalArgumentException(
          "Invalid resource: no 'url', 'hdfs', or 'file' fields specified.");
    }
    return new NamedLocalResource(name, rsrc);
  }
  
  private void configureLocalResourceForPath(LocalResource rsrc, Path path) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    FileStatus stat = fs.getFileStatus(path);
    rsrc.setSize(stat.getLen());
    rsrc.setTimestamp(stat.getModificationTime());
    rsrc.setResource(ConverterUtils.getYarnUrlFromPath(path));
  }
  
  @Override
  public Map<String, String> getEnvironment() {
    Map<String, String> env = Maps.newHashMap(extras.getEnv());
    if (!lv.isNil(LuaFields.ENV)) {
      env.putAll(lv.getTable(LuaFields.ENV).asMap());
    }
    return env;
  }

  @Override
  public List<String> getCommands() {
    List<String> cmds = Lists.newArrayList();
    if (!lv.isNil(LuaFields.COMMANDS)) {
      Iterator<LuaPair> pairsIter = lv.getTable(LuaFields.COMMANDS).arrayIterator();
      while (pairsIter.hasNext()) {
        LuaValue c = pairsIter.next().value;
        if (c.isstring()) {
          cmds.add(c.tojstring());
        } else if (c.istable()) {
          cmds.add(toCommand(new LuaWrapper(c.checktable())));
        }
      }
    } else if (!lv.isNil(LuaFields.COMMAND)) {
      if (lv.isTable(LuaFields.COMMAND)) {
        cmds.add(toCommand(lv.getTable(LuaFields.COMMAND)));
      } else {
        cmds.add(lv.getString(LuaFields.COMMAND));
      }
    }
    if (cmds.isEmpty()) {
      LOG.fatal("No commands found in container!");
    }
    return cmds;
  }
  
  public String toCommand(LuaWrapper table) {
    StringBuilder sb = new StringBuilder(table.getString(LuaFields.COMMAND_BASE));
    if (!table.isNil(LuaFields.ARGS)) {
      LuaWrapper a = table.getTable(LuaFields.ARGS);
      Iterator<LuaPair> namedArgsIter = a.hashIterator();
      while (namedArgsIter.hasNext()) {
        LuaPair lp = namedArgsIter.next();
        sb.append(" ");
        sb.append(lp.key.tojstring());
        sb.append("=");
        sb.append(lp.value.tojstring());
      }
      Iterator<LuaPair> restIter = a.arrayIterator();
      while (restIter.hasNext()) {
        sb.append(" ");
        sb.append(restIter.next().value.tojstring());
      }
    }
    return sb.toString();
  }
}
