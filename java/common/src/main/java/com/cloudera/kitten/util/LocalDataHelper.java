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
package com.cloudera.kitten.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;

/**
 * Handles copying files from the client machine out to HDFS for app master and container tasks, and
 * then mapping them properly to the LocalResource objects used by YARN.
 */
public class LocalDataHelper {

  private static Log LOG = LogFactory.getLog(LocalDataHelper.class);
  
  // Provide a way for tests/clients to override the app base directory.
  public static final String APP_BASE_DIR = "kitten.app.base.dir";
  
  public static InputStream getFileOrResource(String name) {
    File f = new File(name);
    if (f.exists()) {
      try {
        return new FileInputStream(f);
      } catch (FileNotFoundException e) {
        LOG.error("A file suddenly disappeared", e);
      }
    } else {
      return LocalDataHelper.class.getResourceAsStream(name);
    }
    return null;
  }
  
  public static <T> String serialize(Map<String, T> mapping) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(mapping);
      oos.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return Base64.encodeBase64String(baos.toByteArray());
  }
  
  public static <T> Map<String, T> deserialize(String serialized) {
    byte[] data = Base64.decodeBase64(serialized);
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    Map<String, T> mapping = null;
    try {
      ObjectInputStream ois = new ObjectInputStream(bais);
      mapping = (Map<String, T>) ois.readObject();
      ois.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return mapping;
  }
  
  private final ApplicationId applicationId;
  private final Configuration conf;
  private final Map<String, URI> localToHdfs;
  private final Set<String> names;
  
  public LocalDataHelper(ApplicationId applicationId, Configuration conf) {
    this.applicationId = applicationId;
    this.conf = conf;
    this.localToHdfs = Maps.newHashMap();
    this.names = Sets.newHashSet();
  }
  
  public void copyConfiguration(String key, Configuration conf) throws IOException {
    File tmpFile = File.createTempFile("job", ".xml");
    tmpFile.deleteOnExit();
    OutputStream os = new FileOutputStream(tmpFile);
    conf.writeXml(os);
    os.close();
    copyToHdfs(key, tmpFile.getAbsolutePath());
  }
  
  public void copyToHdfs(String localDataName) throws IOException {
    copyToHdfs(localDataName, localDataName);
  }
  
  private void copyToHdfs(String key, String localDataName) throws IOException {
    if (!localToHdfs.containsKey(localDataName)) {
      FileSystem fs = FileSystem.get(conf);
      Path src = new Path(localDataName);
      Path dst = getPath(fs, src.getName());
      InputStream data = getFileOrResource(localDataName);
      FSDataOutputStream os = fs.create(dst, true);
      ByteStreams.copy(data, os);
      os.close();
      URI uri = dst.toUri();
      localToHdfs.put(key, uri);
    }
  }
  
  private Path getPath(FileSystem fs, String name) {
    int cp = 0;
    while (names.contains(name)) {
      name = name + (++cp);
    }
    names.add(name);
    String appDir = "app";
    if (applicationId != null) {
      appDir += applicationId.getId();
    }
    Path base = getAppPath(fs, appDir);
    Path dst = new Path(base, name);
    return dst;
  }

  private Path getAppPath(FileSystem fs, String appDir) {
    String abd = conf.get(APP_BASE_DIR);
    if (abd != null) {
      return new Path(new Path(abd), appDir);
    } else {
      return new Path(fs.getHomeDirectory(), appDir);
    }
  }
  
  public Map<String, URI> getFileMapping() {
    return localToHdfs;
  }
}
