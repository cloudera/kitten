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

import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.newInputStreamSupplier;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.kitten.ContainerLaunchParameters;
import com.cloudera.kitten.client.YarnClientParameters;
import com.cloudera.kitten.client.params.lua.LuaYarnClientParameters;
import com.cloudera.kitten.util.LocalDataHelper;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

public class BasicLuaConfigTest {

  Resource clusterMin;
  Resource clusterMax;
  Configuration conf;
  
  @Before
  public void setUp() throws Exception {
    clusterMin = Records.newRecord(Resource.class);
    clusterMin.setMemory(50);
    clusterMax = Records.newRecord(Resource.class);
    clusterMax.setMemory(90);
    conf = new Configuration();
    conf.set(LocalDataHelper.APP_BASE_DIR, "file:///tmp/");
  }
  
  @Test
  public void testBasicClient() throws Exception {
    File tmpFile = File.createTempFile("kitten", ".lua");
    Files.copy(newInputStreamSupplier(getResource("lua/test1.lua")), tmpFile);
    tmpFile.deleteOnExit();
    YarnClientParameters params = new LuaYarnClientParameters(tmpFile.getAbsolutePath(), "distshell",
        conf);
    assertEquals("Distributed Shell", params.getApplicationName());
    assertEquals(86400L, params.getClientTimeoutMillis());
    assertEquals("", params.getQueue());
    
    ContainerLaunchParameters clp = params.getApplicationMasterParameters(null);
    assertEquals("josh", clp.getUser());
    assertEquals(1, clp.getPriority());
    assertEquals(100, clp.getMemory());
    // clusterMax = 90 < 100
    assertEquals(clusterMax, clp.getContainerResource(clusterMin, clusterMax));
    Map<String, String> expEnv = ImmutableMap.of(
        "zs", "10", "a", "b", "fiz", "faz", "foo", "foo", "biz", "baz");
    Map<String, String> actEnv = clp.getEnvironment();
    for (Map.Entry<String, String> e : expEnv.entrySet()) {
      assertEquals(e.getValue(), actEnv.get(e.getKey()));
    }
  }
  
}
