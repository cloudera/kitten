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
package com.cloudera.kitten;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.kitten.client.KittenClient;
import com.cloudera.kitten.util.LocalDataHelper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

public class TestKittenDistributedShell {

  private static final Log LOG = LogFactory.getLog(TestKittenDistributedShell.class);

  protected static MiniYARNCluster yarnCluster = null;
  protected static Configuration conf = new Configuration();

  @BeforeClass
  public static void setup() throws InterruptedException, IOException {
    LOG.info("Starting up YARN cluster");
    conf.setInt("yarn.scheduler.fifo.minimum-allocation-mb", 128);
    conf.set("yarn.nodemanager.vmem-pmem-ratio", "20.0");
    if (yarnCluster == null) {
      yarnCluster = new MiniYARNCluster(TestKittenDistributedShell.class.getName(),
          1, 1, 1);
      yarnCluster.init(conf);
      yarnCluster.start();
    }
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      LOG.info("setup thread sleep interrupted. message=" + e.getMessage());
    }   
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (yarnCluster != null) {
      yarnCluster.stop();
      yarnCluster = null;
    }
  }
  
  @Test
  public void testKittenShell() throws Exception {
    String config = "/lua/distshell.lua";

    // For the outputs
    File tmpFile = File.createTempFile("distshell", ".txt");
    tmpFile.deleteOnExit();

    KittenClient client = new KittenClient(
        ImmutableMap.<String, Object>of(
            "TEST_FILE", tmpFile.getAbsolutePath(),
            "PWD", (new File(".")).getAbsolutePath()));
    conf.set(LocalDataHelper.APP_BASE_DIR, "file:///tmp/");
    client.setConf(conf);
    
    assertEquals(0, client.run(new String[] { config, "distshell" }));
    assertEquals(12, Files.readLines(tmpFile, Charsets.UTF_8).size()); 
  }
}
