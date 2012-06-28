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
package com.cloudera.kitten.appmaster.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.Maps;

public class HDFSFileFinder extends Configured implements Tool {
  
  private static Log LOG = LogFactory.getLog(HDFSFileFinder.class);
  
  public Map<String,Long> getNumBytesOfGlobHeldByDatanodes(Path p) throws IOException {
    return getNumBytesOfGlobHeldByDatanodes(p, getConf());
  }
  
  public static Map<String,Long> getNumBytesOfGlobHeldByDatanodes(Path p, Configuration conf) throws IOException {
    FileSystem fs = p.getFileSystem(conf);
    
    HashMap<String,Long> bytesHeld = Maps.newHashMap();
    for (FileStatus f : fs.globStatus(p)) {
      BlockLocation[] bls = fs.getFileBlockLocations(p, 0, f.getLen());
      if (bls.length > 0) {
        for (BlockLocation bl : bls) {
          long l = bl.getLength();
          for (String name : bl.getNames()) {
            if (bytesHeld.containsKey(name))
              bytesHeld.put(name, bytesHeld.get(name) + l);
            else
              bytesHeld.put(name, l);
          }
        }
      }
    }
    
    return bytesHeld;
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    for (String a : args) {
      Path p = new Path(a);
      Map<String, Long> bytesHeld = getNumBytesOfGlobHeldByDatanodes(p, conf);
      for (String node : bytesHeld.keySet())
        LOG.info(node + " : " + bytesHeld.get(node) + "b");
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new HDFSFileFinder(), args);
  }
}