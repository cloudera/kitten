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

import static org.junit.Assert.assertEquals;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.luaj.vm2.lib.jse.JsePlatform;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class LuaContainerLaunchParametersTest {

  private LuaWrapper env = new LuaWrapper(JsePlatform.standardGlobals());
  private LuaContainerLaunchParameters lclp = new LuaContainerLaunchParameters(env, new Configuration(),
      ImmutableMap.<String, URI>of());
  
  @Test
  public void testBasics() {
    assertEquals(0, lclp.getPriority());
    assertEquals(1, lclp.getNumInstances());
    assertEquals(0, lclp.getMemory());
  }
  
  @Test
  public void testEnvironment() {
    LuaWrapper e = env.createTable(LuaFields.ENV);
    e.setString("CLASSPATH", "./*.jar");
    e.setString("USER", "bob");
    assertEquals(ImmutableMap.of("CLASSPATH", "./*.jar", "USER", "bob"), lclp.getEnvironment());
  }
  
  @Test
  public void testTableCommandCreation() {
    env.setString(LuaFields.COMMAND_BASE, "${JAVA_HOME}/bin/java -Xmx512M com.cloudera.kitten.test.Main");
    LuaWrapper args = env.createTable(LuaFields.ARGS);
    args.setString("-Dmapred.jobtracker.url", "http://foo:50030");
    args.setBoolean("-Dmapred.compress.output", false);
    args.addString("bar");
    assertEquals("${JAVA_HOME}/bin/java -Xmx512M com.cloudera.kitten.test.Main -Dmapred.compress.output=false -Dmapred.jobtracker.url=http://foo:50030 bar",
        lclp.toCommand(env));
  }
  
  @Test
  public void testRawCommands() {
    LuaWrapper cmds = env.createTable(LuaFields.COMMANDS);
    cmds.addString("/bin/sh foo.sh");
    cmds.addString("echo 'hello world'");
    assertEquals(ImmutableList.of("/bin/sh foo.sh", "echo 'hello world'"), lclp.getCommands());
  }
}
