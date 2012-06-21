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

/**
 * A namespace for looking up the names of specific values from a Lua table.
 */
public class LuaFields {

  // Top-level fields in a yarned table.
  public static final String MASTER = "master";
  public static final String CONTAINERS = "containers";
  public static final String CONTAINER = "container";
  public static final String APP_NAME = "name";
  public static final String TIMEOUT = "timeout";
  public static final String USER = "user";
  public static final String QUEUE = "queue";

  // Fields that are common to containers.
  public static final String CONF = "conf";
  public static final String ENV = "env";
  public static final String COMMANDS = "commands";
  public static final String COMMAND = "command";
  public static final String INSTANCES = "instances";
  public static final String MEMORY = "memory";
  public static final String PRIORITY = "priority";

  // For constructing commands from a LuaTable.
  public static final String COMMAND_BASE = "base";
  public static final String ARGS = "args";
  
  // Fields related to constructing LocalResource objects.
  public static final String RESOURCES = "resources";
  public static final String LOCAL_RESOURCE_TYPE = "type";
  public static final String LOCAL_RESOURCE_VISIBILITY = "visibility";
  public static final String LOCAL_RESOURCE_URL = "url";
  public static final String LOCAL_RESOURCE_LOCAL_FILE = "file";
  public static final String LOCAL_RESOURCE_HDFS_FILE = "hdfs";
  
  // Specific to the 'master' table, i.e., application master parameters.
  public static final String TOLERATED_FAILURES = "tolerated_failures";
  
  // Fields that are internal to the framework.
  public static final String KITTEN_JOB_NAME = "KITTEN_JOB_NAME";
  public static final String KITTEN_LUA_CONFIG_FILE = "kitten_config_file.lua";
  public static final String KITTEN_LOCAL_FILE_TO_URI = "KITTEN_LOCAL_FILE_TO_URI";
  public static final String KITTEN_EXTRA_LUA_VALUES = "KITTEN_EXTRA_LUA_VALUES";
  
  // The file that contains the XMLed Configuration object for each container.
  public static final String KITTEN_JOB_XML_FILE = "job.xml";
  
  // Not instantiated.
  private LuaFields() {}
}
