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

import java.util.Map;

import com.google.common.collect.Maps;

/**
 * A container for extra resources and environment variables that are internal to the Kitten framework.
 */
public class Extras {

  private Map<String, String> env;
  private Map<String, String> resources;
  
  public Extras() {
    this.env = Maps.newHashMap();
    this.resources = Maps.newHashMap();
  }
  
  public Extras putEnv(String name, String value) {
    this.env.put(name, value);
    return this;
  }
  
  public Extras putAllEnv(Map<String, String> otherEnv) {
    this.env.putAll(otherEnv);
    return this;
  }
  
  public Map<String, String> getEnv() {
    return env;
  }
  
  public Extras putResource(String name, String value) {
    this.resources.put(name, value);
    return this;
  }
  
  public Extras putAllResources(Map<String, String> otherResources) {
    this.resources.putAll(otherResources);
    return this;
  }
  
  public Map<String, String> getResources() {
    return resources;
  }
}
