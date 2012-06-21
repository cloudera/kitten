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

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.luaj.vm2.LoadState;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.jse.CoerceJavaToLua;
import org.luaj.vm2.lib.jse.JsePlatform;

import com.cloudera.kitten.util.LocalDataHelper;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A wrapper object to make it nicer to work with LuaTables.
 */
public class LuaWrapper implements Iterable<LuaPair> {

  private static final Log LOG = LogFactory.getLog(LuaWrapper.class);
  
  private final LuaTable env;
  
  public LuaWrapper(String script) {
    this(script, ImmutableMap.<String, Object>of());
  }
  
  public LuaWrapper(String script, Map<String, Object> extras) {
    try {
      this.env = JsePlatform.standardGlobals();
      LoadState.load(getClass().getResourceAsStream("/lua/kitten.lua"), "kitten.lua", env).call();
      for (Map.Entry<String, Object> e : extras.entrySet()) {
        env.set(e.getKey(), CoerceJavaToLua.coerce(e.getValue()));
      }
      InputStream luaCode = LocalDataHelper.getFileOrResource(script);
      LoadState.load(luaCode, script, env).call();
    } catch (IOException e) {
      LOG.error("Lua initialization error", e);
      throw new RuntimeException(e);
    }
  }
  
  public LuaWrapper(LuaTable table) {
    this.env = Preconditions.checkNotNull(table);
  }
  
  public boolean isNil(String name) {
    return env.get(name).isnil();
  }
  
  public boolean isTable(String name) {
    return env.get(name).istable();
  }
  
  public LuaWrapper getTable(String name) {
    return new LuaWrapper(env.get(name).checktable());
  }
  
  public Map<String, String> asMap() {
    Map<String, String> map = Maps.newHashMap();
    for (LuaValue lv : env.keys()) {
      map.put(lv.tojstring(), env.get(lv).tojstring());
    }
    return map;
  }
  
  public List<String> asList() {
    List<String> list = Lists.newArrayList();
    for (int i = 0; i < env.length(); i++) {
      list.add(env.get(i).tojstring());
    }
    return list;
  }
  
  public LuaWrapper createTable(String name) {
    LuaTable lt = new LuaTable();
    env.set(name, lt);
    return new LuaWrapper(lt);
  }
  
  public String getString(String name) {
    return env.get(name).tojstring();
  }
  
  public int getInteger(String name) {
    return env.get(name).toint();
  }
  
  public long getLong(String name) {
    return env.get(name).tolong();
  }
  
  public boolean getBoolean(String name) {
    return env.get(name).toboolean();
  }
  
  public double getDouble(String name) {
    return env.get(name).todouble();
  }
  
  public LuaWrapper setString(String field, String value) {
    env.set(field, LuaValue.valueOf(value));
    return this;
  }
  
  public LuaWrapper setInteger(String field, int value) {
    env.set(field, LuaValue.valueOf(value));
    return this;
  }
  
  public LuaWrapper setBoolean(String field, boolean value) {
    env.set(field, LuaValue.valueOf(value));
    return this;
  }
  
  public LuaWrapper setDouble(String field, double value) {
    env.set(field, LuaValue.valueOf(value));
    return this;
  }

  public LuaWrapper addString(String value) {
    env.set(env.getn().toint() + 1, value);
    return this;
  }
  
  @Override
  public Iterator<LuaPair> iterator() {
    return new LuaIterator(env, false);
  }
  
  public Iterator<LuaPair> hashIterator() {
    return Iterators.filter(iterator(), new Predicate<LuaPair>() {
      @Override
      public boolean apply(LuaPair lp) {
        return !lp.key.isint();
      }
    });
  }
  
  public Iterator<LuaPair> arrayIterator() {
    return new LuaIterator(env, true);
  }
  
  private static class LuaIterator implements Iterator<LuaPair> {

    private final LuaTable t;
    private final boolean array;
    
    private LuaValue currentKey;
    private Varargs varargs;
    
    public LuaIterator(LuaTable t, boolean array) {
      this.t = t;
      this.array = array;
      this.currentKey = array ? LuaValue.ZERO : LuaValue.NIL;
      step();
    }
    
    @Override
    public boolean hasNext() {
      return !currentKey.isnil();
    }

    @Override
    public LuaPair next() {
      LuaPair lp = new LuaPair(currentKey, varargs.arg(2));
      step();
      return lp;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Read-only iterator");
    }
    
    private void step() {
      this.varargs = array ? t.inext(currentKey) : t.next(currentKey);
      currentKey = varargs.arg1();
    }
  }
}
