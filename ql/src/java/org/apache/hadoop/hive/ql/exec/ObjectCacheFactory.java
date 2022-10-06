/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.ql.exec.tez.LlapObjectCache;

/**
 * ObjectCacheFactory returns the appropriate cache depending on settings in
 * the hive conf.
 */
public class ObjectCacheFactory {
  private static final ConcurrentHashMap<String, ObjectCache> llapQueryCaches =
      new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<String, Map<Integer, LlapObjectCache>> llapVertexCaches = new ConcurrentHashMap<>();
  private static final Logger LOG = LoggerFactory.getLogger(ObjectCacheFactory.class);

  private ObjectCacheFactory() {
    // avoid instantiation
  }

  public static ObjectCache getPerTaskMrCache(String queryId) {
    return new ObjectCacheWrapper(
      new org.apache.hadoop.hive.ql.exec.mr.ObjectCache(), queryId);
  }

  /**
   * Returns the appropriate cache
   */
  public static ObjectCache getCache(Configuration conf, String queryId, boolean isPlanCache) {
    // LLAP cache can be disabled via config or isPlanCache
    return getCache(conf, queryId, isPlanCache, false);
  }

  /**
   * Returns the appropriate cache
   * @param conf
   * @param queryId
   * @param isPlanCache
   * @param llapCacheAlwaysEnabled  Whether to always return LLAP cache regardless
   *        of config settings disabling LLAP cache. Valid only if running LLAP.
   * @return
   */
  public static ObjectCache getCache(Configuration conf, String queryId, boolean isPlanCache, boolean llapCacheAlwaysEnabled) {
    if (HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
      if (isPlanCache || !HiveConf.getBoolVar(conf, HiveConf.ConfVars.MR3_CONTAINER_USE_PER_QUERY_CACHE)) {
        // return a per-thread cache
        if (org.apache.hadoop.hive.ql.exec.tez.ObjectCache.isObjectRegistryConfigured()) {
          return new ObjectCacheWrapper(
              new org.apache.hadoop.hive.ql.exec.tez.ObjectCache(), queryId);
        } else {
          // Tez processor needs to configure object registry first.
          return null;
        }
      } else {
        if (llapCacheAlwaysEnabled) {
          // return a per-query cache
          return getLlapObjectCache(queryId);
        } else {
          // return a per-Vertex cache
          int vertexIndex = org.apache.hadoop.hive.ql.exec.tez.ObjectCache.getCurrentVertexIndex();
          return getLlapQueryVertexCache(queryId, vertexIndex);
        }
      }
    } else { // mr
      return new ObjectCacheWrapper(
          new  org.apache.hadoop.hive.ql.exec.mr.ObjectCache(), queryId);
    }
  }

  private static boolean isLlapCacheEnabled(Configuration conf, boolean isPlanCache, boolean llapCacheAlwaysEnabled) {
    return (llapCacheAlwaysEnabled ||
        (HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_OBJECT_CACHE_ENABLED) && !isPlanCache));
  }

  private static ObjectCache getLlapObjectCache(String queryId) {
    // If order of events (i.e. dagstart and fragmentstart) was guaranteed, we could just
    // create the cache when dag starts, and blindly return it to execution here.
    if (queryId == null) throw new RuntimeException("Query ID cannot be null");
    ObjectCache result = llapQueryCaches.get(queryId);
    if (result != null) return result;
    result = new LlapObjectCache();
    ObjectCache old = llapQueryCaches.putIfAbsent(queryId, result);
    if (old == null) {
      LOG.info("Created object cache for " + queryId);
    }
    return (old != null) ? old : result;
  }

  private static LlapObjectCache getLlapQueryVertexCache(String queryId, int vertexIndex) {
    if (queryId == null) throw new RuntimeException("Query ID cannot be null");
    Map<Integer, LlapObjectCache> map = getLlapQueryVertexCacheMap(queryId);
    synchronized (map) {
      LlapObjectCache result = map.get(vertexIndex);
      if (result != null) return result;
      result = new LlapObjectCache();
      map.put(vertexIndex, result);
      LOG.info("Created Vertex cache for " + queryId + " " + vertexIndex);
      return result;
    }
  }

  private static Map<Integer, LlapObjectCache> getLlapQueryVertexCacheMap(String queryId) {
    Map<Integer, LlapObjectCache> result = llapVertexCaches.get(queryId);
    if (result != null) return result;
    result = new HashMap<>();
    Map<Integer, LlapObjectCache> old = llapVertexCaches.putIfAbsent(queryId, result);
    if (old == null && LOG.isInfoEnabled()) {
      LOG.info("Created Vertex cache map for " + queryId);
    }
    return (old != null) ? old : result;
  }

  public static void removeLlapQueryVertexCache(String queryId, int vertexIndex) {
    Map<Integer, LlapObjectCache> result = llapVertexCaches.get(queryId);
    if (result != null) {
      LlapObjectCache prev;
      synchronized (result) {
        prev = result.remove(vertexIndex);
      }
      if (prev != null && LOG.isInfoEnabled()) {
        LOG.info("Removed Vertex cache for " + queryId + " " + vertexIndex);
      }
    }
  }

  public static void removeLlapQueryCache(String queryId) {
    LOG.info("Removing object cache and Vertex cache map for " + queryId);
    llapQueryCaches.remove(queryId);
    llapVertexCaches.remove(queryId);
  }
}
