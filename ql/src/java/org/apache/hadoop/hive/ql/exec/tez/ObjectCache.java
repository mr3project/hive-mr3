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

package org.apache.hadoop.hive.ql.exec.tez;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import com.google.common.collect.MapMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.tez.runtime.api.ObjectRegistry;
import org.apache.tez.runtime.api.ProcessorContext;

import com.google.common.base.Preconditions;

/**
 * ObjectCache. Tez implementation based on the tez object registry.
 *
 */
public class ObjectCache implements org.apache.hadoop.hive.ql.exec.ObjectCache {

  private static final Logger LOG = LoggerFactory.getLogger(ObjectCache.class.getName());

  // ObjectRegistry is available via the Input/Output/ProcessorContext.
  // This is setup as part of the Tez Processor construction, so that it is available whenever an
  // instance of the ObjectCache is created. The assumption is that Tez will initialize the Processor
  // before anything else.

  static class ObjectRegistryVertexIndex {
    public ObjectRegistry registry;
    public int vertexIndex;

    public ObjectRegistryVertexIndex(ObjectRegistry registry, int vertexIndex) {
      this.registry = registry;
      this.vertexIndex = vertexIndex;
    }
  }

  private static final ThreadLocal<ObjectRegistryVertexIndex> staticRegistryIndex =
    new ThreadLocal<ObjectRegistryVertexIndex>(){
      @Override
      protected synchronized ObjectRegistryVertexIndex initialValue() {
        return null;
      }
    };

  private static final ExecutorService staticPool = Executors.newCachedThreadPool();

  // Different ObjectCache wrappers can use the same registry. Coordinate loads across all of them,
  // without retaining registries after their task/container lifetime has ended.
  private static final ConcurrentMap<ObjectRegistry, ConcurrentMap<String, FutureTask<Object>>>
      pendingLoadsByRegistry = new MapMaker().weakKeys().makeMap();

  private final ObjectRegistry registry;
  private final ConcurrentMap<String, FutureTask<Object>> pendingLoads;

  public ObjectCache() {
    Preconditions.checkNotNull(staticRegistryIndex.get(),
        "Object registry not setup yet. This should have been setup by the TezProcessor");
    registry = staticRegistryIndex.get().registry;
    pendingLoads = pendingLoadsByRegistry.computeIfAbsent(registry, ignored -> new ConcurrentHashMap<>());
  }

  public static boolean isObjectRegistryConfigured() {
    return (staticRegistryIndex.get() != null);
  }

  public static void setupObjectRegistry(ProcessorContext context) {
    ObjectRegistryVertexIndex currentRegistryIndex = staticRegistryIndex.get();
    if (currentRegistryIndex == null) {
      // context.getObjectRegistry() in MR3 returns a fresh ObjectRegistry, so each thread has its own
      // ObjectRegistry, which is necessary because ObjectRegistry keeps MapWork.
      int vertexIndex = context.getTaskVertexIndex();
      staticRegistryIndex.set(new ObjectRegistryVertexIndex(context.getObjectRegistry(), vertexIndex));
      if (LOG.isDebugEnabled()) { LOG.debug(
          "ObjectRegistry created from ProcessorContext: {} {} {}", vertexIndex, context.getTaskIndex(), context.getTaskAttemptNumber()); }
    } else {
      int currentVertexIndex = currentRegistryIndex.vertexIndex;
      int newVertexIndex = context.getTaskVertexIndex();
      if (currentVertexIndex != newVertexIndex) {
        currentRegistryIndex.registry = context.getObjectRegistry();
        currentRegistryIndex.vertexIndex = newVertexIndex;
        if (LOG.isDebugEnabled()) { LOG.debug(
            "ObjectRegistry reset from ProcessorContext: {} {} {}", newVertexIndex, context.getTaskIndex(), context.getTaskAttemptNumber()); }
      }
    }
  }

  @com.google.common.annotations.VisibleForTesting
  public static void setupObjectRegistryDummy() {
    staticRegistryIndex.set(new ObjectRegistryVertexIndex(new org.apache.tez.runtime.common.objectregistry.ObjectRegistryImpl(), 0));
  }

  public static void clearObjectRegistry() {
    LOG.info("Clearing ObjectRegistry");
    staticRegistryIndex.set(null);
  }

  public static int getCurrentVertexIndex() {
    ObjectRegistryVertexIndex currentRegistryIndex = staticRegistryIndex.get();
    assert currentRegistryIndex != null;
    return currentRegistryIndex.vertexIndex;
  }

  @Override
  public void release(String key) {
    // nothing to do
    LOG.info("Releasing key: " + key);
  }


  @SuppressWarnings("unchecked")
  @Override
  public <T> T retrieve(String key) throws HiveException {
    T value = null;
    try {
      value = (T) registry.get(key);
      if ( value != null) {
        LOG.info("Found " + key + " in cache with value: " + value);
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
    return value;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T retrieve(String key, Callable<T> fn) throws HiveException {
    try {
      T value = (T) registry.get(key);
      if (value != null) {
        LOG.debug("Found {} in cache with value: {}", key, value);
        return value;
      }

      FutureTask<Object> load = new FutureTask<>(() -> {
        // Another load may have finished between the first lookup and registering this task.
        Object cached = registry.get(key);
        if (cached != null) {
          return cached;
        }
        T loaded = fn.call();
        LOG.debug("Caching key: {}", key);
        registry.cacheForVertex(key, loaded);
        return loaded;
      });
      FutureTask<Object> existing = pendingLoads.putIfAbsent(key, load);
      if (existing != null) {
        return (T) existing.get();
      }
      try {
        // The caller already runs on the async pool when invoked by retrieveAsync. Do not submit
        // another task or hold a registry-wide lock while loading unrelated keys.
        load.run();
        return (T) load.get();
      } finally {
        pendingLoads.remove(key, load);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new HiveException(e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof Error) {
        throw (Error) e.getCause();
      }
      throw new HiveException(e.getCause());
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  @Override
  public <T> Future<T> retrieveAsync(final String key, final Callable<T> fn) throws HiveException {
    return staticPool.submit(new Callable<T>() {
      @Override
      public T call() throws Exception {
        return retrieve(key, fn);
      }
    });
  }

  @Override
  public void remove(String key) {
    LOG.info("Removing key: " + key);
    registry.delete(key);
  }

  public static void close() {
    if (staticPool != null) {
      staticPool.shutdown();
    }
  }
}
