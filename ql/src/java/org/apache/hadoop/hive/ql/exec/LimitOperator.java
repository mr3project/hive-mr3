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

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.tez.LlapObjectCache;
import org.apache.hadoop.hive.ql.exec.tez.TezProcessor;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.mapred.JobConf;

/**
 * Limit operator implementation Limits the number of rows to be passed on.
 **/
public class LimitOperator extends Operator<LimitDesc> implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final String LIMIT_REACHED_KEY_SUFFIX = "_limit_reached";

  protected transient int limit;
  protected transient int offset;
  protected transient int leastRow;
  protected transient int currCount;
  protected transient boolean isMap;

  protected transient ObjectCache runtimeCache;
  protected transient String limitKey;

  private transient boolean calledOnLimitReached;

  /** Kryo ctor. */
  protected LimitOperator() {
    super();
  }

  public LimitOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    limit = conf.getLimit();
    leastRow = conf.getLeastRows();
    offset = (conf.getOffset() == null) ? 0 : conf.getOffset();
    currCount = 0;
    isMap = hconf.getBoolean("mapred.task.is.map", true);

    String queryId = HiveConf.getVar(hconf, HiveConf.ConfVars.HIVEQUERYID);
    int dagIdId = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVE_MR3_QUERY_DAG_ID_ID);
    if (dagIdId == HiveConf.ConfVars.HIVE_MR3_QUERY_DAG_ID_ID.defaultIntVal) {
      this.runtimeCache = null;   // not in TezProcessor
    } else {
      this.runtimeCache = ObjectCacheFactory.getCache(hconf, queryId, dagIdId, false, true);   // user per-query cache
    }

    this.calledOnLimitReached = false;

    // this can happen in HS2 while doing local fetch optimization, where LimitOperator is used
    if (runtimeCache == null) {
      if (!HiveConf.isLoadHiveServer2Config()) {
        throw new IllegalStateException(
            "Cannot get a query cache object while working outside of HS2, this is unexpected");
      }
      // in HS2, this is the only LimitOperator instance for a query, it's safe to fake an object
      // for further processing
      this.runtimeCache = new LlapObjectCache();
    }
    this.limitKey = getOperatorId() + "_record_count";

    AtomicInteger currentCountForAllTasks = getCurrentCount();
    int currentCountForAllTasksInt = currentCountForAllTasks.get();

    if (currentCountForAllTasksInt >= limit) {
      LOG.info("LimitOperator exits early as query limit already reached: {} >= {}",
          currentCountForAllTasksInt, limit);
      onLimitReached();
    }
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    AtomicInteger currentCountForAllTasks = getCurrentCount();
    int currentCountForAllTasksInt = currentCountForAllTasks.get();

    if (offset <= currCount && currCount < (offset + limit) && offset <= currentCountForAllTasksInt
        && currentCountForAllTasksInt < (offset + limit)) {
      forward(row, inputObjInspectors[tag]);
      currCount++;
      currentCountForAllTasks.incrementAndGet();
    } else if (offset > currCount) {
      currCount++;
      currentCountForAllTasks.incrementAndGet();
    } else {
      onLimitReached();
    }
  }

  @Override
  public String getName() {
    return LimitOperator.getOperatorName();
  }

  static public String getOperatorName() {
    return "LIM";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.LIMIT;
  }

  protected void onLimitReached() {
    if (calledOnLimitReached) {
      return;
    }
    super.setDone(true);

    String vertexName = getConfiguration().get(TezProcessor.HIVE_TEZ_VERTEX_NAME);
    String limitReachedKey = getLimitReachedKey(vertexName);

    try {
      runtimeCache.retrieve(limitReachedKey, new Callable<AtomicBoolean>() {
        @Override
        public AtomicBoolean call() {
          return new AtomicBoolean(false);
        }
      }).set(true);
    } catch (HiveException e) {
      throw new RuntimeException(e);
    } finally {
      calledOnLimitReached = true;
    }
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    if (!isMap && currCount < leastRow) {
      throw new HiveException("No sufficient row found");
    }
    super.closeOp(abort);
  }

  public AtomicInteger getCurrentCount() {
    try {
      return runtimeCache.retrieve(limitKey, new Callable<AtomicInteger>() {
        @Override
        public AtomicInteger call() {
          return new AtomicInteger();
        }
      });
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }

  private static String getLimitReachedKey(String vertexName) {
    return vertexName + LIMIT_REACHED_KEY_SUFFIX;
  }

  public static boolean checkLimitReached(String queryId, int dagIdId, String vertexName) {
    String limitReachedKey = getLimitReachedKey(vertexName);

    return checkPeekLimitReached(queryId, dagIdId, limitReachedKey);
  }

  public static boolean checkLimitReachedForVertex(JobConf jobConf) {
    String queryId = HiveConf.getVar(jobConf, HiveConf.ConfVars.HIVEQUERYID);
    int dagIdId = jobConf.getInt(org.apache.tez.mapreduce.input.MRInput.TEZ_MAPREDUCE_DAG_INDEX, -1);
    String vertexName = jobConf.get(org.apache.tez.mapreduce.input.MRInput.TEZ_MAPREDUCE_VERTEX_NAME);
    String limitReachedKey = getLimitReachedKey(vertexName);

    return checkPeekLimitReached(queryId, dagIdId, limitReachedKey);
  }

  // this method never creates an entry with key 'limitReachedKey' in the per-query cache
  // assume MR3_CONTAINER_USE_PER_QUERY_CACHE == true
  private static boolean checkPeekLimitReached(String queryId, int dagIdId, String limitReachedKey) {
    try {
      ObjectCache cache = ObjectCacheFactory.peekLlapObjectCache(queryId, dagIdId);
      if (cache != null) {
        AtomicBoolean limitReached = cache.retrieve(limitReachedKey);
        if (limitReached == null) {
          return false;
        }
        return limitReached.get();
      } else {
        return false;
      }
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }
}
