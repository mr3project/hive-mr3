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
  private static final int MULTIPLE_LIMIT_OPERATOR_FOUND = -1;

  protected transient int limit;
  protected transient int offset;
  protected transient int leastRow;
  protected transient int currCount;
  protected transient boolean isMap;

  // TODO: set runtimeCache only if this LimitOperator is the last operator before RS or TerminalOperator

  protected transient ObjectCache runtimeCache;
  protected transient String limitReachedKey;

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

    boolean isLastOperatorInVertex =
        this.getChildOperators().stream().allMatch(op -> op instanceof TerminalOperator);

    if (isLastOperatorInVertex) {
      String queryId = HiveConf.getVar(hconf, HiveConf.ConfVars.HIVE_QUERY_ID);
      int dagIdId = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVE_MR3_QUERY_DAG_ID_ID);
      if (dagIdId == HiveConf.ConfVars.HIVE_MR3_QUERY_DAG_ID_ID.defaultIntVal) {
        this.runtimeCache = null;   // not in TezProcessor
      } else {
        // use per-thread cache because we do not add the number of records produced by different tasks from the same vertex
        this.runtimeCache = ObjectCacheFactory.getCache(hconf, queryId, dagIdId, true, false);
      }

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

      String vertexName = getConfiguration().get(TezProcessor.HIVE_TEZ_VERTEX_NAME);
      this.limitReachedKey = getLimitReachedKey(vertexName);
      // clean runtimeCache.limitReachedKey which should be initialized only if limit is reached
      resetLimitRecords();
    } else {
      LOG.info("{}: do not cache limit and # of records " +
          "because this operator does not decide the number of records for the current TezProcessor",
          getOperatorId());
      this.runtimeCache = null;
    }
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    if (offset <= currCount && currCount < (offset + limit)) {
      forward(row, inputObjInspectors[tag]);
      currCount++;
    } else if (currCount < offset) {
      currCount++;
    } else {
      setDone(true);
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

  @Override
  public void closeOp(boolean abort) throws HiveException {
    if (!isMap && currCount < leastRow) {
      throw new HiveException("No sufficient row found");
    }

    if (runtimeCache != null) {
      scala.Tuple2<java.lang.Integer, java.lang.Integer> current = runtimeCache.retrieve(limitReachedKey);
      if (current == null) {
        LOG.info("LimitOperator {} sets the final limit and # of records: {}, {}", getOperatorId(), limit, currCount);
        setLimitRecords(limit, currCount);
      } else {
        int currentLimit = current._1();
        if (currentLimit != MULTIPLE_LIMIT_OPERATOR_FOUND) {
          LOG.info("multiple LimitOperators detected: {}, {}", getOperatorId(), currentLimit);
          setLimitRecords(MULTIPLE_LIMIT_OPERATOR_FOUND, 0);
        } else {
          LOG.info("multiple LimitOperators already detected: {}", getOperatorId());
        }
      }
    }

    super.closeOp(abort);
  }

  private static String getLimitReachedKey(String vertexName) {
    return vertexName + LIMIT_REACHED_KEY_SUFFIX;
  }

  private void resetLimitRecords() {
    // resetLimitRecords() may have been called already by another LimitOperator,
    // in which case runtimeCache.remove() is a no-op
    runtimeCache.remove(limitReachedKey);
  }

  private scala.Tuple2<java.lang.Integer, java.lang.Integer> setLimitRecords(int opLimit, int numRecords) {
    try {
      return runtimeCache.retrieve(limitReachedKey, new Callable<scala.Tuple2<java.lang.Integer, java.lang.Integer>>() {
        @Override
        public scala.Tuple2<java.lang.Integer, java.lang.Integer> call() {
          return new scala.Tuple2<java.lang.Integer, java.lang.Integer>(opLimit, numRecords);
        }
      });
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }

  public static boolean checkLimitReachedForVertex(JobConf jobConf) {
    // runtimeCache.retrieve(limitReachedKey) can be accessed only from the same thread that executes LimitOperator
    // Q. Currently inside the same thread?
    // A. Potentially yes, e.g.,
    //   from HiveInputFormat/LlapInputFormat.getRecordReader()
    //   <-- MapRecordProcessor.init() <-- TezProcessor.run()
    // However, we return false because in the case of Hive-MR3,
    // MR3 master decides to kill remaining tasks when limit is reached.
    return false;
  }

  public static scala.Tuple2<java.lang.Integer, java.lang.Integer> getLimitRecords(
      Configuration conf, String queryId, int dagIdId, String vertexName) {
    assert dagIdId != HiveConf.ConfVars.HIVE_MR3_QUERY_DAG_ID_ID.defaultIntVal;
    ObjectCache runtimeCache = ObjectCacheFactory.getCache(conf, queryId, dagIdId, true, false);
    if (runtimeCache == null) {
      // ObjectCache.clearObjectRegistry() can be called in TezProcessor.initializeAndRunProcessor(),
      // although Exception is always thrown in such a case
      return null;
    } else {
      try {
        String limitReachedKey = getLimitReachedKey(vertexName);
        scala.Tuple2<java.lang.Integer, java.lang.Integer> current = runtimeCache.retrieve(limitReachedKey);
        if (current != null && current._1() != MULTIPLE_LIMIT_OPERATOR_FOUND) {
          return current;
        } else {
          return null;
        }
      } catch (HiveException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
