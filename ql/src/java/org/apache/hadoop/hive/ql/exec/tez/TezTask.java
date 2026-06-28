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

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.UnionWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.mapred.JobConf;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * TezTask handles the execution of TezWork. Currently it executes a graph of map and reduce work
 * using the Tez APIs directly.
 *
 */
@SuppressWarnings({"serial"})
public class TezTask extends Task<TezWork> {

  private static final String CLASS_NAME = TezTask.class.getName();
  public static final String JOB_ID_TEMPLATE = "job_%s%d_%s";
  public static final String ICEBERG_PROPERTY_PREFIX = "iceberg.mr.";
  public static final String ICEBERG_SERIALIZED_TABLE_PREFIX = "iceberg.mr.serialized.table.";
  private static transient Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  private TezCounters counters;

  Map<BaseWork, Vertex> workToVertex = new HashMap<BaseWork, Vertex>();
  Map<BaseWork, JobConf> workToConf = new HashMap<BaseWork, JobConf>();

  public TezTask() {
    super();
  }

  public TezTask(DagUtils utils) {
    super();
  }

  public TezCounters getTezCounters() {
    return counters;
  }

  public void setTezCounters(final TezCounters counters) {
    this.counters = counters;
  }

  /**
   * Making TezTask backward compatible with the old MR-based Task API (ExecDriver/MapRedTask)
   */
  @Override
  public String getExternalHandle() {
    return this.jobID;
  }

  @Override
  public int execute() {
    return executeMr3();
  }

  private java.util.concurrent.atomic.AtomicBoolean isShutdownMr3 = new java.util.concurrent.atomic.AtomicBoolean(false);
  private transient DriverContext driverContext;

  public void setDriverContext(DriverContext driverContext) {
    this.driverContext = driverContext;
  }

  private int executeMr3() {
    org.apache.hadoop.hive.ql.exec.mr3.MR3Task mr3Task =
      new org.apache.hadoop.hive.ql.exec.mr3.MR3Task(conf, console, isShutdownMr3, driverContext);
    int returnCode = mr3Task.execute(context, this.getWork());
    // Utils.mergeTezCounters is null-safe.
    counters = Utils.mergeTezCounters(mr3Task.getTezCounters(), counters);
    Throwable exFromMr3 = mr3Task.getException();
    if (exFromMr3 != null) {
      this.setException(exFromMr3);
    }
    if (exFromMr3 == null) {
      updateNumRows();
    }
    return returnCode;
  }

  private void updateNumRows() {
    if (counters != null) {
      TezCounter counter = counters.findCounter(
        conf.getVar(HiveConf.ConfVars.HIVE_COUNTER_GROUP), FileSinkOperator.TOTAL_TABLE_ROWS_WRITTEN);
      if (counter != null) {
        queryState.setNumModifiedRows(counter.getValue());
      }
    }
  }

  @Override
  public void updateTaskMetrics(Metrics metrics) {
    metrics.incrementCounter(MetricsConstant.HIVE_TEZ_TASKS);
  }

  @Override
  public boolean isMapRedTask() {
    return true;
  }

  @Override
  public StageType getType() {
    return StageType.MAPRED;
  }

  @Override
  public String getName() {
    return "TEZ";
  }

  @Override
  public boolean canExecuteInParallel() {
    return false;
  }

  @Override
  public Collection<MapWork> getMapWork() {
    List<MapWork> result = new LinkedList<MapWork>();
    TezWork work = getWork();

    // framework expects MapWork instances that have no physical parents (i.e.: union parent is
    // fine, broadcast parent isn't)
    for (BaseWork w: work.getAllWorkUnsorted()) {
      if (w instanceof MapWork) {
        List<BaseWork> parents = work.getParents(w);
        boolean candidate = true;
        for (BaseWork parent: parents) {
          if (!(parent instanceof UnionWork)) {
            candidate = false;
          }
        }
        if (candidate) {
          result.add((MapWork)w);
        }
      }
    }
    return result;
  }

  @Override
  public Operator<? extends OperatorDesc> getReducer(MapWork mapWork) {
    List<BaseWork> children = getWork().getChildren(mapWork);
    if (children.size() != 1) {
      return null;
    }

    if (!(children.get(0) instanceof ReduceWork)) {
      return null;
    }

    return ((ReduceWork)children.get(0)).getReducer();
  }

  @Override
  public void shutdown() {
    super.shutdown();
    isShutdownMr3.set(true);
    LOG.info("Setting isShutdownMr3 to true");
  }
}
