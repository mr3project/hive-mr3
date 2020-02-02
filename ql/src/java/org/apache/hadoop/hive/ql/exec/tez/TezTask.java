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

import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.mr3.MR3Task;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.UnionWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.tez.common.counters.TezCounters;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 * TezTask handles the execution of TezWork. Currently it executes a graph of map and reduce work
 * using the Tez APIs directly.
 *
 */
@SuppressWarnings({"serial"})
public class TezTask extends Task<TezWork> {

  private AtomicBoolean isShutdown = new AtomicBoolean(false);
  private TezCounters counters;

  public TezTask() {
    super();
  }

  public TezCounters getTezCounters() {
    return counters;
  }


  @Override
  public int execute(DriverContext driverContext) {
    String engine = conf.getVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE);
    if(engine.equals("mr3")) {
      return executeMr3(driverContext);
    } else {
      LOG.warn("Run MR3 instead of Tez");
      return executeMr3(driverContext);
    }
  }

  private int executeMr3(DriverContext driverContext) {
    MR3Task mr3Task = new MR3Task(conf, console, isShutdown);
    int returnCode = mr3Task.execute(driverContext, this.getWork());
    counters = mr3Task.getTezCounters();
    Throwable exFromMr3 = mr3Task.getException();
    if (exFromMr3 != null) {
      this.setException(exFromMr3);
    }
    return returnCode;
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
    isShutdown.set(true);
    LOG.info("Shutting down Tez task " + this);
  }
}
