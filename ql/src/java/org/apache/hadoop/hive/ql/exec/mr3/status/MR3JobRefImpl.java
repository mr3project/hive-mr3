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

package org.apache.hadoop.hive.ql.exec.mr3.status;

import com.datamonad.mr3.api.client.DAGStatus;
import com.datamonad.mr3.api.client.VertexStatus;
import com.datamonad.mr3.api.common.MR3Exception;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.mr3.monitoring.MR3JobMonitor;
import org.apache.hadoop.hive.ql.exec.mr3.dag.DAG;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import com.datamonad.mr3.api.client.DAGClient;
import org.apache.tez.common.counters.TezCounters;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class MR3JobRefImpl implements MR3JobRef {

  private final DAGClient dagClient;
  private final MR3JobMonitor monitor;

  public MR3JobRefImpl(
      HiveConf hiveConf,
      DAGClient dagClient,
      Map<String, BaseWork> workMap,
      DAG dag,
      Context ctx,
      AtomicBoolean isShutdown) {
    this.dagClient = dagClient;
    this.monitor = new MR3JobMonitor(workMap, dagClient, hiveConf, dag, ctx, isShutdown);
  }

  @Override
  public String getJobId() {
    // We should not really call dagClient.getApplicationReport() because we are in MR3SessionClient,
    // not in MR3JobClient. Currently we do not call getJobId().
    ApplicationReport applicationReport = dagClient.getApplicationReport().getOrElse(null); // == .orNull
    return applicationReport != null ? applicationReport.getApplicationId().toString(): "None";
  }

  @Override
  public int monitorJob() {
    return monitor.monitorExecution();
  }

  // Invariant: must be called after monitorJob() returns
  public String getDiagnostics() {
    return monitor.getDiagnostics();
  }

  // Invariant: must be called after monitorJob() returns
  public TezCounters getDagCounters() {
    return monitor.getDagCounters();
  }

  // Invariant: must be called after monitorJob() returns with returnCode == 0
  public DAGStatus getDagStatus() {
    return monitor.getDagStatus();
  }

  public String getDagIdStr() throws MR3Exception {
    scala.Option<String> dagIdStr = dagClient.dagIdStr();
    if (dagIdStr.isDefined()) {
      return dagIdStr.get();
    } else {
      throw new MR3Exception("DAG ID not found");
    }
  }
}
