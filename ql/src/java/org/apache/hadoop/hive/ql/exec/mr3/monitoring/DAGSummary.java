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

package org.apache.hadoop.hive.ql.exec.mr3.monitoring;

import com.datamonad.mr3.api.client.DAGStatus;
import com.datamonad.mr3.api.client.Progress;
import com.datamonad.mr3.api.client.VertexStatus;
import org.apache.hadoop.hive.common.log.InPlaceUpdate;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.MapOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.mr3.dag.DAG;
import org.apache.hadoop.hive.ql.exec.mr3.dag.Vertex;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;


class DAGSummary implements PrintSummary {

  private static final int FILE_HEADER_SEPARATOR_WIDTH = InPlaceUpdate.MIN_TERMINAL_WIDTH + 34;
  private static final String FILE_HEADER_SEPARATOR = new String(new char[FILE_HEADER_SEPARATOR_WIDTH]).replace("\0", "-");

  private static final String FORMATTING_PATTERN = "%10s %12s %16s %13s %14s %15s";
  private static final String FILE_HEADER = String.format(
      FORMATTING_PATTERN,
      "VERTICES",
      "TOTAL_TASKS",
      "FAILED_ATTEMPTS",
      "KILLED_TASKS",
      "INPUT_RECORDS",
      "OUTPUT_RECORDS"
  );

  private final DecimalFormat secondsFormatter = new DecimalFormat("#0.00");
  private final NumberFormat commaFormatter = NumberFormat.getNumberInstance(Locale.US);

  private final String hiveCountersGroup;
  private final TezCounters hiveCounters;

  private Map<String, VertexStatus> vertexStatusMap;
  private DAG dag;
  private PerfLogger perfLogger;

  DAGSummary(Map<String, VertexStatus> vertexStatusMap,
             DAGStatus status, HiveConf hiveConf,
             DAG dag, PerfLogger perfLogger) {
    this.vertexStatusMap = vertexStatusMap;
    this.dag = dag;
    this.perfLogger = perfLogger;

    this.hiveCountersGroup = HiveConf.getVar(hiveConf, HiveConf.ConfVars.HIVE_COUNTER_GROUP);
    this.hiveCounters = hiveCounters(status);
  }

  private long hiveInputRecordsFromOtherVertices(String vertexName) {
    List<Vertex> inputVerticesList = dag.getVertices().get(vertexName).getInputVertices();
    long result = 0;
    for (Vertex inputVertex : inputVerticesList) {
      String intermediateRecordsCounterName = formattedName(
          ReduceSinkOperator.Counter.RECORDS_OUT_INTERMEDIATE.toString(),
          inputVertex.getName()
      );
      String recordsOutCounterName = formattedName(FileSinkOperator.Counter.RECORDS_OUT.toString(),
          inputVertex.getName());
      result += (
          hiveCounterValue(intermediateRecordsCounterName)
              + hiveCounterValue(recordsOutCounterName)
      );
    }
    return result;
  }

  private String formattedName(String counterName, String vertexName) {
    return String.format("%s_", counterName) + vertexName.replace(" ", "_");
  }

  private long getCounterValueByGroupName(TezCounters counters, String pattern, String counterName) {
    TezCounter tezCounter = counters.getGroup(pattern).findCounter(counterName);
    return (tezCounter == null) ? 0 : tezCounter.getValue();
  }

  private long hiveCounterValue(String counterName) {
    return getCounterValueByGroupName(hiveCounters, hiveCountersGroup, counterName);
  }

  private TezCounters hiveCounters(DAGStatus status) {
    // assert stats.counters().isDefined() == true
    try {
      return status.counters().get();
    } catch (Exception e) {
      // best attempt, shouldn't really kill DAG for this
    }
    return null;
  }

  @Override
  public void print(SessionState.LogHelper console) {
    console.printInfo("Task Execution Summary");

    /* If the counters are missing there is no point trying to print progress */
    if (hiveCounters == null) {
      return;
    }

  /* Print the per Vertex summary */
    printHeader(console);
    SortedSet<String> keys = new TreeSet<>(vertexStatusMap.keySet());
    for (String vertexName : keys) {
      VertexStatus vertexStatus = vertexStatusMap.get(vertexName);
      console.printInfo(vertexSummary(vertexName, vertexStatus));
    }
    console.printInfo(FILE_HEADER_SEPARATOR);
  }

  private String vertexSummary(String vertexName, VertexStatus vertexStatus) {
    Progress progress = vertexStatus.progress();

    /*
     * Get the HIVE counters
     *
     * HIVE
     *  CREATED_FILES=1
     *  DESERIALIZE_ERRORS=0
     *  RECORDS_IN_Map_1=550076554
     *  RECORDS_OUT_INTERMEDIATE_Map_1=854987
     *  RECORDS_OUT_Reducer_2=1
     */
    final long hiveInputRecords =
        hiveCounterValue(formattedName(MapOperator.Counter.RECORDS_IN.toString(), vertexName))
            + hiveInputRecordsFromOtherVertices(vertexName);

    final long hiveOutputRecords =
        hiveCounterValue(formattedName(FileSinkOperator.Counter.RECORDS_OUT.toString(), vertexName)) +
            hiveCounterValue(formattedName(ReduceSinkOperator.Counter.RECORDS_OUT_INTERMEDIATE.toString(), vertexName));

    return String.format(FORMATTING_PATTERN,
        vertexName,
        progress.numTasks(),
        progress.numFailedTaskAttempts(),
        progress.numKilledTaskAttempts(),
        commaFormatter.format(hiveInputRecords),
        commaFormatter.format(hiveOutputRecords));
  }

  private void printHeader(SessionState.LogHelper console) {
    console.printInfo(FILE_HEADER_SEPARATOR);
    console.printInfo(FILE_HEADER);
    console.printInfo(FILE_HEADER_SEPARATOR);
  }
}
