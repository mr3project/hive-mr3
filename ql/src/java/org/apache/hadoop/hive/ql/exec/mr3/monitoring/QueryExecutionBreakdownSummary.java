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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.mr3.MR3Task;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.text.DecimalFormat;

import static org.apache.hadoop.hive.ql.exec.mr3.monitoring.Constants.SEPARATOR;

class QueryExecutionBreakdownSummary implements PrintSummary {
  // Methods summary
  private static final String OPERATION_SUMMARY = "%-35s %9s";
  private static final String OPERATION = "OPERATION";
  private static final String DURATION = "DURATION";


  private DecimalFormat decimalFormat = new DecimalFormat("#0.00");
  private PerfLogger perfLogger;

  private final Long dagSubmitStartTime;
  private final Long submitToRunningDuration;

  QueryExecutionBreakdownSummary(PerfLogger perfLogger) {
    this.perfLogger = perfLogger;
    this.dagSubmitStartTime = perfLogger.getStartTime(PerfLogger.MR3_SUBMIT_DAG);
    this.submitToRunningDuration = perfLogger.getDuration(PerfLogger.MR3_SUBMIT_TO_RUNNING);
  }

  private String formatNumber(long number) {
    return decimalFormat.format(number / 1000.0) + "s";
  }

  private String format(String value, long number) {
    return String.format(OPERATION_SUMMARY, value, formatNumber(number));
  }

  public void print(SessionState.LogHelper console) {
    console.printInfo("Query Execution Summary");

    String execBreakdownHeader = String.format(OPERATION_SUMMARY, OPERATION, DURATION);
    console.printInfo(SEPARATOR);
    console.printInfo(execBreakdownHeader);
    console.printInfo(SEPARATOR);

    HiveConf sessionConf = SessionState.get().getConf();
    long compileStartTime = sessionConf.getLong(MR3Task.HIVE_CONF_COMPILE_START_TIME, 0l);
    long compileEndTime = sessionConf.getLong(MR3Task.HIVE_CONF_COMPILE_END_TIME, 0l);

    // parse, analyze, optimize and compile
    long compile = compileEndTime - compileStartTime;
    console.printInfo(format("Compile Query", compile));

    // prepare plan for submission (building DAG, adding resources, creating scratch dirs etc.)
    long totalDAGPrep = dagSubmitStartTime - compileEndTime;
    console.printInfo(format("Prepare Plan", totalDAGPrep));

    // submit to accept dag (if session is closed, this will include re-opening of session time,
    // localizing files for AM, submitting DAG)
    // "Submit Plan" includes the time for calling 1) DAG.createDagProto() and MR3Client.submitDag().
    // MR3Client.submitDag() returns after DAGAppMaster.submitDag() returns in MR3.
    // DAG may transition to Running before DAGAppMaster.submitDag() returns.
    long submitToAccept = perfLogger.getStartTime(PerfLogger.MR3_RUN_DAG) - dagSubmitStartTime;
    console.printInfo(format("Submit Plan", submitToAccept));

    // accept to start dag (schedule wait time, resource wait time etc.)
    // "Start DAG" reports 0 if DAG transitions to Running during "Submit Plan".
    console.printInfo(format("Start DAG", submitToRunningDuration));

    // time to actually run the dag (actual dag runtime)
    final long startToEnd;
    if (submitToRunningDuration == 0) {
      startToEnd = perfLogger.getDuration(PerfLogger.MR3_RUN_DAG);
    } else {
      startToEnd = perfLogger.getEndTime(PerfLogger.MR3_RUN_DAG) -
          perfLogger.getEndTime(PerfLogger.MR3_SUBMIT_TO_RUNNING);
    }
    console.printInfo(format("Run DAG", startToEnd));
    console.printInfo(SEPARATOR);
    console.printInfo("");
  }
}
