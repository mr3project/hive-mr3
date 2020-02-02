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

import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.text.DecimalFormat;

class QueryExecutionBreakdownSummary implements PrintSummary {
  public static final int MIN_TERMINAL_WIDTH = 94;
  public static final String SEPARATOR = new String(new char[MIN_TERMINAL_WIDTH]).replace("\0", "-");

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

    // "Compile Query" and "Prepare Plan" cannot be calculated in Hive 1

    // submit to accept dag (if session is closed, this will include re-opening of session time,
    // localizing files for AM, submitting DAG)
    long submitToAccept = perfLogger.getStartTime(PerfLogger.MR3_RUN_DAG) - dagSubmitStartTime;
    console.printInfo(format("Submit Plan", submitToAccept));

    // accept to start dag (schedule wait time, resource wait time etc.)
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
