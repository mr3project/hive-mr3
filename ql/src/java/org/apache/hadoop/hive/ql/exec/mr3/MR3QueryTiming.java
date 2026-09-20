/**
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

package org.apache.hadoop.hive.ql.exec.mr3;

import com.datamonad.mr3.DAGAPI;
import java.util.LinkedHashMap;
import java.util.Map;

/** Hive-owned query timing values transported by MR3 as opaque attributes. */
public final class MR3QueryTiming {

  public static final String COMPILE = "hive.query.compile.duration.ms";
  public static final String PREPARE = "hive.query.prepare_plan.duration.ms";
  public static final String SUBMIT = "hive.query.submit_plan.duration.ms";
  public static final String PRE_RUN = "hive.query.pre_run.duration.ms";
  public static final String TOTAL = "hive.query.total.duration.ms";

  private final long compileQueryDurationMs;
  private final long preparePlanDurationMs;
  private final long submitPlanDurationMs;
  private final long preRunDurationMs;
  private final long compileStartTime;

  private Long totalTimeMs;

  public MR3QueryTiming(long compileStartTime, long compileEndTime,
      long submitStartTime, long submitInvocationTime) {
    this.compileStartTime = compileStartTime;
    compileQueryDurationMs = Math.max(0, compileEndTime - compileStartTime);
    preparePlanDurationMs = Math.max(0, submitStartTime - compileEndTime);
    submitPlanDurationMs = Math.max(0, submitInvocationTime - submitStartTime);
    preRunDurationMs = compileQueryDurationMs + preparePlanDurationMs + submitPlanDurationMs;
  }

  public void addSubmissionAttributes(DAGAPI.DAGProto.Builder builder) {
    Map<String, String> values = new LinkedHashMap<>();
    values.put(COMPILE, Long.toString(compileQueryDurationMs));
    values.put(PREPARE, Long.toString(preparePlanDurationMs));
    values.put(SUBMIT, Long.toString(submitPlanDurationMs));
    values.put(PRE_RUN, Long.toString(preRunDurationMs));

    values.forEach((key, value) -> builder.addDagAttributes(
        DAGAPI.DAGAttributeProto.newBuilder()
          .setKey(key)
          .setValue(value)));
  }

  public void observeTerminal(long timestamp) {
    if (totalTimeMs == null) {
      totalTimeMs = Math.max(0, timestamp - compileStartTime);
    }
  }

  public long getCompileQueryDurationMs() { return compileQueryDurationMs; }
  public long getPreparePlanDurationMs() { return preparePlanDurationMs; }
  public long getSubmitPlanDurationMs() { return submitPlanDurationMs; }
  public long getPreRunDurationMs() { return preRunDurationMs; }
  public long getTotalTimeMs() { return totalTimeMs == null ? 0 : totalTimeMs; }
  public long getRunDagDurationMs() { return Math.max(0, getTotalTimeMs() - preRunDurationMs); }
}
