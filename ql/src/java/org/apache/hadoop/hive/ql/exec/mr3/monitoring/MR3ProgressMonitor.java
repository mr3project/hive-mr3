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
import com.datamonad.mr3.api.client.VertexState$;
import com.datamonad.mr3.api.client.VertexStatus;
import org.apache.hadoop.hive.common.log.ProgressMonitor;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import scala.collection.JavaConversions$;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class MR3ProgressMonitor implements ProgressMonitor {
  private static final int COLUMN_1_WIDTH = 16;
  private final Map<String, BaseWork> workMap;
  private final SessionState.LogHelper console;
  private final long executionStartTime;
  private final DAGStatus status;
  Map<String, VertexProgress> progressCountsMap = new HashMap<>();

  MR3ProgressMonitor(DAGStatus status, Map<String, BaseWork> workMap,
      SessionState.LogHelper console, long executionStartTime) {
    this.status = status;
    this.workMap = workMap;
    this.console = console;
    this.executionStartTime = executionStartTime;

    Map<String, VertexStatus> vertexStatusMap =
        JavaConversions$.MODULE$.mapAsJavaMap(status.vertexStatusMap());
    for (Map.Entry<String, VertexStatus> entry : vertexStatusMap.entrySet()) {
      String vertexName = entry.getKey();
      VertexState$.Value vertexState = entry.getValue().state();
      Progress progress = entry.getValue().progress();
      progressCountsMap.put(vertexName, new VertexProgress(vertexState, progress));
    }
  }

  public List<String> headers() {
    return Arrays.asList(
        "VERTICES",
        "MODE",
        "STATUS",
        "TOTAL",
        "COMPLETED",
        "RUNNING",
        "PENDING",
        "FAILED",
        "KILLED"
    );
  }

  public List<List<String>> rows() {
    try {
      List<List<String>> results = new ArrayList<>();
      SortedSet<String> keys = new TreeSet<>(progressCountsMap.keySet());
      for (String s : keys) {
        VertexProgress progress = progressCountsMap.get(s);

        // Map 1 .......... container  SUCCEEDED      7          7        0        0       0       0

        results.add(
            Arrays.asList(
                getNameWithProgress(s, progress.succeededTaskCount, progress.totalTaskCount),
                getMode(s, workMap),
                progress.vertexState(),
                progress.total(),
                progress.completed(),
                progress.running(),
                progress.pending(),
                progress.failed(),
                progress.killed()
            )
        );
      }
      return results;
    } catch (Exception e) {
      console.printInfo(
          "Getting  Progress Bar table rows failed: " + e.getMessage() + " stack trace: " + Arrays
              .toString(e.getStackTrace())
      );
    }
    return Collections.emptyList();
  }

  // -------------------------------------------------------------------------------
  // VERTICES: 03/04            [=================>>-----] 86%  ELAPSED TIME: 1.71 s
  // -------------------------------------------------------------------------------
  // contains footerSummary , progressedPercentage, starTime

  @Override
  public String footerSummary() {
    return String.format("VERTICES: %02d/%02d", completed(), progressCountsMap.keySet().size());
  }

  @Override
  public long startTime() {
    return executionStartTime;
  }

  @Override
  public double progressedPercentage() {
    int sumTotal = 0, sumComplete = 0;
    for (String s : progressCountsMap.keySet()) {
      VertexProgress progress = progressCountsMap.get(s);
      final int complete = progress.succeededTaskCount;
      final int total = progress.totalTaskCount;
      if (total > 0) {
        sumTotal += total;
        sumComplete += complete;
      }
    }
    return (sumTotal == 0) ? 0.0f : (float) sumComplete / (float) sumTotal;
  }

  @Override
  public String executionStatus() {
    return this.status.state().toString();
  }

  public int getTotalReducers() {
    int totalReducers = 0;
    for (Map.Entry<String, VertexProgress> entry : progressCountsMap.entrySet()) {
      if (entry.getKey().contains("Reducer")) {
        totalReducers += entry.getValue().getTotalTaskCount();
      }
    }
    return totalReducers;
  }

  private int completed() {
    // TODO: why not use a counter??? because of duplicate Vertex names???
    Set<String> completed = new HashSet<>();
    for (String s : progressCountsMap.keySet()) {
      VertexProgress progress = progressCountsMap.get(s);
      final int complete = progress.succeededTaskCount;
      final int total = progress.totalTaskCount;
      if (total > 0) {
        if (complete == total) {
          completed.add(s);
        }
      }
    }
    return completed.size();
  }

  // Map 1 ..........

  private String getNameWithProgress(String s, int complete, int total) {
    String result = "";
    if (s != null) {
      float percent = total == 0 ? 0.0f : (float) complete / (float) total;
      // lets use the remaining space in column 1 as progress bar
      int spaceRemaining = COLUMN_1_WIDTH - s.length() - 1;
      String trimmedVName = s;

      // if the vertex name is longer than column 1 width, trim it down
      // "MR3 Merge File Work" will become "MR3 Merge File.."
      if (s.length() > COLUMN_1_WIDTH) {
        trimmedVName = s.substring(0, COLUMN_1_WIDTH - 1);
        trimmedVName = trimmedVName + "..";
      }

      result = trimmedVName + " ";
      int toFill = (int) (spaceRemaining * percent);
      for (int i = 0; i < toFill; i++) {
        result += ".";
      }
    }
    return result;
  }

  private String getMode(String name, Map<String, BaseWork> workMap) {
    String mode = "container";
    BaseWork work = workMap.get(name);
    if (work != null) {
      // uber > llap > container
      if (work.getUberMode()) {
        mode = "uber";
      } else if (work.getLlapMode()) {
        mode = "llap";
      } else {
        mode = "container";
      }
    }
    return mode;
  }

  static class VertexProgress {
    private final VertexState$.Value vertexState;
    private final int totalTaskCount;
    private final int succeededTaskCount;
    private final int failedTaskAttemptCount;
    private final long killedTaskAttemptCount;
    private final int runningTaskCount;

    VertexProgress(VertexState$.Value vertexState, Progress progress) {
      this.vertexState = vertexState;
      this.totalTaskCount = progress.numTasks();
      this.succeededTaskCount = progress.numSucceededTasks();
      this.failedTaskAttemptCount = progress.numFailedTaskAttempts();
      this.killedTaskAttemptCount = progress.numKilledTaskAttempts();
      this.runningTaskCount =
        progress.numScheduledTasks() - progress.numSucceededTasks() - progress.numFailedTasks();
    }

    boolean isRunning() {
      return succeededTaskCount < totalTaskCount && (succeededTaskCount > 0 || runningTaskCount > 0
          || failedTaskAttemptCount > 0);
    }

    String vertexState() { return vertexState.toString(); }

    //    "TOTAL", "COMPLETED", "RUNNING", "PENDING", "FAILED", "KILLED"

    String total() {
      return String.valueOf(totalTaskCount);
    }

    String completed() {
      return String.valueOf(succeededTaskCount);
    }

    String running() {
      return String.valueOf(runningTaskCount);
    }

    String pending() {
      return String.valueOf(totalTaskCount - (succeededTaskCount + runningTaskCount));
    }

    String failed() {
      return String.valueOf(failedTaskAttemptCount);
    }

    String killed() {
      return String.valueOf(killedTaskAttemptCount);
    }

    int getTotalTaskCount() {
      return totalTaskCount;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      VertexProgress that = (VertexProgress) o;

      if (totalTaskCount != that.totalTaskCount)
        return false;
      if (succeededTaskCount != that.succeededTaskCount)
        return false;
      if (failedTaskAttemptCount != that.failedTaskAttemptCount)
        return false;
      if (killedTaskAttemptCount != that.killedTaskAttemptCount)
        return false;
      if (runningTaskCount != that.runningTaskCount)
        return false;
      return vertexState == that.vertexState;
    }

    @Override
    public int hashCode() {
      int result = totalTaskCount;
      result = 31 * result + succeededTaskCount;
      result = 31 * result + failedTaskAttemptCount;
      result = 31 * result + (int) (killedTaskAttemptCount ^ (killedTaskAttemptCount >>> 32));
      result = 31 * result + runningTaskCount;
      result = 31 * result + vertexState.hashCode();
      return result;
    }
  }
}
