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

package org.apache.hadoop.hive.ql.exec.mr3.metrics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.mr3.timeline.security.ACLManager;
import org.apache.hadoop.security.UserGroupInformation;

public class MR3MetricsDataManager {

  private static volatile MR3MetricsDataManager instance;

  public static MR3MetricsDataManager getInstance() {
    MR3MetricsDataManager current = instance;
    if (current == null) {
      throw new IllegalStateException("MR3 metrics service is not active");
    }
    return current;
  }

  public static MR3MetricsDataManager createInstance(
      MetricsStore store, ACLManager aclManager, int restMaxPoints) {
    instance = new MR3MetricsDataManager(store, aclManager, restMaxPoints);
    return instance;
  }

  public static void clearInstance() {
    instance = null;
  }

  private final MetricsStore metricsStore;
  private final ACLManager aclManager;
  private final int restMaxPoints;

  public MR3MetricsDataManager(MetricsStore metricsStore, ACLManager aclManager, int restMaxPoints) {
    assert restMaxPoints > 0;
    this.metricsStore = metricsStore;
    this.aclManager = aclManager;
    this.restMaxPoints = restMaxPoints;
  }

  public MetricsResponse application(
      String attemptId,
      long applicationStartTime, long start, long end,
      String fields, UserGroupInformation user) throws Exception {
    validate(attemptId, start, end, user);
    if (applicationStartTime < 0 || applicationStartTime > start) {
      throw new IllegalArgumentException(
          "applicationStartTime must be between 0 and startTime");
    }
    Set<String> selected = parseFields(fields, APPLICATION_FIELDS);
    MetricSnapshotPage page =
        metricsStore.getApplicationSnapshotsPage(attemptId, start, end, restMaxPoints);
    MetricSnapshotMessage predecessor = start > applicationStartTime
        ? metricsStore.getApplicationSnapshotBefore(attemptId, start) : null;
    return response(attemptId, page, selected, predecessor);
  }

  public MetricsResponse container(
      String attemptId,
      long start, long end,
      String fields, UserGroupInformation user) throws Exception {
    validate(attemptId, start, end, user);
    Set<String> selected = parseFields(fields, CONTAINER_FIELDS);
    MetricSnapshotPage page =
        metricsStore.getContainerSnapshotsPage(attemptId, start, end, restMaxPoints);
    return response(attemptId, page, selected, null);
  }

  private void validate(
      String attemptId, long start, long end, UserGroupInformation user) throws Exception {
    if (attemptId == null || attemptId.isEmpty()) {
      throw new IllegalArgumentException("ApplicationAttemptID is required");
    }
    if (start < 0) {
      throw new IllegalArgumentException("startTime must not be negative");
    }
    if (end < 0) {
      throw new IllegalArgumentException("endTime must not be negative");
    }
    if (start > end) {
      throw new IllegalArgumentException("startTime must not exceed endTime");
    }

    if (!aclManager.checkAMViewAccess(user) || !metricsStore.hasAttempt(attemptId)) {
      throw new AttemptNotFoundException();
    }
  }

  private static Set<String> parseFields(String fields, Set<String> allowed) {
    if (fields == null || fields.trim().isEmpty()) {
      return allowed;
    }

    Set<String> selected = new LinkedHashSet<>(Arrays.asList(fields.split(",")));
    for (String field : selected) {
      if (!allowed.contains(field)) {
        throw new IllegalArgumentException("Unknown metric field: " + field);
      }
    }
    return selected;
  }

  private static MetricsResponse response(
      String attemptId, MetricSnapshotPage page, Set<String> fields,
      MetricSnapshotMessage predecessor) {
    Long nextStartTime = null;
    if (page.hasMore()) {
      assert !page.snapshots().isEmpty();
      long lastTimestamp = page.snapshots().get(page.snapshots().size() - 1).timestampMillis();
      assert lastTimestamp < Long.MAX_VALUE;
      nextStartTime = lastTimestamp + 1;
    }
    List<Map<String, Object>> snapshots = new ArrayList<>();
    if (predecessor != null) {
      addSnapshot(snapshots, predecessor, fields);
    }
    for (MetricSnapshotMessage snapshot : page.snapshots()) {
      addSnapshot(snapshots, snapshot, fields);
    }
    return new MetricsResponse(attemptId, snapshots, page.hasMore(), nextStartTime);
  }

  private static void addSnapshot(List<Map<String, Object>> snapshots,
      MetricSnapshotMessage snapshot, Set<String> fields) {
      Map<String, Object> value = new LinkedHashMap<>();
      value.put("timestampMillis", snapshot.timestampMillis());
      if (snapshot.snapshot() instanceof MR3Metrics.ApplicationSnapshot) {
        MR3Metrics.ApplicationSnapshot s = (MR3Metrics.ApplicationSnapshot) snapshot.snapshot();
        put(value, fields, "runningDags", s.getRunningDags());
        put(value, fields, "totalDags", s.getTotalDags());
        put(value, fields, "succeededDags", s.getSucceededDags());
        put(value, fields, "failedDags", s.getFailedDags());
        put(value, fields, "killedDags", s.getKilledDags());
      } else {
        MR3Metrics.ContainerGroupSnapshot s = (MR3Metrics.ContainerGroupSnapshot) snapshot.snapshot();
        put(value, fields, "containers", s.getContainers());
        put(value, fields, "queuedTasks", s.getQueuedTasks());
        put(value, fields, "runningTasks", s.getRunningTasks());
        put(value, fields, "nodes", s.getNodes());
        put(value, fields, "heapBytesMax", s.getHeapBytesMax());
        put(value, fields, "heapBytesUsed", s.getHeapBytesUsed());
        put(value, fields, "heapWindowBytesMax", s.getHeapWindowBytesMax());
        put(value, fields, "heapWindowBytesUsed", s.getHeapWindowBytesUsed());
        put(value, fields, "heapWindowUsagePercent", s.getHeapWindowUsagePercent());
        put(value, fields, "autoScaleOutThresholdPercent", s.getAutoScaleOutThresholdPercent());
        put(value, fields, "autoScaleInThresholdPercent", s.getAutoScaleInThresholdPercent());
        put(value, fields, "containersTotal", s.getContainersTotal());
        put(value, fields, "completedTasksTotal", s.getCompletedTasksTotal());
        put(value, fields, "succeededTasksTotal", s.getSucceededTasksTotal());
        put(value, fields, "failedTasksTotal", s.getFailedTasksTotal());
        put(value, fields, "killedTasksTotal", s.getKilledTasksTotal());
      }
      snapshots.add(value);
  }

  private static void put(Map<String, Object> value, Set<String> fields, String name, Object field) {
    if (fields.contains(name)) value.put(name, field);
  }

  private static final Set<String> APPLICATION_FIELDS = new LinkedHashSet<>(Arrays.asList(
      "runningDags",
      "totalDags",
      "succeededDags",
      "failedDags",
      "killedDags"));

  private static final Set<String> CONTAINER_FIELDS = new LinkedHashSet<>(Arrays.asList(
      "containers",
      "queuedTasks",
      "runningTasks",
      "nodes",
      "heapBytesMax",
      "heapBytesUsed",
      "heapWindowBytesMax",
      "heapWindowBytesUsed",
      "heapWindowUsagePercent",
      "autoScaleOutThresholdPercent",
      "autoScaleInThresholdPercent",
      "containersTotal",
      "completedTasksTotal",
      "succeededTasksTotal",
      "failedTasksTotal",
      "killedTasksTotal"));

  public static final class MetricsResponse {
    public final String applicationAttemptId;
    public final List<Map<String, Object>> snapshots;
    public final boolean hasMore;
    public final Long nextStartTime;

    MetricsResponse(String applicationAttemptId, List<Map<String, Object>> snapshots,
                    boolean hasMore, Long nextStartTime) {
      this.applicationAttemptId = applicationAttemptId;
      this.snapshots = snapshots;
      this.hasMore = hasMore;
      this.nextStartTime = nextStartTime;
    }
  }

  public static final class AttemptNotFoundException extends Exception {}
}
