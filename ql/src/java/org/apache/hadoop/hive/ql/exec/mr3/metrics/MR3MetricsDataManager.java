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

import com.datamonad.mr3.api.client.ApplicationMetricSnapshot;
import com.datamonad.mr3.api.client.ContainerGroupMetricSnapshot;
import com.datamonad.mr3.api.client.MR3MetricSnapshot;
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

  private final MetricsStore store;
  private final ACLManager aclManager;
  private final int configuredMaxPoints;

  public MR3MetricsDataManager(MetricsStore store, ACLManager aclManager, int configuredMaxPoints) {
    this.store = store;
    this.aclManager = aclManager;
    this.configuredMaxPoints = configuredMaxPoints;
  }

  public static void setInstance(MR3MetricsDataManager value) {
    instance = value;
  }

  public static MR3MetricsDataManager getInstance() {
    if (instance == null) throw new IllegalStateException("MR3 metrics service is not active");
    return instance;
  }

  public MetricsResponse application(
      String attemptId, long start, long end, Integer maxPoints,
      String fields, UserGroupInformation user) throws Exception {
    int limit = validate(attemptId, start, end, maxPoints, user);
    Set<String> selected = parseFields(fields, APPLICATION_FIELDS);
    List<MR3MetricSnapshot> values = store.getApplicationSnapshots(attemptId, start, end, limit);
    return response(attemptId, null, values, selected);
  }

  public MetricsResponse containerGroup(
      String attemptId, String group, long start, long end,
      Integer maxPoints, String fields, UserGroupInformation user) throws Exception {
    if (group == null || group.isEmpty()) throw new IllegalArgumentException("containerGroupId is required");
    int limit = validate(attemptId, start, end, maxPoints, user);
    Set<String> selected = parseFields(fields, CONTAINER_FIELDS);
    List<MR3MetricSnapshot> values = store.getContainerGroupSnapshots(attemptId, group, start, end, limit);
    return response(attemptId, group, values, selected);
  }

  public Set<String> containerGroups(String attemptId, UserGroupInformation user) throws Exception {
    validate(attemptId, 0L, 0L, 1, user);
    return store.listContainerGroupIds(attemptId);
  }

  private int validate(
      String attemptId, long start, long end, Integer requested, UserGroupInformation user) throws Exception {
    if (attemptId == null || attemptId.isEmpty()) {
      throw new IllegalArgumentException("attemptId is required");
    }
    if (start > end) {
      throw new IllegalArgumentException("startTime must not exceed endTime");
    }

    int limit = requested == null ? configuredMaxPoints : requested;
    if (limit <= 0 || limit > configuredMaxPoints) {
      throw new IllegalArgumentException("maxPoints must be between 1 and " + configuredMaxPoints);
    }

    if (!aclManager.checkAMViewAccess(user) || !store.hasAttempt(attemptId)) {
      throw new AttemptNotFoundException();
    }
    return limit;
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

  private static MetricsResponse response(String attemptId, String group,
      List<MR3MetricSnapshot> values, Set<String> fields) {
    List<Map<String, Object>> snapshots = new ArrayList<>();
    for (MR3MetricSnapshot snapshot : values) {
      Map<String, Object> value = new LinkedHashMap<>();
      value.put("timestampMillis", snapshot.timestampMillis());
      if (snapshot instanceof ApplicationMetricSnapshot) {
        ApplicationMetricSnapshot s = (ApplicationMetricSnapshot) snapshot;
        put(value, fields, "runningDags", s.runningDags()); put(value, fields, "totalDags", s.totalDags());
        put(value, fields, "succeededDags", s.succeededDags()); put(value, fields, "failedDags", s.failedDags());
        put(value, fields, "killedDags", s.killedDags());
      } else {
        ContainerGroupMetricSnapshot s = (ContainerGroupMetricSnapshot) snapshot;
        put(value, fields, "containers", s.containers()); put(value, fields, "queuedTasks", s.queuedTasks());
        put(value, fields, "runningTasks", s.runningTasks()); put(value, fields, "nodes", s.nodes());
        put(value, fields, "heapBytesMax", s.heapBytesMax()); put(value, fields, "heapBytesUsed", s.heapBytesUsed());
        put(value, fields, "heapWindowBytesMax", s.heapWindowBytesMax());
        put(value, fields, "heapWindowBytesUsed", s.heapWindowBytesUsed());
        put(value, fields, "heapWindowUsagePercent", s.heapWindowUsagePercent());
        put(value, fields, "autoScaleOutThresholdPercent", s.autoScaleOutThresholdPercent());
        put(value, fields, "autoScaleInThresholdPercent", s.autoScaleInThresholdPercent());
        put(value, fields, "containersTotal", s.containersTotal());
        put(value, fields, "completedTasksTotal", s.completedTasksTotal());
        put(value, fields, "succeededTasksTotal", s.succeededTasksTotal());
        put(value, fields, "failedTasksTotal", s.failedTasksTotal());
        put(value, fields, "killedTasksTotal", s.killedTasksTotal());
      }
      snapshots.add(value);
    }
    return new MetricsResponse(attemptId, group, snapshots);
  }

  private static void put(Map<String, Object> value, Set<String> fields, String name, Object field) {
    if (fields.contains(name)) value.put(name, field);
  }

  private static final Set<String> APPLICATION_FIELDS = new LinkedHashSet<>(Arrays.asList(
      "runningDags", "totalDags", "succeededDags", "failedDags", "killedDags"));

  private static final Set<String> CONTAINER_FIELDS = new LinkedHashSet<>(Arrays.asList(
      "containers", "queuedTasks", "runningTasks", "nodes", "heapBytesMax", "heapBytesUsed",
      "heapWindowBytesMax", "heapWindowBytesUsed", "heapWindowUsagePercent",
      "autoScaleOutThresholdPercent", "autoScaleInThresholdPercent", "containersTotal",
      "completedTasksTotal", "succeededTasksTotal", "failedTasksTotal", "killedTasksTotal"));

  public static final class MetricsResponse {
    public final String applicationAttemptId;
    public final String containerGroupId;
    public final List<Map<String, Object>> snapshots;

    MetricsResponse(String applicationAttemptId, String containerGroupId, List<Map<String, Object>> snapshots) {
      this.applicationAttemptId = applicationAttemptId;
      this.containerGroupId = containerGroupId;
      this.snapshots = snapshots;
    }
  }

  public static final class AttemptNotFoundException extends Exception {}
}
