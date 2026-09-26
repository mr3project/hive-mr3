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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.hadoop.hive.ql.exec.mr3.dag.DAG;

final class MetricProtoUtils {
  private MetricProtoUtils() {}

  static byte[] encode(ApplicationMetricSnapshot snapshot) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    MR3Metrics.ApplicationSnapshot.newBuilder()
          .setRunningDags(snapshot.runningDags())
          .setTotalDags(snapshot.totalDags())
          .setSucceededDags(snapshot.succeededDags())
          .setFailedDags(snapshot.failedDags())
          .setKilledDags(snapshot.killedDags())
          .build()
          .writeTo(bytes);
    return bytes.toByteArray();
  }

  static byte[] encode(ContainerGroupMetricSnapshot snapshot) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    assert DAG.ALL_IN_ONE_CONTAINER_GROUP_NAME.equals(snapshot.containerGroupId());
    MR3Metrics.ContainerGroupSnapshot.newBuilder()
          .setContainers(snapshot.containers())
          .setQueuedTasks(snapshot.queuedTasks())
          .setRunningTasks(snapshot.runningTasks())
          .setNodes(snapshot.nodes())
          .setHeapBytesMax(snapshot.heapBytesMax())
          .setHeapBytesUsed(snapshot.heapBytesUsed())
          .setHeapWindowBytesMax(snapshot.heapWindowBytesMax())
          .setHeapWindowBytesUsed(snapshot.heapWindowBytesUsed())
          .setHeapWindowUsagePercent(snapshot.heapWindowUsagePercent())
          .setAutoScaleOutThresholdPercent(snapshot.autoScaleOutThresholdPercent())
          .setAutoScaleInThresholdPercent(snapshot.autoScaleInThresholdPercent())
          .setContainersTotal(snapshot.containersTotal())
          .setCompletedTasksTotal(snapshot.completedTasksTotal())
          .setSucceededTasksTotal(snapshot.succeededTasksTotal())
          .setFailedTasksTotal(snapshot.failedTasksTotal())
          .setKilledTasksTotal(snapshot.killedTasksTotal())
          .build()
          .writeTo(bytes);
    return bytes.toByteArray();
  }

  static MetricSnapshotMessage decode(byte subtype, long timestampMillis, byte[] bytes)
      throws IOException {
    ByteArrayInputStream input = new ByteArrayInputStream(bytes);
    if (subtype == LeveldbMetricsStore.APPLICATION_SUBTYPE) {
      MR3Metrics.ApplicationSnapshot s = MR3Metrics.ApplicationSnapshot.parseFrom(input);
      return new MetricSnapshotMessage(timestampMillis, s);
    } else if (subtype == LeveldbMetricsStore.CONTAINER_GROUP_SUBTYPE) {
      MR3Metrics.ContainerGroupSnapshot s = MR3Metrics.ContainerGroupSnapshot.parseFrom(input);
      return new MetricSnapshotMessage(timestampMillis, s);
    }
    throw new IOException("Unknown metric snapshot type: " + subtype);
  }
}
