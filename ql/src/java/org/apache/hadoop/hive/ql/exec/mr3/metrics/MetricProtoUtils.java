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
import com.datamonad.mr3.client.DAGClientHandlerProtocolRPC.ApplicationMetricSnapshotProto;
import com.datamonad.mr3.client.DAGClientHandlerProtocolRPC.ContainerGroupMetricSnapshotProto;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.hadoop.hive.ql.exec.mr3.dag.DAG;

final class MetricProtoUtils {
  private MetricProtoUtils() {}

  static byte[] encode(MR3MetricSnapshot snapshot) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    if (snapshot instanceof ApplicationMetricSnapshot) {
      ApplicationMetricSnapshot s = (ApplicationMetricSnapshot) snapshot;
      bytes.write(LeveldbMetricsStore.APPLICATION_SUBTYPE);
      ApplicationMetricSnapshotProto.newBuilder()
          .setTimestampMillis(s.timestampMillis())
          .setRunningDags(s.runningDags())
          .setTotalDags(s.totalDags())
          .setSucceededDags(s.succeededDags())
          .setFailedDags(s.failedDags())
          .setKilledDags(s.killedDags())
          .build()
          .writeTo(bytes);
    } else {
      ContainerGroupMetricSnapshot s = (ContainerGroupMetricSnapshot) snapshot;
      assert DAG.ALL_IN_ONE_CONTAINER_GROUP_NAME.equals(s.containerGroupId());
      bytes.write(LeveldbMetricsStore.CONTAINER_GROUP_SUBTYPE);
      ContainerGroupMetricSnapshotProto.newBuilder()
          .setTimestampMillis(s.timestampMillis())
          .setContainerGroupId(s.containerGroupId())
          .setContainers(s.containers())
          .setQueuedTasks(s.queuedTasks())
          .setRunningTasks(s.runningTasks())
          .setNodes(s.nodes())
          .setHeapBytesMax(s.heapBytesMax())
          .setHeapBytesUsed(s.heapBytesUsed())
          .setHeapWindowBytesMax(s.heapWindowBytesMax())
          .setHeapWindowBytesUsed(s.heapWindowBytesUsed())
          .setHeapWindowUsagePercent(s.heapWindowUsagePercent())
          .setAutoScaleOutThresholdPercent(s.autoScaleOutThresholdPercent())
          .setAutoScaleInThresholdPercent(s.autoScaleInThresholdPercent())
          .setContainersTotal(s.containersTotal())
          .setCompletedTasksTotal(s.completedTasksTotal())
          .setSucceededTasksTotal(s.succeededTasksTotal())
          .setFailedTasksTotal(s.failedTasksTotal())
          .setKilledTasksTotal(s.killedTasksTotal())
          .build()
          .writeTo(bytes);
    }
    return bytes.toByteArray();
  }

  static MR3MetricSnapshot decode(byte[] bytes) throws IOException {
    if (bytes.length == 0) {
      throw new IOException("Empty metric snapshot payload");
    }
    ByteArrayInputStream input = new ByteArrayInputStream(bytes, 1, bytes.length - 1);
    if (bytes[0] == LeveldbMetricsStore.APPLICATION_SUBTYPE) {
      ApplicationMetricSnapshotProto s = ApplicationMetricSnapshotProto.parseFrom(input);
      return new ApplicationMetricSnapshot(
          s.getTimestampMillis(),
          s.getRunningDags(),
          s.getTotalDags(),
          s.getSucceededDags(),
          s.getFailedDags(),
          s.getKilledDags());
    } else if (bytes[0] == LeveldbMetricsStore.CONTAINER_GROUP_SUBTYPE) {
      ContainerGroupMetricSnapshotProto s = ContainerGroupMetricSnapshotProto.parseFrom(input);
      assert DAG.ALL_IN_ONE_CONTAINER_GROUP_NAME.equals(s.getContainerGroupId());
      return new ContainerGroupMetricSnapshot(
          s.getTimestampMillis(),
          s.getContainerGroupId(),
          s.getContainers(),
          s.getQueuedTasks(),
          s.getRunningTasks(),
          s.getNodes(),
          s.getHeapBytesMax(),
          s.getHeapBytesUsed(),
          s.getHeapWindowBytesMax(),
          s.getHeapWindowBytesUsed(),
          s.getHeapWindowUsagePercent(),
          s.getAutoScaleOutThresholdPercent(),
          s.getAutoScaleInThresholdPercent(),
          s.getContainersTotal(),
          s.getCompletedTasksTotal(),
          s.getSucceededTasksTotal(),
          s.getFailedTasksTotal(),
          s.getKilledTasksTotal());
    }
    throw new IOException("Unknown metric snapshot type: " + bytes[0]);
  }
}
