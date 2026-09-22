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
import com.datamonad.mr3.api.client.IndexedMetricSnapshot;
import com.datamonad.mr3.api.client.MR3MetricSnapshot;
import com.datamonad.mr3.client.DAGClientHandlerProtocolRPC.ApplicationMetricSnapshotProto;
import com.datamonad.mr3.client.DAGClientHandlerProtocolRPC.ContainerGroupMetricSnapshotProto;
import com.datamonad.mr3.client.DAGClientHandlerProtocolRPC.IndexedMetricSnapshotProto;
import java.io.IOException;

final class MetricProtoUtils {
  private MetricProtoUtils() {}

  static byte[] encode(IndexedMetricSnapshot indexed) {
    IndexedMetricSnapshotProto.Builder builder = IndexedMetricSnapshotProto.newBuilder()
        .setPublisherIndex(indexed.publisherIndex());
    MR3MetricSnapshot snapshot = indexed.snapshot();
    if (snapshot instanceof ApplicationMetricSnapshot) {
      ApplicationMetricSnapshot s = (ApplicationMetricSnapshot) snapshot;
      builder.setApplication(ApplicationMetricSnapshotProto.newBuilder()
          .setTimestampMillis(s.timestampMillis()).setRunningDags(s.runningDags())
          .setTotalDags(s.totalDags()).setSucceededDags(s.succeededDags())
          .setFailedDags(s.failedDags()).setKilledDags(s.killedDags()));
    } else if (snapshot instanceof ContainerGroupMetricSnapshot) {
      ContainerGroupMetricSnapshot s = (ContainerGroupMetricSnapshot) snapshot;
      builder.setContainerGroup(ContainerGroupMetricSnapshotProto.newBuilder()
          .setTimestampMillis(s.timestampMillis()).setContainerGroupId(s.containerGroupId())
          .setContainers(s.containers()).setQueuedTasks(s.queuedTasks()).setRunningTasks(s.runningTasks())
          .setNodes(s.nodes()).setHeapBytesMax(s.heapBytesMax()).setHeapBytesUsed(s.heapBytesUsed())
          .setHeapWindowBytesMax(s.heapWindowBytesMax()).setHeapWindowBytesUsed(s.heapWindowBytesUsed())
          .setHeapWindowUsagePercent(s.heapWindowUsagePercent())
          .setAutoScaleOutThresholdPercent(s.autoScaleOutThresholdPercent())
          .setAutoScaleInThresholdPercent(s.autoScaleInThresholdPercent())
          .setContainersTotal(s.containersTotal()).setCompletedTasksTotal(s.completedTasksTotal())
          .setSucceededTasksTotal(s.succeededTasksTotal()).setFailedTasksTotal(s.failedTasksTotal())
          .setKilledTasksTotal(s.killedTasksTotal()));
    } else {
      throw new IllegalArgumentException("Unknown MR3 metric snapshot type: " + snapshot.getClass());
    }
    return builder.build().toByteArray();
  }

  static IndexedMetricSnapshot decode(byte[] bytes) throws IOException {
    IndexedMetricSnapshotProto indexed = IndexedMetricSnapshotProto.parseFrom(bytes);
    MR3MetricSnapshot snapshot;
    if (indexed.hasApplication() && !indexed.hasContainerGroup()) {
      ApplicationMetricSnapshotProto s = indexed.getApplication();
      snapshot = new ApplicationMetricSnapshot(s.getTimestampMillis(), s.getRunningDags(),
          s.getTotalDags(), s.getSucceededDags(), s.getFailedDags(), s.getKilledDags());
    } else if (!indexed.hasApplication() && indexed.hasContainerGroup()) {
      ContainerGroupMetricSnapshotProto s = indexed.getContainerGroup();
      snapshot = new ContainerGroupMetricSnapshot(s.getTimestampMillis(), s.getContainerGroupId(),
          s.getContainers(), s.getQueuedTasks(), s.getRunningTasks(), s.getNodes(),
          s.getHeapBytesMax(), s.getHeapBytesUsed(), s.getHeapWindowBytesMax(),
          s.getHeapWindowBytesUsed(), s.getHeapWindowUsagePercent(),
          s.getAutoScaleOutThresholdPercent(), s.getAutoScaleInThresholdPercent(),
          s.getContainersTotal(), s.getCompletedTasksTotal(), s.getSucceededTasksTotal(),
          s.getFailedTasksTotal(), s.getKilledTasksTotal());
    } else {
      throw new IOException("Metric snapshot must contain exactly one typed payload");
    }
    return new IndexedMetricSnapshot(indexed.getPublisherIndex(), snapshot);
  }
}
