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
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestLeveldbMetricsStore {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testExpirationRemovesOnlyAttemptsWithoutRetainedSamples() throws Exception {
    long now = System.currentTimeMillis();
    String partiallyRetainedAttempt = "appattempt_1_1_1";
    String expiredAttempt = "appattempt_1_2_1";
    HiveConf conf = createConf();

    LeveldbMetricsStore store = new LeveldbMetricsStore();
    store.initialize(conf);
    store.appendBatch(partiallyRetainedAttempt, 0L, Arrays.asList(
        applicationSnapshot(now - TimeUnit.HOURS.toMillis(2)),
        applicationSnapshot(now)));
    store.appendBatch(expiredAttempt, 0L, Collections.singletonList(
        applicationSnapshot(now - TimeUnit.HOURS.toMillis(2))));
    store.stop();

    // Reopening resets the expiration schedule and runs an expiration pass during initialization.
    store = new LeveldbMetricsStore();
    store.initialize(conf);
    try {
      assertTrue(store.hasAttempt(partiallyRetainedAttempt));
      assertEquals(1, store.getApplicationSnapshots(
          partiallyRetainedAttempt, Long.MIN_VALUE, Long.MAX_VALUE, 10).size());
      assertFalse(store.hasAttempt(expiredAttempt));
      assertTrue(store.getApplicationSnapshots(
          expiredAttempt, Long.MIN_VALUE, Long.MAX_VALUE, 10).isEmpty());
    } finally {
      store.stop();
    }
  }

  @Test
  public void testRejectedBatchDoesNotCreateAttemptMarker() throws Exception {
    HiveConf conf = createConf();
    LeveldbMetricsStore store = new LeveldbMetricsStore();
    store.initialize(conf);
    try {
      String attempt = "appattempt_2_1_1";
      store.appendBatch(attempt, 0L, Collections.singletonList(
          containerSnapshot(System.currentTimeMillis(), "unexpected-container-group")));
      assertFalse(store.hasAttempt(attempt));
    } finally {
      store.stop();
    }
  }

  @Test
  public void testScanReturnsEarliestMaxPoints() throws Exception {
    HiveConf conf = createConf();
    LeveldbMetricsStore store = new LeveldbMetricsStore();
    store.initialize(conf);
    try {
      String attempt = "appattempt_3_1_1";
      long now = System.currentTimeMillis();
      store.appendBatch(attempt, 0L, Arrays.asList(
          applicationSnapshot(now),
          applicationSnapshot(now + 1),
          applicationSnapshot(now + 2)));

      List<MetricSnapshotMessage> snapshots = store.getApplicationSnapshots(
          attempt, now, now + 2, 2);
      assertEquals(2, snapshots.size());
      assertEquals(now, snapshots.get(0).timestampMillis());
      assertEquals(now + 1, snapshots.get(1).timestampMillis());
    } finally {
      store.stop();
    }
  }

  @Test
  public void testApplicationPage() throws Exception {
    HiveConf conf = createConf();
    LeveldbMetricsStore store = new LeveldbMetricsStore();
    store.initialize(conf);
    try {
      String attempt = "appattempt_3_2_1";
      long now = System.currentTimeMillis();
      store.appendBatch(attempt, 0L, Arrays.asList(
          applicationSnapshot(now),
          applicationSnapshot(now + 1),
          applicationSnapshot(now + 2),
          applicationSnapshot(now + 3)));

      MetricSnapshotPage firstPage =
          store.getApplicationSnapshotsPage(attempt, now + 2, now + 3, 1);
      assertEquals(1, firstPage.snapshots().size());
      assertEquals(now + 2, firstPage.snapshots().get(0).timestampMillis());
      assertTrue(firstPage.hasMore());

      MetricSnapshotPage lastPage =
          store.getApplicationSnapshotsPage(attempt, now + 3, now + 3, 1);
      assertEquals(1, lastPage.snapshots().size());
      assertEquals(now + 3, lastPage.snapshots().get(0).timestampMillis());
      assertFalse(lastPage.hasMore());
    } finally {
      store.stop();
    }
  }

  @Test
  public void testPageIncludesAllSnapshotsAtBoundaryTimestamp() throws Exception {
    HiveConf conf = createConf();
    LeveldbMetricsStore store = new LeveldbMetricsStore();
    store.initialize(conf);
    try {
      String attempt = "appattempt_3_3_1";
      long now = System.currentTimeMillis();
      store.appendBatch(attempt, 0L, Arrays.asList(
          applicationSnapshot(now),
          applicationSnapshot(now),
          applicationSnapshot(now + 1)));

      MetricSnapshotPage firstPage =
          store.getApplicationSnapshotsPage(attempt, now, now + 1, 1);
      assertEquals(2, firstPage.snapshots().size());
      assertEquals(now, firstPage.snapshots().get(0).timestampMillis());
      assertEquals(now, firstPage.snapshots().get(1).timestampMillis());
      assertTrue(firstPage.hasMore());

      MetricSnapshotPage secondPage =
          store.getApplicationSnapshotsPage(attempt, now + 1, now + 1, 1);
      assertEquals(1, secondPage.snapshots().size());
      assertEquals(now + 1, secondPage.snapshots().get(0).timestampMillis());
      assertFalse(secondPage.hasMore());
    } finally {
      store.stop();
    }
  }

  @Test
  public void testContainerPageReportsMoreSnapshots() throws Exception {
    HiveConf conf = createConf();
    LeveldbMetricsStore store = new LeveldbMetricsStore();
    store.initialize(conf);
    try {
      String attempt = "appattempt_4_2_1";
      long now = System.currentTimeMillis();
      store.appendBatch(attempt, 0L, Arrays.asList(
          containerSnapshot(now,
              org.apache.hadoop.hive.ql.exec.mr3.dag.DAG.ALL_IN_ONE_CONTAINER_GROUP_NAME),
          containerSnapshot(now + 1,
              org.apache.hadoop.hive.ql.exec.mr3.dag.DAG.ALL_IN_ONE_CONTAINER_GROUP_NAME)));

      MetricSnapshotPage firstPage =
          store.getContainerSnapshotsPage(attempt, now, now + 1, 1);
      assertEquals(1, firstPage.snapshots().size());
      assertEquals(now, firstPage.snapshots().get(0).timestampMillis());
      assertTrue(firstPage.hasMore());
    } finally {
      store.stop();
    }
  }

  @Test
  public void testContainerSnapshotRoundTripAndReplay() throws Exception {
    HiveConf conf = createConf();
    LeveldbMetricsStore store = new LeveldbMetricsStore();
    store.initialize(conf);
    try {
      String attempt = "appattempt_4_1_1";
      long timestamp = System.currentTimeMillis();
      MR3MetricSnapshot snapshot = containerSnapshot(
          timestamp, org.apache.hadoop.hive.ql.exec.mr3.dag.DAG.ALL_IN_ONE_CONTAINER_GROUP_NAME);
      store.appendBatch(attempt, 0L, Collections.singletonList(snapshot));
      store.appendBatch(attempt, 0L, Collections.singletonList(snapshot));

      List<MetricSnapshotMessage> snapshots = store.getContainerSnapshots(
          attempt, timestamp, timestamp, 10);
      assertEquals(1, snapshots.size());
      MetricSnapshotMessage stored = snapshots.get(0);
      assertEquals(timestamp, stored.timestampMillis());
      MR3Metrics.ContainerGroupSnapshot value =
          (MR3Metrics.ContainerGroupSnapshot) stored.snapshot();
      assertEquals(1, value.getContainers());
      assertEquals(0, value.getQueuedTasks());
      assertEquals(1, value.getRunningTasks());
      assertEquals(1, value.getNodes());
      assertEquals(100L, value.getHeapBytesMax());
      assertEquals(50L, value.getHeapBytesUsed());
      assertEquals(100L, value.getHeapWindowBytesMax());
      assertEquals(50L, value.getHeapWindowBytesUsed());
      assertEquals(50, value.getHeapWindowUsagePercent());
      assertEquals(80, value.getAutoScaleOutThresholdPercent());
      assertEquals(20, value.getAutoScaleInThresholdPercent());
      assertEquals(1L, value.getContainersTotal());
      assertEquals(1L, value.getCompletedTasksTotal());
      assertEquals(1L, value.getSucceededTasksTotal());
      assertEquals(0L, value.getFailedTasksTotal());
      assertEquals(0L, value.getKilledTasksTotal());
    } finally {
      store.stop();
    }
  }

  private HiveConf createConf() throws Exception {
    File dbDirectory = temporaryFolder.newFolder();
    HiveConf conf = new HiveConf();
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_MR3_UI_METRICS_LEVELDB_PATH,
        dbDirectory.getAbsolutePath());
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_MR3_UI_METRICS_RETENTION_DURATION, "1h");
    return conf;
  }

  private static MR3MetricSnapshot applicationSnapshot(long timestamp) {
    return new ApplicationMetricSnapshot(timestamp, 1, 2, 1, 0, 0);
  }

  private static MR3MetricSnapshot containerSnapshot(long timestamp, String containerGroupId) {
    return new ContainerGroupMetricSnapshot(
        timestamp, containerGroupId,
        1, 0, 1, 1,
        100L, 50L, 100L, 50L,
        50, 80, 20,
        1L, 1L, 1L, 0L, 0L);
  }
}
