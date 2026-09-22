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

      List<MR3MetricSnapshot> snapshots = store.getApplicationSnapshots(
          attempt, now, now + 2, 2);
      assertEquals(2, snapshots.size());
      assertEquals(now, snapshots.get(0).timestampMillis());
      assertEquals(now + 1, snapshots.get(1).timestampMillis());
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
        50.0f, 80.0f, 20.0f,
        1L, 1L, 1L, 0L, 0L);
  }
}
