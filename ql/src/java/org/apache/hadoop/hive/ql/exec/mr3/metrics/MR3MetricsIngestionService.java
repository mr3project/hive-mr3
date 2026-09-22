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

import com.datamonad.mr3.api.client.MR3MetricSnapshot;
import com.datamonad.mr3.api.client.MR3SessionClient;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.mr3.session.MR3Session;
import org.apache.hadoop.hive.ql.exec.mr3.session.MR3SessionManagerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

public class MR3MetricsIngestionService implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(MR3MetricsIngestionService.class);
  private final MetricsStore store;
  private final long intervalMillis;
  private ScheduledExecutorService executor;
  private ScheduledFuture<?> task;
  private MR3SessionClient client;
  private String attemptId;
  private long fromIndex;

  public MR3MetricsIngestionService(MetricsStore store, HiveConf conf) {
    this.store = store;
    intervalMillis = conf.getTimeVar(
        HiveConf.ConfVars.HIVE_MR3_UI_METRICS_INGESTION_INTERVAL, TimeUnit.MILLISECONDS);
  }

  public synchronized void start() {
    if (executor != null) {
      return;
    }
    executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat("MR3 metrics ingestion").build());
    task = executor.scheduleWithFixedDelay(this::ingestSafely, intervalMillis,
        intervalMillis, TimeUnit.MILLISECONDS);
  }

  private void ingestSafely() {
    try {
      ingest();
    } catch (Exception e) {
      LOG.warn("Failed to ingest native MR3 metrics", e);
    }
  }

  private void ingest() throws Exception {
    store.expire();
    if (client == null) {
      MR3Session session = MR3SessionManagerImpl.getInstance().getActiveMR3SessionForMR3UI();
      client = session == null ? null : session.getMR3SessionClient();
    }
    if (client == null) return;
    String currentAttempt = client.getAppAttemptIdStr();
    if (!Objects.equals(attemptId, currentAttempt)) {
      attemptId = currentAttempt;
      fromIndex = 0L;
    }

    while (true) {
      scala.collection.immutable.List<MR3MetricSnapshot> received =
          client.getMetricSnapshots(fromIndex);
      List<MR3MetricSnapshot> snapshots = JavaConverters.seqAsJavaListConverter(
          received).asJava();
      if (snapshots.isEmpty()) return;
      assert snapshots.stream().allMatch(Objects::nonNull);
      store.appendBatch(attemptId, fromIndex, snapshots);
      fromIndex += snapshots.size();
      assert fromIndex > 0;
    }
  }

  @Override
  public synchronized void close() {
    if (task != null) task.cancel(true);
    if (executor != null) {
      executor.shutdownNow();
      try {
        executor.awaitTermination(intervalMillis, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    task = null;
    executor = null;
  }
}
