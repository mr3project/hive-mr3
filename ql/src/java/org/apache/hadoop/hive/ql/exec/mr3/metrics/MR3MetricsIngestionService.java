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
  private final long ingestionIntervalMillis;
  private ScheduledExecutorService executorService;
  private ScheduledFuture<?> ingestionTask;
  private MR3SessionClient mr3SessionClient;
  private String applicationAttemptId;
  private long fromIndex = 0L;

  public MR3MetricsIngestionService(MetricsStore store, HiveConf conf) {
    this.store = store;
    ingestionIntervalMillis = conf.getTimeVar(
        HiveConf.ConfVars.HIVE_MR3_UI_METRICS_INGESTION_INTERVAL, TimeUnit.MILLISECONDS);
  }

  public synchronized void start() {
    if (executorService != null) {
      return;
    }

    executorService = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("MR3 metrics ingestion")
            .build());
    ingestionTask = executorService.scheduleWithFixedDelay(
        this::ingestSafely,
      ingestionIntervalMillis, ingestionIntervalMillis, TimeUnit.MILLISECONDS);
  }

  private void ingestSafely() {
    try {
      ingestMetric();
    } catch (Exception e) {
      LOG.warn("Failed to ingest native MR3 metrics", e);
    }
  }

  private void ingestMetric() throws Exception {
    store.expire();

    if (mr3SessionClient == null) {
      MR3Session session = MR3SessionManagerImpl.getInstance().getActiveMR3SessionForMR3UI();
      mr3SessionClient = session == null ? null : session.getMR3SessionClient();
    }
    if (mr3SessionClient == null) {
      return;
    }

    String currentApplicationAttemptId = mr3SessionClient.getAppAttemptIdStr();
    if (!Objects.equals(applicationAttemptId, currentApplicationAttemptId)) {
      applicationAttemptId = currentApplicationAttemptId;
      fromIndex = 0L;
    }

    while (true) {
      scala.collection.immutable.List<MR3MetricSnapshot> received =
          mr3SessionClient.getMetricSnapshots(fromIndex);
      List<MR3MetricSnapshot> snapshots = JavaConverters.seqAsJavaListConverter(
          received).asJava();
      if (snapshots.isEmpty()) {
        return;
      }
      store.appendBatch(applicationAttemptId, fromIndex, snapshots);
      fromIndex += snapshots.size();
    }
  }

  @Override
  public synchronized void close() {
    if (ingestionTask != null) {
      ingestionTask.cancel(true);
      ingestionTask = null;
    }
    if (executorService != null) {
      executorService.shutdownNow();
      try {
        executorService.awaitTermination(ingestionIntervalMillis, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      executorService = null;
    }
  }
}
