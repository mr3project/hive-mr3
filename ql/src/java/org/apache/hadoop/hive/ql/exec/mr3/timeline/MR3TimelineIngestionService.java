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

package org.apache.hadoop.hive.ql.exec.mr3.timeline;

import com.datamonad.mr3.api.client.MR3SessionClient;
import com.datamonad.mr3.api.common.MR3Exception;
import com.datamonad.mr3.history.MR3TimelineDataPublisher;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.mr3.session.MR3Session;
import org.apache.hadoop.hive.ql.exec.mr3.session.MR3SessionManagerImpl;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

public class MR3TimelineIngestionService implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(MR3TimelineIngestionService.class);
  private static final int MAX_NUM_ENTITIES_PER_REQUEST =
      MR3TimelineDataPublisher.maxNumEntitiesPerRequest();

  private final TimelineDataManager timelineDataManager;
  private final long ingestionIntervalMillis;
  private ScheduledExecutorService executorService;
  private ScheduledFuture<?> ingestionTask;
  private String applicationAttemptId;
  private long fromIndex = 0;

  public MR3TimelineIngestionService(TimelineDataManager timelineDataManager, HiveConf conf) {
    this.timelineDataManager = timelineDataManager;
    this.ingestionIntervalMillis = conf.getTimeVar(
        HiveConf.ConfVars.HIVE_MR3_TIMELINE_INGESTION_INTERVAL, TimeUnit.MILLISECONDS);
  }

  public synchronized void start() {
    if (executorService != null) {
      return;
    }

    executorService = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("MR3 timeline ingestion")
            .build());
    ingestionTask = executorService.scheduleWithFixedDelay(
        this::ingest,
        ingestionIntervalMillis,
        ingestionIntervalMillis,
        TimeUnit.MILLISECONDS);
  }

  private void ingest() {
    try {
      ingestTimelineEvents();
    } catch (MR3Exception e) {
      LOG.warn("Failed to ingest MR3 timeline events: {}", e.getMessage());
    } catch (Exception e) {
      LOG.warn("Failed to ingest MR3 timeline events", e);
    }
  }

  private void ingestTimelineEvents() throws Exception {
    MR3Session mr3Session = MR3SessionManagerImpl.getInstance().getActiveMR3SessionForMR3UI();
    MR3SessionClient mr3SessionClient = mr3Session == null ? null : mr3Session.getMR3SessionClient();
    if (mr3SessionClient == null) {
      return;
    }

    String currentApplicationAttemptId = mr3SessionClient.getAppAttemptIdStr();
    if (!Objects.equals(applicationAttemptId, currentApplicationAttemptId)) {
      applicationAttemptId = currentApplicationAttemptId;
      fromIndex = 0;
    }

    int numEntities;
    do {
      scala.collection.immutable.List<TimelineEntity> timelineEntities =
          mr3SessionClient.getTimelineDataEntities(fromIndex);
      List<TimelineEntity> entities =
          JavaConverters.seqAsJavaListConverter(timelineEntities).asJava();
      numEntities = entities.size();
      if (numEntities > 0) {
        for (TimelineEntity entity : entities) {
          if (TimelineEntityDiagnostics.isDag(entity)) {
            LOG.info("xxx MR3 timeline diagnostics after AM call: fromIndex={} {}",
                fromIndex, TimelineEntityDiagnostics.describeDag(entity));
          }
        }
        appendTimelineEntities(applicationAttemptId, fromIndex, entities);
        fromIndex += numEntities;
      }
    } while (numEntities == MAX_NUM_ENTITIES_PER_REQUEST);
  }

  private TimelinePutResponse appendTimelineEntities(
      String applicationAttemptId,
      long sequenceNumber,
      List<TimelineEntity> entities) throws IOException {
    TimelinePutResponse response = timelineDataManager.postEntities(entities);
    for (TimelineEntity incomingEntity : entities) {
      if (TimelineEntityDiagnostics.isDag(incomingEntity)) {
        try {
          UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
          TimelineEntity storedEntity = timelineDataManager.getEntity(
              incomingEntity.getEntityType(), incomingEntity.getEntityId(), null, currentUser);
          LOG.info("xxx MR3 timeline diagnostics after store: applicationAttemptId={} sequenceNumber={} "
                  + "incoming=[{}] stored=[{}]",
              applicationAttemptId, sequenceNumber,
              TimelineEntityDiagnostics.describeDag(incomingEntity),
              TimelineEntityDiagnostics.describeDag(storedEntity));
        } catch (IOException e) {
          // Diagnostics must not cause a successfully stored batch to be fetched again.
          LOG.warn("xxx Unable to read MR3 DAG {} after store for timeline diagnostics",
              incomingEntity.getEntityId(), e);
        }
      }
    }
    return response;
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
        if (!executorService.awaitTermination(
            ingestionIntervalMillis, TimeUnit.MILLISECONDS)) {
          LOG.warn("MR3 timeline ingestion worker did not stop in time");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      executorService = null;
    }
  }
}
