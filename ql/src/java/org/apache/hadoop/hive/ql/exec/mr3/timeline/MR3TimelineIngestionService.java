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
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hive.ql.exec.mr3.session.MR3Session;
import org.apache.hadoop.hive.ql.exec.mr3.session.MR3SessionManagerImpl;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MR3TimelineIngestionService implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(MR3TimelineIngestionService.class);
  private static final long INGESTION_INTERVAL_MILLIS = 1000L;

  private final TimelineDataManager timelineDataManager;
  private final AtomicBoolean stopped = new AtomicBoolean();
  private Thread ingestionThread;

  public MR3TimelineIngestionService(TimelineDataManager timelineDataManager) {
    this.timelineDataManager = timelineDataManager;
  }

  public synchronized void start() {
    if (ingestionThread != null) {
      return;
    }

    ingestionThread = new Thread(this::run, "MR3 timeline ingestion");
    ingestionThread.setDaemon(true);
    ingestionThread.start();
  }

  private void run() {
    while (!stopped.get()) {
      try {
        Thread.sleep(INGESTION_INTERVAL_MILLIS);
        ingestTimelineEvents();
      } catch (InterruptedException e) {
        if (stopped.get()) {
          return;
        }
      } catch (Exception e) {
        LOG.warn("Failed to ingest MR3 timeline events", e);
      }
    }
  }

  private void ingestTimelineEvents() throws Exception {
    MR3Session mr3Session = MR3SessionManagerImpl.getInstance().getActiveMR3SessionForMR3UI();
    MR3SessionClient mr3SessionClient =
        mr3Session == null ? null : mr3Session.getMR3SessionClient();
    if (mr3SessionClient == null) {
      return;
    }

    // TODO: Fetch timeline events from mr3SessionClient and append them to timelineDataManager.
  }

  public TimelinePutResponse appendTimelineEntities(
      String applicationAttemptId,
      long sequenceNumber,
      List<TimelineEntity> entities) throws IOException {
    timelineDataManager.postEntities(entities);

    return new TimelinePutResponse();
  }

  @Override
  public synchronized void close() {
    stopped.set(true);
    if (ingestionThread != null) {
      ingestionThread.interrupt();
      ingestionThread = null;
    }
  }
}
