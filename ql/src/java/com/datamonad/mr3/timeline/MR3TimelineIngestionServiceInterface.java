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

package com.datamonad.mr3.timeline;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;

/**
 * Receives ordered batches of timeline entities from an MR3 application.
 *
 * <p>Only the active HiveServer2 instance should run the implementation that
 * writes to LevelDB. This guarantees one active writer per timeline database.
 * Because the database is local, its history follows the active HiveServer2
 * instance.</p>
 */
public interface MR3TimelineIngestionServiceInterface extends AutoCloseable {
  TimelinePutResponse appendTimelineEntities(
      String producerApplicationId,
      String producerAttemptId,
      long sequenceNumber,
      List<TimelineEntity> entities) throws IOException;

  /** Stops accepting batches and drains or cancels queued writes. */
  @Override
  void close();
}
