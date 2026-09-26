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
import java.util.List;
import org.apache.hadoop.hive.conf.HiveConf;

public interface MetricsStore {
  void initialize(HiveConf conf) throws Exception;

  void appendBatch(
      String applicationAttemptId, long fromIndex, List<MR3MetricSnapshot> snapshots) throws Exception;

  /**
   * Returns up to {@code maxPoints} of the earliest application snapshots in the inclusive time
   * range, ordered chronologically.
   */
  List<MetricSnapshotMessage> getApplicationSnapshots(
      String applicationAttemptId, long startTime, long endTime, int maxPoints) throws Exception;

  /**
   * Returns a page of application snapshots and whether a later page exists. All snapshots with
   * the timestamp at the {@code maxPoints} boundary are included, so the page may exceed
   * {@code maxPoints}.
   */
  MetricSnapshotPage getApplicationSnapshotsPage(
      String applicationAttemptId, long startTime, long endTime, int maxPoints) throws Exception;

  /**
   * Returns up to {@code maxPoints} of the earliest container snapshots in the inclusive time
   * range, ordered chronologically.
   */
  List<MetricSnapshotMessage> getContainerSnapshots(
      String applicationAttemptId, long startTime, long endTime, int maxPoints) throws Exception;

  /**
   * Returns a page of container snapshots and whether a later page exists. All snapshots with the
   * timestamp at the {@code maxPoints} boundary are included, so the page may exceed
   * {@code maxPoints}.
   */
  MetricSnapshotPage getContainerSnapshotsPage(
      String applicationAttemptId, long startTime, long endTime, int maxPoints) throws Exception;

  MetricSnapshotMessage getLatestApplicationSnapshot(String applicationAttemptId) throws Exception;

  MetricSnapshotMessage getLatestContainerSnapshot(String applicationAttemptId) throws Exception;

  boolean hasAttempt(String applicationAttemptId) throws Exception;

  void expire() throws Exception;

  void stop() throws Exception;
}
