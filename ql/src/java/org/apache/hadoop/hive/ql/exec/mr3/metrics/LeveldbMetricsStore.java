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

import com.datamonad.mr3.api.client.ContainerGroupMetricSnapshot;
import com.datamonad.mr3.api.client.MR3MetricSnapshot;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.mr3.dag.DAG;
import org.apache.hadoop.io.IOUtils;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeveldbMetricsStore implements MetricsStore {

  private static final Logger LOG = LoggerFactory.getLogger(LeveldbMetricsStore.class);

  public static final byte APPLICATION_SUBTYPE = 1;
  public static final byte CONTAINER_GROUP_SUBTYPE = 2;

  private static final byte SAMPLE = 'S';
  private static final byte ATTEMPT = 'A';

  private DB db;
  private long retentionMillis;
  private long nextExpirationMillis;

  @Override
  public synchronized void initialize(HiveConf conf) throws Exception {
    retentionMillis = conf.getTimeVar(
        HiveConf.ConfVars.HIVE_MR3_UI_METRICS_RETENTION_DURATION, TimeUnit.MILLISECONDS);

    Path path = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_MR3_UI_METRICS_LEVELDB_PATH));
    FileSystem fs = null;
    try {
      fs = FileSystem.getLocal(conf);
      if (!fs.exists(path) && !fs.mkdirs(path)) {
        throw new IOException("Could not create MR3 metrics LevelDB directory " + path);
      }
    } finally {
      IOUtils.cleanupWithLogger(LOG, fs);
    }
    Options options = new Options().createIfMissing(true);
    db = new JniDBFactory().open(new File(path.toString()), options);
    expire();
  }

  @Override
  public synchronized void appendBatch(
      String attemptId, long fromIndex, List<MR3MetricSnapshot> snapshots) throws Exception {
    assert fromIndex >= 0;
    try (WriteBatch writes = db.createWriteBatch()) {
      boolean hasAcceptedSnapshot = false;

      for (int i = 0; i < snapshots.size(); ++i) {
        MR3MetricSnapshot snapshot = snapshots.get(i);
        long snapshotIndex = fromIndex + i;

        boolean isContainer = snapshot instanceof ContainerGroupMetricSnapshot;
        if (isContainer) {
          String containerGroupId = ((ContainerGroupMetricSnapshot) snapshot).containerGroupId();
          if (!DAG.ALL_IN_ONE_CONTAINER_GROUP_NAME.equals(containerGroupId)) {
            LOG.warn("Ignoring malformed MR3 metric snapshot for attempt {} at index {}: " +
                    "expected container group {}, but found {}",
                attemptId, snapshotIndex, DAG.ALL_IN_ONE_CONTAINER_GROUP_NAME, containerGroupId);
            continue;
          }
        }
        hasAcceptedSnapshot = true;

        byte[] value = MetricProtoUtils.encode(snapshot);
        byte subtype = (byte) (isContainer ? CONTAINER_GROUP_SUBTYPE : APPLICATION_SUBTYPE);
        writes.put(sampleKey(
            attemptId, subtype, snapshot.timestampMillis(), snapshotIndex), value);
      }
      if (hasAcceptedSnapshot) {
        writes.put(attemptKey(attemptId), new byte[0]);
      }
      db.write(writes);
    }
    expire();
  }

  @Override
  public synchronized List<MetricSnapshotMessage> getApplicationSnapshots(
      String attemptId, long startTime, long endTime, int maxPoints) throws Exception {
    return scan(attemptId, APPLICATION_SUBTYPE, startTime, endTime, maxPoints);
  }

  @Override
  public synchronized List<MetricSnapshotMessage> getContainerSnapshots(
      String attemptId, long startTime, long endTime, int maxPoints) throws Exception {
    return scan(attemptId, CONTAINER_GROUP_SUBTYPE, startTime, endTime, maxPoints);
  }

  @Override
  public synchronized MetricSnapshotMessage getLatestApplicationSnapshot(String attemptId)
      throws Exception {
    return latest(attemptId, APPLICATION_SUBTYPE);
  }

  @Override
  public synchronized MetricSnapshotMessage getLatestContainerSnapshot(String attemptId)
      throws Exception {
    return latest(attemptId, CONTAINER_GROUP_SUBTYPE);
  }

  private MetricSnapshotMessage latest(String attemptId, byte subtype) throws Exception {
    byte[] prefix = samplePrefix(attemptId, subtype);
    MetricSnapshotMessage latest = null;
    try (DBIterator iterator = db.iterator()) {
      iterator.seek(prefix);
      while (iterator.hasNext()) {
        Map.Entry<byte[], byte[]> entry = iterator.next();
        if (!startsWith(entry.getKey(), prefix)) break;
        latest = MetricProtoUtils.decode(subtype, readTimestamp(entry.getKey()), entry.getValue());
      }
    }
    return latest;
  }

  private List<MetricSnapshotMessage> scan(
      String attemptId, byte subtype,
      long startTime, long endTime, int maxPoints) throws Exception {
    if (maxPoints <= 0) {
      throw new IllegalArgumentException("maxPoints must be greater than 0");
    }

    byte[] prefix = samplePrefix(attemptId, subtype);
    List<MetricSnapshotMessage> result = new ArrayList<>(maxPoints);
    try (DBIterator iterator = db.iterator()) {
      iterator.seek(sampleKey(attemptId, subtype, startTime, Long.MIN_VALUE));
      while (iterator.hasNext() && result.size() < maxPoints) {
        Map.Entry<byte[], byte[]> entry = iterator.next();
        if (!startsWith(entry.getKey(), prefix)) {
          break;
        }

        long timestampMillis = readTimestamp(entry.getKey());
        if (timestampMillis > endTime) {
          break;
        }
        result.add(MetricProtoUtils.decode(subtype, timestampMillis, entry.getValue()));
        assert result.size() <= maxPoints;
      }
    }

    return result;
  }

  @Override
  public synchronized boolean hasAttempt(String attemptId) throws Exception {
    return db.get(attemptKey(attemptId)) != null;
  }

  @Override
  public synchronized void expire() throws Exception {
    long now = System.currentTimeMillis();
    if (now < nextExpirationMillis) {
      return;
    }
    nextExpirationMillis = now + Math.min(TimeUnit.HOURS.toMillis(1), retentionMillis);
    long cutoff = System.currentTimeMillis() - retentionMillis;
    Set<String> retainedAttempts = new HashSet<>();
    try (WriteBatch deletes = db.createWriteBatch()) {
      try (DBIterator iterator = db.iterator()) {
        iterator.seek(new byte[] {SAMPLE});
        while (iterator.hasNext()) {
          Map.Entry<byte[], byte[]> entry = iterator.next();
          if (entry.getKey()[0] != SAMPLE) break;
          String attempt = readAttempt(entry.getKey());
          if (readTimestamp(entry.getKey()) < cutoff) {
            deletes.delete(entry.getKey());
          } else {
            retainedAttempts.add(attempt);
          }
        }
      }
      try (DBIterator iterator = db.iterator()) {
        iterator.seek(new byte[] {ATTEMPT});
        while (iterator.hasNext()) {
          Map.Entry<byte[], byte[]> entry = iterator.next();
          if (entry.getKey()[0] != ATTEMPT) break;
          if (!retainedAttempts.contains(readAttempt(entry.getKey()))) {
            deletes.delete(entry.getKey());
          }
        }
      }
      db.write(deletes);
    }
  }

  @Override
  public synchronized void stop() throws Exception {
    if (db != null) {
      db.close();
      db = null;
    }
  }

  private static byte[] attemptKey(String attempt) throws IOException {
    return keyPrefix(ATTEMPT, attempt);
  }

  private static byte[] samplePrefix(String attempt, byte subtype) throws IOException {
    return key(SAMPLE, attempt, subtype, null, null);
  }

  private static byte[] sampleKey(String attempt, byte subtype, long timestamp,
      long index) throws IOException {
    return key(SAMPLE, attempt, subtype, timestamp, index);
  }

  private static byte[] keyPrefix(byte kind, String attempt) throws IOException {
    return key(kind, attempt, (byte) 0, null, null);
  }

  private static byte[] key(
      byte kind, String attempt, byte subtype, Long timestamp, Long index) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytes);
    out.writeByte(kind); out.writeUTF(attempt);
    if (kind == SAMPLE) {
      out.writeByte(subtype);
    }
    if (timestamp != null) {
      out.writeLong(timestamp ^ Long.MIN_VALUE);
    }
    if (index != null) {
      out.writeLong(index ^ Long.MIN_VALUE);
    }
    out.close();
    return bytes.toByteArray();
  }

  private static String readAttempt(byte[] key) throws IOException {
    return new java.io.DataInputStream(new java.io.ByteArrayInputStream(key, 1, key.length - 1)).readUTF();
  }

  private static long readTimestamp(byte[] key) throws IOException {
    assert key.length >= 2 * Long.BYTES;
    return new java.io.DataInputStream(new java.io.ByteArrayInputStream(
        key, key.length - 2 * Long.BYTES, Long.BYTES)).readLong() ^ Long.MIN_VALUE;
  }

  private static boolean startsWith(byte[] value, byte[] prefix) {
    if (value.length < prefix.length) {
      return false;
    }
    for (int i = 0; i < prefix.length; ++i) if (value[i] != prefix[i]) {
      return false;
    }
    return true;
  }
}
