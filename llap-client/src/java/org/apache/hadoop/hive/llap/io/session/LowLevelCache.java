package org.apache.hadoop.hive.llap.io.session;

import java.nio.ByteBuffer;

/** Minimal data cache interface for per-session cache. */
public interface LowLevelCache {
  /** Returns a retained entry or null on miss. */
  ByteBuffer get(Object chunkKey);
  /** Inserts the entry if admitted; returns true if inserted. */
  boolean put(Object chunkKey, ByteBuffer buf);
  long usedBytes();
  long capacityBytes();
  void clear();
  void incRef(ByteBuffer buf);
  void decRef(ByteBuffer buf);
}
