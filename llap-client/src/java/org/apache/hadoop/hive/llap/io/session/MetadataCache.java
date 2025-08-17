package org.apache.hadoop.hive.llap.io.session;

/** Minimal metadata cache interface for per-session use. */
public interface MetadataCache {
  Object get(Object key);
  void put(Object key, Object value, long weight);
  void clear();
  long usedBytes();
  long capacityBytes();
}
