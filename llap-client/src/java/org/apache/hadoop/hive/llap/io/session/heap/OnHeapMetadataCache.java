package org.apache.hadoop.hive.llap.io.session.heap;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.llap.io.session.MetadataCache;

/** Simple on-heap metadata cache. */
public class OnHeapMetadataCache implements MetadataCache {
  private final ConcurrentHashMap<Object, Object> map = new ConcurrentHashMap<>();
  private final long capacity;
  private volatile long used;

  public OnHeapMetadataCache(long capacity) {
    this.capacity = capacity;
  }

  @Override
  public Object get(Object key) {
    return map.get(key);
  }

  @Override
  public void put(Object key, Object value, long weight) {
    map.put(key, value);
    used += weight;
  }

  @Override
  public void clear() {
    map.clear();
    used = 0;
  }

  @Override
  public long usedBytes() { return used; }

  @Override
  public long capacityBytes() { return capacity; }
}
