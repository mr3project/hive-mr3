package org.apache.hadoop.hive.llap.io.session.heap;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.llap.io.session.LowLevelCache;

/**
 * Simple on-heap cache with LRU eviction and reference counting.
 */
public class OnHeapLowLevelCache implements LowLevelCache {
  private static final class Entry {
    final ByteBuffer buf;
    int refCount = 1; // held by cache itself
    Entry(ByteBuffer b) { this.buf = b; }
  }

  private final long capacity;
  private volatile long used;

  private final ConcurrentHashMap<Object, Entry> map = new ConcurrentHashMap<>();
  private final LinkedHashMap<Object, Entry> lru = new LinkedHashMap<>(16, 0.75f, true);

  public OnHeapLowLevelCache(long capacity) {
    this.capacity = capacity;
  }

  @Override
  public ByteBuffer get(Object chunkKey) {
    Entry e = map.get(chunkKey);
    if (e == null) return null;
    synchronized (e) { e.refCount++; }
    synchronized (lru) { lru.get(chunkKey); }
    return e.buf.duplicate();
  }

  @Override
  public boolean put(Object chunkKey, ByteBuffer buf) {
    int size = buf.remaining();
    Entry e = new Entry(buf.slice());
    Entry prev = map.putIfAbsent(chunkKey, e);
    if (prev != null) {
      return false;
    }
    used += size;
    synchronized (lru) { lru.put(chunkKey, e); }
    evictIfNeeded();
    return true;
  }

  private void evictIfNeeded() {
    synchronized (lru) {
      while (used > capacity && !lru.isEmpty()) {
        Map.Entry<Object, Entry> eldest = lru.entrySet().iterator().next();
        Entry e = eldest.getValue();
        synchronized (e) {
          if (e.refCount > 1) {
            // in use, move to end and continue
            lru.remove(eldest.getKey());
            lru.put(eldest.getKey(), e);
            continue;
          }
        }
        lru.remove(eldest.getKey());
        map.remove(eldest.getKey());
        used -= e.buf.remaining();
      }
    }
  }

  @Override
  public long usedBytes() { return used; }

  @Override
  public long capacityBytes() { return capacity; }

  @Override
  public void clear() {
    map.clear();
    synchronized (lru) { lru.clear(); }
    used = 0;
  }

  @Override
  public void incRef(ByteBuffer buf) {
    // find entry by buffer identity
    map.forEach((k, e) -> {
      if (e.buf == buf) {
        synchronized (e) { e.refCount++; }
      }
    });
  }

  @Override
  public void decRef(ByteBuffer buf) {
    map.forEach((k, e) -> {
      if (e.buf == buf) {
        synchronized (e) { e.refCount--; }
      }
    });
  }
}
