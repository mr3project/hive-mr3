package org.apache.hadoop.hive.llap.io.session;

import java.util.concurrent.*;
import java.util.function.Supplier;

/** Deduplicates concurrent loads of the same chunk within a CacheContext. */
public final class InflightTracker {
  private final ConcurrentHashMap<Object, CompletableFuture<Object>> map = new ConcurrentHashMap<>();

  @SuppressWarnings("unchecked")
  public <K, V> V getOrLoad(K key, Supplier<V> loader) {
    CompletableFuture<Object> f = map.computeIfAbsent(key, k -> new CompletableFuture<>());
    // Fast path: already completed by another thread.
    if (f.isDone()) return (V) f.join();

    // Attempt to be the loader; others await the same future.
    boolean doLoad = f.completeAsync(() -> {
      V v = loader.get();
      return v;
    }).isDone();

    try {
      return (V) f.join();
    } finally {
      map.remove(key, f);
    }
  }
}
