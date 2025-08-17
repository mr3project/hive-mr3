package org.apache.hadoop.hive.llap.io.session.heap;

import org.apache.hadoop.hive.llap.io.session.LowLevelCache;
import org.apache.hadoop.hive.llap.io.session.MetadataCache;

public final class HeapCacheFactory {
  private HeapCacheFactory() {}

  public static LowLevelCache createDataCache(long capacity) {
    return new OnHeapLowLevelCache(capacity);
  }

  public static MetadataCache createMetadataCache(long capacity) {
    return new OnHeapMetadataCache(capacity);
  }
}
