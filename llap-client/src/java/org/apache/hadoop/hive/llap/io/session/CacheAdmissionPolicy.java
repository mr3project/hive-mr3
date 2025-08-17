package org.apache.hadoop.hive.llap.io.session;

import org.apache.hadoop.hive.llap.cache.FileCacheKey;
import org.apache.hadoop.hive.common.io.DiskRange;

public interface CacheAdmissionPolicy {
  /** Return true if this range should be admitted into the cache under current conditions. */
  boolean shouldCache(FileCacheKey key, DiskRange range, long estimatedBytes, CacheStats stats, CacheTag tag);

  interface CacheStats {
    long usedBytes();
    long capacityBytes();
  }

  /** Default allow-all policy. */
  CacheAdmissionPolicy ALWAYS = (k, r, est, s, t) -> true;
}
