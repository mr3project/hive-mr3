package org.apache.hadoop.hive.llap.io.session;

import org.apache.hadoop.hive.llap.io.session.LowLevelCache;
import org.apache.hadoop.hive.llap.io.session.MetadataCache;

/** Immutable routing handle for all cache operations. */
public final class CacheContext {
  public final LowLevelCache dataCache;
  public final MetadataCache metadataCache;
  public final CacheAdmissionPolicy admission;
  public final CacheTag tag;
  public final InflightTracker inflight;

  public CacheContext(LowLevelCache dataCache,
                      MetadataCache metadataCache,
                      CacheAdmissionPolicy admission,
                      CacheTag tag,
                      InflightTracker inflight) {
    this.dataCache = dataCache;
    this.metadataCache = metadataCache;
    this.admission = (admission == null) ? CacheAdmissionPolicy.ALWAYS : admission;
    this.tag = tag;
    this.inflight = inflight;
  }
}
