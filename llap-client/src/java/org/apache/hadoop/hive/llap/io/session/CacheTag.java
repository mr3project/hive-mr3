package org.apache.hadoop.hive.llap.io.session;

/** Optional identifier for diagnostics. */
public final class CacheTag {
  public final long dagId;
  public final long tableId;
  public CacheTag(long dagId, long tableId) {
    this.dagId = dagId;
    this.tableId = tableId;
  }
  @Override
  public String toString() { return "dag=" + dagId + ",table=" + tableId; }
}
