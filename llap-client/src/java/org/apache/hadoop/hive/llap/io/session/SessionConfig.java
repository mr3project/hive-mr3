package org.apache.hadoop.hive.llap.io.session;

public final class SessionConfig {
  /** Max on-heap bytes for data cache. */
  public final long dataCacheBytes;
  /** Max on-heap bytes for metadata cache. */
  public final long metadataCacheBytes;
  /** Optional soft-admission threshold; e.g. 0.8 means stop admitting above 80% used. */
  public final double admissionWatermark;
  public SessionConfig(long dataCacheBytes, long metadataCacheBytes, double admissionWatermark) {
    this.dataCacheBytes = dataCacheBytes;
    this.metadataCacheBytes = metadataCacheBytes;
    this.admissionWatermark = admissionWatermark;
  }
}
