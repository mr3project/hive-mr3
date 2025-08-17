package org.apache.hadoop.hive.llap.io.session;

public interface LlapIoSession extends AutoCloseable {
  /** The routing handle to be passed into read pipelines. */
  CacheContext context();
  /** Clears the data+metadata caches and releases all associated memory. */
  @Override void close();
}
