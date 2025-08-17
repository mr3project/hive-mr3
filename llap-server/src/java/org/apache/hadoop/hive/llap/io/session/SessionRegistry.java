package org.apache.hadoop.hive.llap.io.session;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.llap.LlapProxy;

/** Worker-local registry of LlapIoSession per (dagId, tableId). */
public final class SessionRegistry {
  private final ConcurrentHashMap<Key, LlapIoSession> sessions = new ConcurrentHashMap<>();

  private static final class Key {
    final long d, t;
    Key(long d, long t) { this.d = d; this.t = t; }
    @Override public int hashCode() { return Long.hashCode(d * 31 + t); }
    @Override public boolean equals(Object o) {
      if (!(o instanceof Key)) return false;
      Key k = (Key)o; return k.d == d && k.t == t;
    }
  }

  public LlapIoSession getOrOpen(long dagId, long tableId, SessionConfig cfg) {
    return sessions.computeIfAbsent(new Key(dagId, tableId),
        k -> LlapProxy.getIo().openSession(cfg, dagId, tableId));
  }

  /** Called when all reads for (dagId, tableId) are finished. */
  public void close(long dagId, long tableId) {
    LlapIoSession s = sessions.remove(new Key(dagId, tableId));
    if (s != null) {
      try { s.close(); } catch (Exception e) { }
    }
  }
}
