package org.apache.hadoop.hive.ql.exec.tez;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class LlapObjectCacheThreadFactory implements ThreadFactory {
  private final AtomicInteger threadNumber;
  private final String name = "LLAP_OBJECT_CACHE #";
  private final ThreadGroup group;

  public LlapObjectCacheThreadFactory() {
    threadNumber = new AtomicInteger(1);

    SecurityManager s = System.getSecurityManager();
    if (s != null) {
      group = s.getThreadGroup();
    } else {
      group = Thread.currentThread().getThreadGroup();
    }
  }

  public Thread newThread(Runnable runnable) {
    int threadId = threadNumber.getAndIncrement();
    Thread thread = new Thread(group, runnable, name + threadId, 0);
    thread.setDaemon(false);
    // do not use the current Thread's ClassLoader (which is DAGClassLoader from MR3)
    thread.setContextClassLoader(ClassLoader.getSystemClassLoader());
    return thread;
  }
}
