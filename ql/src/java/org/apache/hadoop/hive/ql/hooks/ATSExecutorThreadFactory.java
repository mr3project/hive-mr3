package org.apache.hadoop.hive.ql.hooks;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ATSExecutorThreadFactory implements ThreadFactory {
    private final AtomicInteger threadNumber;
    private final String name = "ATS Logger ";
    private final ThreadGroup group;

    public ATSExecutorThreadFactory() {
        threadNumber = new AtomicInteger(0);

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
        thread.setDaemon(true);
        // do not use the current Thread's ClassLoader (which is UDFClassLoader)
        thread.setContextClassLoader(ClassLoader.getSystemClassLoader());
        return thread;
    }
}
