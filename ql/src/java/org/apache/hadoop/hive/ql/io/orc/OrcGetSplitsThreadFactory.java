/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.io.orc;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class OrcGetSplitsThreadFactory implements ThreadFactory {
  private final AtomicInteger threadNumber;
  private final String name = "ORC_GET_SPLITS #";
  private final ThreadGroup group;

  public OrcGetSplitsThreadFactory() {
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
    thread.setDaemon(true);
    // do not use the current Thread's ClassLoader (which is DAGClassLoader from MR3)
    thread.setContextClassLoader(ClassLoader.getSystemClassLoader());
    return thread;
  }
}
