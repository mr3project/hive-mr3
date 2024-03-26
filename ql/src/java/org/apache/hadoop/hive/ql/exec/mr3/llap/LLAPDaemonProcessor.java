/**
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

package org.apache.hadoop.hive.ql.exec.mr3.llap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.tez.common.TezUtils;
import org.apache.tez.runtime.api.AbstractLogicalIOProcessor;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.events.TaskAttemptStopRequestEvent;
import org.apache.tez.runtime.api.events.TaskAttemptDAGJoiningEvent;
import org.apache.tez.runtime.api.events.TaskAttemptDAGLeavingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class LLAPDaemonProcessor extends AbstractLogicalIOProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(LLAPDaemonProcessor.class.getName());

  public LLAPDaemonProcessor(ProcessorContext context) {
    super(context);
  }

  @Override
  public void initialize() throws IOException {
    Configuration conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
    LlapProxy.initializeLlapIo(conf);
  }

  private final Object waitLock = new Object();

  @Override
  public boolean run(Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs)
      throws Exception {
    LOG.info("LLAP daemon running");
    synchronized (waitLock) {
      waitLock.wait();
    }
    return false;
  }

  @Override
  public void handleEvents(List<Event> events) {
    for (Event event: events) {
      if (event instanceof TaskAttemptStopRequestEvent) {
        LOG.info("TaskAttemptStopRequestEvent received - shutting down LLAP daemon");
        synchronized (waitLock) {
          waitLock.notifyAll();
        }
      } else if (event instanceof TaskAttemptDAGJoiningEvent) {
        TaskAttemptDAGJoiningEvent ev = (TaskAttemptDAGJoiningEvent)event;
      } else if (event instanceof TaskAttemptDAGLeavingEvent) {
        TaskAttemptDAGLeavingEvent ev = (TaskAttemptDAGLeavingEvent)event;
      }
    }
  }

  @Override
  public void close() throws IOException {
  }
}