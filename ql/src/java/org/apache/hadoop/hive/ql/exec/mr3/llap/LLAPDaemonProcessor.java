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

import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.EvictEntityRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.MR3LlapDaemonProtocolProtos.MR3LlapDaemonProcessorEventProto;
import org.apache.hadoop.hive.llap.daemon.rpc.MR3LlapDaemonProtocolProtos.MR3LlapDaemonProcessorEventType;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.tez.common.TezUtils;
import org.apache.tez.runtime.api.AbstractLogicalIOProcessor;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.events.DaemonPayloadEvent;
import org.apache.tez.runtime.api.events.TaskAttemptStopRequestEvent;
import org.apache.tez.runtime.api.events.TaskAttemptDAGJoiningEvent;
import org.apache.tez.runtime.api.events.TaskAttemptDAGLeavingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class LLAPDaemonProcessor extends AbstractLogicalIOProcessor {

  public final static String daemonVertexName = "LLAP";

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
  public scala.Tuple2<java.lang.Integer, java.lang.Integer> run(Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs)
      throws Exception {
    LOG.info("LLAP daemon running");
    synchronized (waitLock) {
      waitLock.wait();
    }
    return null;
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
      } else if (event instanceof DaemonPayloadEvent) {
        DaemonPayloadEvent dpe = (DaemonPayloadEvent)event;
        try {
          handleDaemonPayloadEvent(dpe);
        } catch (Exception e) {
          LOG.warn("Failed to handle DaemonPayloadEvent", e);
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
  }

  private void handleDaemonPayloadEvent(DaemonPayloadEvent dpe) throws Exception {
    ByteString payload = dpe.payload;
    MR3LlapDaemonProcessorEventProto eventProto = MR3LlapDaemonProcessorEventProto.parseFrom(payload);

    if (eventProto.getType() == MR3LlapDaemonProcessorEventType.PURGE) {
      assert eventProto.getEvictedEntitiesList().isEmpty();
      LlapProxy.getIo().purge();
    } else if (eventProto.getType() == MR3LlapDaemonProcessorEventType.PROACTIVE_EVICTION) {
      assert !eventProto.getEvictedEntitiesList().isEmpty();
      for (EvictEntityRequestProto evictEntity: eventProto.getEvictedEntitiesList()) {
        LlapProxy.getIo().evictEntity(evictEntity);
      }
    }
  }
}
