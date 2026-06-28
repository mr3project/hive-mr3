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

package org.apache.hadoop.hive.ql;

import com.google.protobuf.ByteString;

import java.io.DataInput;
import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Presents MR3 DAG output payloads as the same DataInput streams used by the
 * legacy file-backed result reader.
 */
public class DagOutputResultReader {
  private static final Logger LOG = LoggerFactory.getLogger(DagOutputResultReader.class);
  private final List<ByteString> payloads;
  private int nextPayloadIndex;

  public DagOutputResultReader(List<ByteString> payloads) {
    this.payloads = Collections.unmodifiableList(new ArrayList<>(payloads));
    this.nextPayloadIndex = 0;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created DagOutputResultReader with {} payload(s), payloadSizes={}",
          this.payloads.size(), getPayloadSizes(this.payloads));
    }
  }

  public synchronized DataInput nextStream() {
    LOG.debug("DagOutputResultReader.nextStream called: nextPayloadIndex={}, payloadCount={}",
        nextPayloadIndex, payloads.size());
    if (nextPayloadIndex >= payloads.size()) {
      LOG.debug("DagOutputResultReader.nextStream returning null: all payloads consumed");
      return null;
    }
    ByteString payload = payloads.get(nextPayloadIndex);
    LOG.debug("DagOutputResultReader.nextStream returning payload index {} with {} byte(s)",
        nextPayloadIndex, payload.size());
    nextPayloadIndex++;
    return new DataInputStream(payload.newInput());
  }

  public synchronized void reset() {
    LOG.debug("DagOutputResultReader.reset called: nextPayloadIndex={}, payloadCount={}",
        nextPayloadIndex, payloads.size());
    nextPayloadIndex = 0;
  }

  public synchronized boolean hasPayloads() {
    boolean hasPayloads = !payloads.isEmpty();
    LOG.debug("DagOutputResultReader.hasPayloads called: hasPayloads={}, payloadCount={}",
        hasPayloads, payloads.size());
    return hasPayloads;
  }

  public synchronized int getPayloadCount() {
    return payloads.size();
  }

  private static List<Integer> getPayloadSizes(List<ByteString> payloads) {
    List<Integer> payloadSizes = new ArrayList<>(payloads.size());
    for (ByteString payload : payloads) {
      payloadSizes.add(payload.size());
    }
    return payloadSizes;
  }
}
