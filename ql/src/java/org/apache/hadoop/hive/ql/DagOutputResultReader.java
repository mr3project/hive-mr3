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

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Presents MR3 DAG output payloads as the same DataInput streams used by the
 * legacy file-backed result reader.
 */
public class DagOutputResultReader {
  private final List<ByteString> payloads;
  private final List<Object> rows;
  private int nextPayloadIndex;
  private int nextRowIndex;

  public DagOutputResultReader(List<ByteString> payloads) {
    this.payloads = Collections.unmodifiableList(new ArrayList<>(payloads));
    this.rows = Collections.emptyList();
    this.nextPayloadIndex = 0;
    this.nextRowIndex = 0;
  }

  public DagOutputResultReader(List<ByteString> payloads, List<Object> rows) {
    this.payloads = Collections.unmodifiableList(new ArrayList<>(payloads));
    this.rows = Collections.unmodifiableList(new ArrayList<>(rows));
    this.nextPayloadIndex = 0;
    this.nextRowIndex = 0;
  }

  public synchronized DataInput nextStream() throws IOException {
    if (nextPayloadIndex >= payloads.size()) {
      return null;
    }
    return new DataInputStream(unframePayload(payloads.get(nextPayloadIndex++).toByteArray()).newInput());
  }

  public synchronized void reset() {
    nextPayloadIndex = 0;
    nextRowIndex = 0;
  }

  public synchronized boolean hasPayloads() {
    return !payloads.isEmpty();
  }

  public synchronized boolean hasRows() {
    return !rows.isEmpty();
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public synchronized int nextRows(List results, int maxRows) {
    int count = 0;
    while (count < maxRows && nextRowIndex < rows.size()) {
      results.add(rows.get(nextRowIndex++));
      count++;
    }
    return count;
  }

  private ByteString unframePayload(byte[] payload) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream(payload.length);
    int offset = 0;
    while (offset < payload.length) {
      if (payload.length - offset < Integer.BYTES) {
        throw new IOException("Truncated DAG output row length");
      }
      int recordLength = readInt(payload, offset);
      offset += Integer.BYTES;
      if (recordLength < 0 || recordLength > payload.length - offset) {
        throw new IOException("Invalid DAG output row length: " + recordLength);
      }
      output.write(payload, offset, recordLength);
      offset += recordLength;
      if (offset >= payload.length) {
        throw new IOException("Missing DAG output row separator");
      }
      output.write(payload[offset++]);
    }
    return ByteString.copyFrom(output.toByteArray());
  }

  private int readInt(byte[] bytes, int offset) {
    return ((bytes[offset] & 0xff) << 24)
        | ((bytes[offset + 1] & 0xff) << 16)
        | ((bytes[offset + 2] & 0xff) << 8)
        | (bytes[offset + 3] & 0xff);
  }
}
