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
package org.apache.hadoop.hive.common.io;

import org.apache.hive.common.util.SuppressFBWarnings;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import static org.apache.tez.util.FastByteComparisons.BYTE_ARRAY_BASE_OFFSET;
import static org.apache.tez.util.FastByteComparisons.theUnsafe;

/**
 * A thread-not-safe version of ByteArrayOutputStream, which removes all
 * synchronized modifiers.
 */
public class NonSyncByteArrayOutputStream extends ByteArrayOutputStream {

  /**
   * The maximum size of array to allocate.
   * Some VMs reserve some header words in an array.
   * Attempts to allocate larger arrays may result in
   * OutOfMemoryError: Requested array size exceeds VM limit
   */
  private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

  public NonSyncByteArrayOutputStream(int size) {
    super(size);
  }

  public NonSyncByteArrayOutputStream() {
    super();
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "Ref external obj for efficiency")
  public byte[] getData() {
    return buf;
  }

  public int getLength() {
    return count;
  }

  public void setWritePosition(int writePosition) {
    count = writePosition;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reset() {
    count = 0;
  }

  public void write(DataInput in, int length) throws IOException {
    enLargeBuffer(length);
    in.readFully(buf, count, length);
    count += length;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(int b) {
    enLargeBuffer(1);
    buf[count] = (byte) b;
    count += 1;
  }

  public void writeInt(long offset, int value) {
    value = Integer.reverseBytes(value);  // required for correctness (sort order in BinarySortableSerDe)
    theUnsafe.putInt(buf, BYTE_ARRAY_BASE_OFFSET + offset, value);
  }

  public void serializeBytes(byte[] data, int offset, int length, boolean invert) {
    enLargeBuffer(length * 2 + 1);

    final int end = offset + length;
    int position = count;
    if (invert) {
      for (int i = offset; i < end; i++) {
        byte value = data[i];
        if (value == 0 || value == 1) {
          buf[position++] = (byte) 0xfe;
          buf[position++] = (byte) (0xff ^ (value + 1));
        } else {
          buf[position++] = (byte) (0xff ^ value);
        }
      }
      buf[position++] = (byte) 0xff;
    } else {
      for (int i = offset; i < end; i++) {
        byte value = data[i];
        if (value == 0 || value == 1) {
          buf[position++] = (byte) 1;
          buf[position++] = (byte) (value + 1);
        } else {
          buf[position++] = value;
        }
      }
      buf[position++] = (byte) 0;
    }
    count = position;
  }

  private void enLargeBuffer(final int increment) {
    final int requestCapacity = Math.addExact(count, increment);
    final int currentCapacity = buf.length;

    if (requestCapacity > currentCapacity) {
      // Increase size by a factor of 1.5x
      int newCapacity = currentCapacity + (currentCapacity >> 1);

      // Check for overflow scenarios
      if (newCapacity < 0 || newCapacity > MAX_ARRAY_SIZE) {
        newCapacity = MAX_ARRAY_SIZE;
      } else if (newCapacity < requestCapacity) {
        newCapacity = requestCapacity;
      }
      buf = Arrays.copyOf(buf, newCapacity);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(byte b[]) {
    write(b, 0, b.length);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(byte b[], int off, int len) {
    // skip sanity check for off and len
    if (len == 0) {
      return;
    }
    enLargeBuffer(len);
    System.arraycopy(b, off, buf, count, len);
    count += len;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeTo(OutputStream out) throws IOException {
    out.write(buf, 0, count);
  }

  public void appendInt(int value) {
    enLargeBuffer(Integer.BYTES);
    value = Integer.reverseBytes(value);  // required for correctness (sort order in BinarySortableSerDe)
    theUnsafe.putInt(buf, BYTE_ARRAY_BASE_OFFSET + count, value);
    count += Integer.BYTES;
  }

  public void appendLong(long value) {
    enLargeBuffer(Long.BYTES);
    value = Long.reverseBytes(value);  // required for correctness (sort order in BinarySortableSerDe)
    theUnsafe.putLong(buf, BYTE_ARRAY_BASE_OFFSET + count, value);
    count += Long.BYTES;
  }
}
