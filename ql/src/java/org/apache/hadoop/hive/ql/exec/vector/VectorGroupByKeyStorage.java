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

package org.apache.hadoop.hive.ql.exec.vector;

import java.util.Arrays;

import static org.apache.hadoop.hive.ql.exec.vector.VectorGroupByHashTable.JAVA_DATA_MODEL;
import static org.apache.hadoop.hive.ql.exec.vector.VectorGroupByHashTable.MAX_COMPONENT_CHUNK_BYTES;
import static org.apache.hadoop.hive.ql.exec.vector.VectorGroupByHashTable.keySlotsPerChunk;

/** Flat or chunked storage for the same-type components of each grouping key. */
abstract class VectorGroupByKeyStorage<A> {
  final int componentsPerSlot;
  private final int bytesPerSlot;
  private A flat;
  private A[] chunks;
  private int slotsPerChunk;

  private VectorGroupByKeyStorage(int componentsPerSlot, int bytesPerComponent) {
    assert componentsPerSlot > 0 && bytesPerComponent > 0;
    this.componentsPerSlot = componentsPerSlot;
    this.bytesPerSlot = componentsPerSlot * bytesPerComponent;
  }

  final void reset(int capacity) {
    if (fitsInFlatArray(capacity)) {
      flat = newArray(capacity * componentsPerSlot);
      chunks = null;
      slotsPerChunk = 0;
    } else {
      flat = null;
      slotsPerChunk = keySlotsPerChunk(capacity, bytesPerSlot);
      chunks = newDirectory(capacity / slotsPerChunk);
      allocateChunks(0);
    }
    assert checkShape(capacity);
  }

  final void grow(int capacity) {
    assert capacity % 2 == 0 && checkShape(capacity / 2);
    if (fitsInFlatArray(capacity)) {
      A newFlat = newArray(capacity * componentsPerSlot);
      System.arraycopy(flat, 0, newFlat, 0, arrayLength(flat));
      flat = newFlat;
    } else if (flat != null) {
      slotsPerChunk = keySlotsPerChunk(capacity, bytesPerSlot);
      assert capacity == 2L * slotsPerChunk &&
          arrayLength(flat) == (long) slotsPerChunk * componentsPerSlot;
      // The old flat array already stores the first chunk in slot order.
      chunks = newDirectory(2);
      chunks[0] = flat;
      allocateChunks(1);
      flat = null;
    } else {
      int oldChunkCount = chunks.length;
      chunks = Arrays.copyOf(chunks, capacity / slotsPerChunk);
      allocateChunks(oldChunkCount);
    }
    assert checkShape(capacity);
  }

  private void allocateChunks(int start) {
    int length = slotsPerChunk * componentsPerSlot;
    for (int chunk = start; chunk < chunks.length; ++chunk) {
      chunks[chunk] = newArray(length);
    }
  }

  private boolean fitsInFlatArray(int capacity) {
    return (long) capacity * bytesPerSlot <= MAX_COMPONENT_CHUNK_BYTES;
  }

  final int chunkIndex(int slot) {
    return chunks == null ? 0 : slot / slotsPerChunk;
  }

  final A arrayForChunk(int chunk) {
    return chunks == null ? flat : chunks[chunk];
  }

  final int offsetInChunk(int slot, int chunk) {
    return (slot - chunk * slotsPerChunk) * componentsPerSlot;
  }

  final boolean checkShape(int capacity) {
    if (fitsInFlatArray(capacity)) {
      return flat != null && arrayLength(flat) == (long) capacity * componentsPerSlot &&
          chunks == null && slotsPerChunk == 0;
    }
    return flat == null && chunks != null && slotsPerChunk > 0 &&
        (long) slotsPerChunk * bytesPerSlot <= MAX_COMPONENT_CHUNK_BYTES &&
        (long) chunks.length * slotsPerChunk == capacity;
  }

  final boolean checkInvariants(int capacity) {
    if (!checkShape(capacity)) {
      return false;
    }
    if (chunks != null) {
      if (slotsPerChunk != keySlotsPerChunk(capacity, bytesPerSlot)) {
        return false;
      }
      for (A chunk : chunks) {
        if (chunk == null || arrayLength(chunk) != (long) slotsPerChunk * componentsPerSlot) {
          return false;
        }
      }
    }
    return true;
  }

  final long estimatedMemorySize() {
    if (chunks == null) {
      return arrayMemorySize(arrayLength(flat));
    }
    return JAVA_DATA_MODEL.lengthForObjectArrayOfSize(chunks.length)
        + chunks.length * arrayMemorySize(slotsPerChunk * componentsPerSlot);
  }

  abstract A newArray(int length);

  abstract A[] newDirectory(int length);

  abstract int arrayLength(A array);

  abstract long arrayMemorySize(int length);

  static final class LongKeys extends VectorGroupByKeyStorage<long[]> {
    LongKeys(int componentsPerSlot) {
      super(componentsPerSlot, Long.BYTES);
    }

    @Override
    long[] newArray(int length) {
      return new long[length];
    }

    @Override
    long[][] newDirectory(int length) {
      return new long[length][];
    }

    @Override
    int arrayLength(long[] array) {
      return array.length;
    }

    @Override
    long arrayMemorySize(int length) {
      return JAVA_DATA_MODEL.lengthForLongArrayOfSize(length);
    }
  }

  static final class StringKeys extends VectorGroupByKeyStorage<byte[][]> {
    StringKeys(int componentsPerSlot) {
      // Use the memory model's conservative size for each string reference.
      super(componentsPerSlot, JAVA_DATA_MODEL.ref());
    }

    void clear(int usedSlots) {
      for (int chunk = 0, remaining = usedSlots; remaining > 0; ++chunk) {
        byte[][] array = arrayForChunk(chunk);
        int slots = Math.min(remaining, array.length / componentsPerSlot);
        Arrays.fill(array, 0, slots * componentsPerSlot, null);
        remaining -= slots;
      }
    }

    @Override
    byte[][] newArray(int length) {
      return new byte[length][];
    }

    @Override
    byte[][][] newDirectory(int length) {
      return new byte[length][][];
    }

    @Override
    int arrayLength(byte[][] array) {
      return array.length;
    }

    @Override
    long arrayMemorySize(int length) {
      return JAVA_DATA_MODEL.lengthForObjectArrayOfSize(length);
    }
  }
}
