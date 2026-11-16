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

import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperBase;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperGeneralLongString;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.util.JavaDataModel;

/**
 * Array-backed, separate-chaining aggregation hash table for long and string grouping keys.
 */
final class VectorGroupByGeneraLongStringHashTable extends VectorGroupByHashTable {
  // Column mappings are shared with the compiled key layout.
  private static final long OBJECT_MEMORY_SIZE = JavaDataModel.alignUp(
      JAVA_DATA_MODEL.object() + 9L * JAVA_DATA_MODEL.ref() + 6L * JAVA_DATA_MODEL.primitive1() +
      JAVA_DATA_MODEL.primitive2(), JAVA_DATA_MODEL.memoryAlign());
  private static final byte[] EMPTY_BYTES = new byte[0];

  private final int[] longIndices;
  private final int[] stringIndices;
  private final int keyCount;
  private final int nullWordCount;

  private int[] hashes;
  // Entry-major storage keeps each key's offsets stable when entry arrays grow.
  private long[] longValues;
  private byte[][] stringValues;
  private long[] nullWords;
  private long stringMemorySize;

  VectorGroupByGeneraLongStringHashTable(int expectedEntries, VectorColumnSetInfo keyInfo) {
    super(expectedEntries);
    longIndices = keyInfo.longIndices;
    stringIndices = keyInfo.stringIndices;
    keyCount = keyInfo.keyCount;
    nullWordCount = (keyCount + Long.SIZE - 1) / Long.SIZE;
    assert keyCount == longIndices.length + stringIndices.length;
    resetKeyStorage();
    assert checkInvariants();
  }

  @Override
  int prepareBatch(VectorGroupByOperator.ProcessingModeHashAggregate context,
      VectorHashKeyWrapperBase[] keyWrappers, int n, VectorAggregationBufferBatch batchInfo)
      throws HiveException {
    int newEntries = 0;
    for (int i = 0; i < n; ++i) {
      VectorHashKeyWrapperBase key = keyWrappers[i];
      int slot = findSlot(key);
      VectorAggregationBufferRow aggregationBuffer =
          slot >= 0 ? getValue(slot) : null;
      if (aggregationBuffer == null) {
        aggregationBuffer = context.allocateAggregationBuffer();
        put(key, ~slot, aggregationBuffer);
        ++newEntries;
      } else {
        aggregationBuffer.incrementAccessCount();
      }
      batchInfo.mapAggregationBufferSet(aggregationBuffer, i);
    }
    assert size() == context.getHashTableSize() + newEntries;
    return newEntries;
  }

  @Override
  int flush(VectorGroupByOperator.ProcessingModeHashAggregate context, boolean all,
      int entriesToFlush, int avgAccess, VectorHashKeyWrapperBase outputKey) throws HiveException {
    VectorHashKeyWrapperGeneralLongString keyWrapper =
        (VectorHashKeyWrapperGeneralLongString) outputKey;
    assert size() == context.getHashTableSize();
    int initialEntries = context.getHashTableSize();
    int entriesFlushed = 0;
    int rowsEmitted = 0;
    try {
      flushEntries:
      for (int bucket = 0; bucket < bucketCount(); ++bucket) {
        int previousSlot = -1;
        int slot = firstSlot(bucket);
        while (slot != -1) {
          int nextSlot = nextSlot(slot);
          VectorAggregationBufferRow bufferRow = getValue(slot);
          if (!all && avgAccess >= 1 && bufferRow.getAccessCount() > avgAccess) {
            context.resetHashTableAccess(bufferRow);
            previousSlot = slot;
            slot = nextSlot;
            continue;
          }

          copyKey(slot, keyWrapper);
          // String keys are copied into outputBatch before the retained entry is removed.
          context.writeHashTableRow(keyWrapper, bufferRow);
          ++rowsEmitted;
          if (!all) {
            context.recycleHashTableBuffer(bufferRow);
            remove(bucket, previousSlot, slot);
            if (++entriesFlushed >= entriesToFlush) {
              break flushEntries;
            }
          } else {
            previousSlot = slot;
          }
          slot = nextSlot;
        }
      }
    } finally {
      // The reusable output key must not retain bytes from flushed entries.
      for (int i = 0; i < stringIndices.length; ++i) {
        keyWrapper.assignNullString(stringIndices[i], i);
      }
    }
    assert all || (checkInvariants() && rowsEmitted == entriesFlushed &&
        initialEntries - entriesFlushed == context.getHashTableSize() &&
        size() == context.getHashTableSize());
    assert !all || rowsEmitted == initialEntries;
    return entriesFlushed;
  }

  /**
   * Returns the matching slot, or the bitwise complement of an insertion slot.
   */
  int findSlot(VectorHashKeyWrapperBase key) {
    assert numEntries >= 0 && numEntries <= numUsed && numUsed <= values.length;
    assert numEntries <= resizeThreshold && Integer.bitCount(buckets.length) == 1;
    assert (freeList == 0) == (numEntries == numUsed);
    int hash = key.hashCode();
    int bucket = (hash ^ (hash >>> 16)) & (buckets.length - 1);
    for (int ref = buckets[bucket]; ref != 0; ref = next[ref - 1]) {
      int slot = ref - 1;
      if (hashes[slot] == hash && equalKey(slot, key)) {
        return slot;
      }
    }
    return ~(freeList == 0 ? numUsed : freeList - 1);
  }

  private boolean isNull(int slot, int keyIndex) {
    return (nullWords[slot * nullWordCount + keyIndex / Long.SIZE] &
        (1L << (keyIndex % Long.SIZE))) != 0;
  }

  private boolean equalKey(int slot, VectorHashKeyWrapperBase key) {
    for (int i = 0; i < keyCount; ++i) {
      if (isNull(slot, i) != key.isNull(i)) {
        return false;
      }
    }
    for (int i = 0, offset = slot * longIndices.length; i < longIndices.length; ++i) {
      if (longValues[offset + i] != key.getLongValue(i)) {
        return false;
      }
    }
    for (int i = 0, offset = slot * stringIndices.length; i < stringIndices.length; ++i) {
      byte[] bytes = stringValues[offset + i];
      if (bytes != null && !StringExpr.equal(bytes, 0, bytes.length,
          key.getBytes(i), key.getByteStart(i), key.getByteLength(i))) {
        return false;
      }
    }
    return true;
  }

  void put(VectorHashKeyWrapperBase key, int insertionSlot, VectorAggregationBufferRow value) {
    assert value != null;
    assert insertionSlot == ~findSlot(key);
    allocateSlot(insertionSlot);
    int nullOffset = insertionSlot * nullWordCount;
    Arrays.fill(nullWords, nullOffset, nullOffset + nullWordCount, 0);
    for (int i = 0; i < keyCount; ++i) {
      if (key.isNull(i)) {
        nullWords[nullOffset + i / Long.SIZE] |= 1L << (i % Long.SIZE);
      }
    }
    for (int i = 0, offset = insertionSlot * longIndices.length; i < longIndices.length; ++i) {
      longValues[offset + i] = key.getLongValue(i);
    }
    for (int i = 0, offset = insertionSlot * stringIndices.length; i < stringIndices.length; ++i) {
      assert stringValues[offset + i] == null;
      if (!key.isNull(stringIndices[i])) {
        int length = key.getByteLength(i);
        byte[] bytes = length == 0 ? EMPTY_BYTES : Arrays.copyOfRange(
            key.getBytes(i), key.getByteStart(i), key.getByteStart(i) + length);
        stringValues[offset + i] = bytes;
        if (length != 0) {
          stringMemorySize += JAVA_DATA_MODEL.lengthForByteArrayOfSize(length);
        }
      }
    }
    int hash = key.hashCode();
    hashes[insertionSlot] = hash;
    linkEntry(insertionSlot, hash, value);
    assert findSlot(key) == insertionSlot && equalKey(insertionSlot, key);
    assert getValue(insertionSlot) == value;
  }

  void copyKey(int slot, VectorHashKeyWrapperBase target) {
    assert values[slot] != null;
    target.clearIsNull();
    for (int i = 0, offset = slot * longIndices.length; i < longIndices.length; ++i) {
      if (isNull(slot, longIndices[i])) {
        target.assignNullLong(longIndices[i], i);
      } else {
        target.assignLong(longIndices[i], i, longValues[offset + i]);
      }
    }
    for (int i = 0, offset = slot * stringIndices.length; i < stringIndices.length; ++i) {
      byte[] bytes = stringValues[offset + i];
      if (bytes == null) {
        target.assignNullString(stringIndices[i], i);
      } else {
        target.assignString(i, bytes, 0, bytes.length);
      }
    }
  }

  void remove(int bucket, int previousSlot, int slot) {
    assert values[slot] != null;
    assert bucket == ((hashes[slot] ^ (hashes[slot] >>> 16)) & (buckets.length - 1));
    for (int i = 0, offset = slot * stringIndices.length; i < stringIndices.length; ++i) {
      byte[] bytes = stringValues[offset + i];
      if (bytes != null && bytes.length != 0) {
        stringMemorySize -= JAVA_DATA_MODEL.lengthForByteArrayOfSize(bytes.length);
      }
      stringValues[offset + i] = null;
    }
    Arrays.fill(nullWords, slot * nullWordCount, (slot + 1) * nullWordCount, 0);
    unlinkEntry(bucket, previousSlot, slot);
    assert stringMemorySize >= 0;
  }

  @Override
  void clear() {
    Arrays.fill(stringValues, 0, numUsed * stringIndices.length, null);
    Arrays.fill(nullWords, 0, numUsed * nullWordCount, 0);
    clearEntries();
    stringMemorySize = 0;
    assert checkInvariants();
  }

  @Override
  protected void growKeyStorage(int capacity) {
    hashes = Arrays.copyOf(hashes, capacity);
    longValues = Arrays.copyOf(longValues, Math.multiplyExact(capacity, longIndices.length));
    stringValues = Arrays.copyOf(stringValues, Math.multiplyExact(capacity, stringIndices.length));
    nullWords = Arrays.copyOf(nullWords, Math.multiplyExact(capacity, nullWordCount));
  }

  @Override
  protected void resetKeyStorage() {
    hashes = new int[values.length];
    longValues = new long[Math.multiplyExact(values.length, longIndices.length)];
    stringValues = new byte[Math.multiplyExact(values.length, stringIndices.length)][];
    nullWords = new long[Math.multiplyExact(values.length, nullWordCount)];
    stringMemorySize = 0;
  }

  @Override
  protected void rehash() {
    resizeBuckets();
    int copiedEntries = 0;
    for (int slot = 0; slot < numUsed; ++slot) {
      if (values[slot] != null) {
        int hash = hashes[slot];
        int bucket = (hash ^ (hash >>> 16)) & (buckets.length - 1);
        next[slot] = buckets[bucket];
        buckets[bucket] = slot + 1;
        ++copiedEntries;
      }
    }
    assert copiedEntries == numEntries;
    assert checkInvariants();
  }

  private boolean equalKeys(int slot, int otherSlot) {
    for (int i = 0; i < nullWordCount; ++i) {
      if (nullWords[slot * nullWordCount + i] != nullWords[otherSlot * nullWordCount + i]) {
        return false;
      }
    }
    for (int i = 0; i < longIndices.length; ++i) {
      if (longValues[slot * longIndices.length + i] !=
          longValues[otherSlot * longIndices.length + i]) {
        return false;
      }
    }
    for (int i = 0; i < stringIndices.length; ++i) {
      if (!Arrays.equals(stringValues[slot * stringIndices.length + i],
          stringValues[otherSlot * stringIndices.length + i])) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks chain reachability, unique key membership, the free list, and owned string bytes.
   */
  @Override
  boolean checkInvariants() {
    if (!checkStructure() || hashes.length != values.length ||
        longValues.length != (long) values.length * longIndices.length ||
        stringValues.length != (long) values.length * stringIndices.length ||
        nullWords.length != (long) values.length * nullWordCount) {
      return false;
    }
    for (int bucket = 0; bucket < buckets.length; ++bucket) {
      for (int ref = buckets[bucket]; ref != 0; ref = next[ref - 1]) {
        int slot = ref - 1;
        if (((hashes[slot] ^ (hashes[slot] >>> 16)) & (buckets.length - 1)) != bucket) {
          return false;
        }
        for (int other = buckets[bucket]; other != ref; other = next[other - 1]) {
          if (hashes[slot] == hashes[other - 1] && equalKeys(slot, other - 1)) {
            return false;
          }
        }
      }
    }
    long stringBytes = 0;
    for (int slot = 0; slot < values.length; ++slot) {
      for (int i = 0; i < stringIndices.length; ++i) {
        byte[] bytes = stringValues[slot * stringIndices.length + i];
        if (values[slot] == null ? bytes != null :
            (bytes == null) != isNull(slot, stringIndices[i])) {
          return false;
        }
        if (bytes != null && bytes.length != 0) {
          stringBytes += JAVA_DATA_MODEL.lengthForByteArrayOfSize(bytes.length);
        }
      }
      if (values[slot] == null) {
        for (int i = 0; i < nullWordCount; ++i) {
          if (nullWords[slot * nullWordCount + i] != 0) {
            return false;
          }
        }
      }
    }
    return stringBytes == stringMemorySize;
  }

  @Override
  long getEstimatedFixedMemorySize() {
    return OBJECT_MEMORY_SIZE + getEstimatedArrayMemorySize()
        + JAVA_DATA_MODEL.lengthForIntArrayOfSize(hashes.length)
        + JAVA_DATA_MODEL.lengthForLongArrayOfSize(longValues.length)
        + JAVA_DATA_MODEL.lengthForObjectArrayOfSize(stringValues.length)
        + JAVA_DATA_MODEL.lengthForLongArrayOfSize(nullWords.length);
  }

  @Override
  long getEstimatedMemorySize() {
    return getEstimatedFixedMemorySize() + stringMemorySize;
  }
}
