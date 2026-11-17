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
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperBatch;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperSingleLongSingleString;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Array-backed, separate-chaining aggregation hash table for one long and one string grouping keys.
 */
final class VectorGroupByLongStringHashTable extends VectorGroupByHashTable {
  private long[][] keys0;
  private byte[][][] stringKeys;
  private byte[][] nullMasks;
  // Cached spread hashes used for lookup and bucket selection.
  private int[][] hashCodes;
  private final int longKeyIndex0;
  private final int stringKeyIndex;
  private VectorAggregationBufferRow nullValue;
  private long keyMemorySize;

  VectorGroupByLongStringHashTable(int expectedEntries, VectorHashKeyWrapperBatch keyLayout) {
    super(expectedEntries);
    longKeyIndex0 = keyLayout.longIndices[0];
    stringKeyIndex = keyLayout.stringIndices[0];
    resetKeyStorage();
    assert checkInvariants();
  }

  @Override
  int prepareBatch(VectorGroupByOperator.ProcessingModeHashAggregate context,
      VectorHashKeyWrapperBase[] keyWrappers, int n, VectorAggregationBufferBatch batchInfo)
      throws HiveException {
    int newEntries = 0;
    for (int i = 0; i < n; ++i) {
      VectorHashKeyWrapperSingleLongSingleString kw =
          (VectorHashKeyWrapperSingleLongSingleString) keyWrappers[i];
      int nullMask = (kw.isNull(0) ? 1 : 0) | (kw.isNull(1) ? 2 : 0);
      boolean isNull = nullMask == 3;
      int hashCode = isNull ? 0 : spreadHash(kw.hashCode());
      int slot = isNull ? 0 : findSlot(kw.getLongValue(0), nullMask,
          kw.getBytes(0), kw.getByteStart(0), kw.getByteLength(0), hashCode);
      VectorAggregationBufferRow aggregationBuffer = isNull ? nullValue :
          (slot >= 0 ? getValue(slot) : null);
      if (aggregationBuffer == null) {
        aggregationBuffer = context.allocateAggregationBuffer();
        if (isNull) {
          assert nullValue == null;
          nullValue = aggregationBuffer;
        } else {
          put(kw.getLongValue(0), nullMask, kw.getBytes(0), kw.getByteStart(0),
              kw.getByteLength(0), hashCode, ~slot, aggregationBuffer);
        }
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
    assert size() == context.getHashTableSize();
    int initialEntries = context.getHashTableSize();
    VectorHashKeyWrapperSingleLongSingleString keyWrapper =
        (VectorHashKeyWrapperSingleLongSingleString) outputKey;
    int entriesFlushed = 0;
    int rowsEmitted = 0;
    // Visit the separate all-NULL group before the bucket chains.
    flushEntries:
    for (int bucket = -1; bucket < bucketCount(); ++bucket) {
      if (bucket == -1 && nullValue == null) {
        continue;
      }
      int previousSlot = -1;
      int slot = bucket == -1 ? 0 : firstSlot(bucket);
      while (slot != -1) {
        int segment = entrySegment(slot);
        int offset = entryOffset(slot, segment);
        int nextSlot = bucket == -1 ? -1 : nextSlot(slot);
        VectorAggregationBufferRow bufferRow = bucket == -1 ? nullValue : getValue(slot);
        assert bufferRow != null;
        if (!all && avgAccess >= 1 && bufferRow.getAccessCount() > avgAccess) {
          context.resetHashTableAccess(bufferRow);
          previousSlot = slot;
          slot = nextSlot;
          continue;
        }

        keyWrapper.clearIsNull();
        int nullMask = bucket == -1 ? 3 : nullMasks[segment][offset];
        if ((nullMask & (1 << longKeyIndex0)) != 0) {
          keyWrapper.assignNullLong(longKeyIndex0, 0);
        } else {
          keyWrapper.assignLong(longKeyIndex0, 0, keys0[segment][offset]);
        }
        if ((nullMask & (1 << stringKeyIndex)) != 0) {
          keyWrapper.assignNullString(stringKeyIndex, 0);
        } else {
          byte[] bytes = stringKeys[segment][offset];
          keyWrapper.assignString(0, bytes, 0, bytes.length);
        }
        // writeSingleRow copies the key into outputBatch before this wrapper is reused.
        context.writeHashTableRow(keyWrapper, bufferRow);
        ++rowsEmitted;

        if (!all) {
          context.recycleHashTableBuffer(bufferRow);
          if (bucket == -1) {
            nullValue = null;
          } else {
            remove(bucket, previousSlot, slot);
          }
          if (++entriesFlushed >= entriesToFlush) {
            break flushEntries;
          }
        } else {
          previousSlot = slot;
        }
        slot = nextSlot;
      }
    }
    assert all || (checkInvariants() && rowsEmitted == entriesFlushed &&
        initialEntries - entriesFlushed == context.getHashTableSize() &&
        size() == context.getHashTableSize());
    assert !all || rowsEmitted == initialEntries;
    keyWrapper.assignNullString(stringKeyIndex, 0);
    return entriesFlushed;
  }

  /**
   * Returns the matching slot, or the bitwise complement of an insertion slot.
   * The supplied hash code has already been spread for bucket selection.
   */
  int findSlot(long key0, int nullMask, byte[] bytes, int start, int length,
      int hashCode) {
    assert checkKeyStorage();
    assert Integer.bitCount(bucketCapacity) == 1;
    assert numEntries >= 0 && numEntries <= numUsed && numUsed <= entryCapacity &&
        numEntries <= resizeThreshold && resizeThreshold < bucketCapacity;
    assert (freeList == 0) == (numEntries == numUsed);
    assert nullMask >= 0 && nullMask < 3;
    assert ((nullMask & (1 << stringKeyIndex)) != 0) == (length == -1);
    assert hashCode == spreadHash(VectorHashKeyWrapperSingleLongSingleString.calculateHashCode(
        key0, nullMask, bytes, start, length));
    int bucket = hashCode & (bucketCapacity - 1);
    for (int ref = bucketHead(bucket); ref != 0;) {
      int slot = ref - 1;
      int segment = entrySegment(slot);
      int offset = entryOffset(slot, segment);
      ref = next[segment][offset];
      if (hashCodes[segment][offset] == hashCode && keys0[segment][offset] == key0 &&
          nullMasks[segment][offset] == nullMask &&
          (length == -1 || StringExpr.equal(bytes, start, length,
              stringKeys[segment][offset], 0, stringKeys[segment][offset].length))) {
        return slot;
      }
    }
    return ~(freeList == 0 ? numUsed : freeList - 1);
  }

  void put(long key0, int nullMask, byte[] bytes, int start, int length,
      int hashCode, int insertionSlot, VectorAggregationBufferRow value) {
    assert value != null;
    assert insertionSlot == ~findSlot(key0, nullMask, bytes, start, length, hashCode);
    allocateSlot(insertionSlot);
    int segment = entrySegment(insertionSlot);
    int offset = entryOffset(insertionSlot, segment);
    hashCodes[segment][offset] = hashCode;
    assert stringKeys[segment][offset] == null;
    keys0[segment][offset] = key0;
    nullMasks[segment][offset] = (byte) nullMask;
    if (length != -1) {
      stringKeys[segment][offset] = Arrays.copyOfRange(bytes, start, start + length);
      keyMemorySize += JAVA_DATA_MODEL.lengthForByteArrayOfSize(length);
    }
    linkEntryWithSpreadHash(insertionSlot, hashCode, value);
    assert findSlot(key0, nullMask, bytes, start, length, hashCode) == insertionSlot;
    assert getValue(insertionSlot) == value;
  }

  private int hashCode(int slot) {
    int segment = entrySegment(slot);
    int offset = entryOffset(slot, segment);
    byte[] bytes = stringKeys[segment][offset];
    return VectorHashKeyWrapperSingleLongSingleString.calculateHashCode(
        keys0[segment][offset], nullMasks[segment][offset], bytes, 0, bytes == null ? -1 : bytes.length);
  }

  void remove(int bucket, int previousSlot, int slot) {
    int segment = entrySegment(slot);
    int offset = entryOffset(slot, segment);
    assert values[segment][offset] != null;
    assert bucket == (hashCodes[segment][offset] & (bucketCapacity - 1));
    unlinkEntry(bucket, previousSlot, slot);
    byte[] bytes = stringKeys[segment][offset];
    assert findSlot(keys0[segment][offset], nullMasks[segment][offset], bytes, 0,
        bytes == null ? -1 : bytes.length, hashCodes[segment][offset]) == ~slot;
    if (bytes != null) {
      keyMemorySize -= JAVA_DATA_MODEL.lengthForByteArrayOfSize(bytes.length);
      stringKeys[segment][offset] = null;
    }
  }

  @Override
  int size() {
    return numEntries + (nullValue == null ? 0 : 1);
  }

  @Override
  void clear() {
    int remaining = numUsed;
    for (int segment = 0; remaining > 0; ++segment) {
      int count = Math.min(remaining, values[segment].length);
      Arrays.fill(stringKeys[segment], 0, count, null);
      remaining -= count;
    }
    clearEntries();
    nullValue = null;
    keyMemorySize = 0;
    assert checkInvariants();
  }

  @Override
  protected void growKeyStorage(int segment, int capacity) {
    keys0 = Arrays.copyOf(keys0, segment + 1);
    keys0[segment] = new long[capacity];
    stringKeys = Arrays.copyOf(stringKeys, segment + 1);
    stringKeys[segment] = new byte[capacity][];
    nullMasks = Arrays.copyOf(nullMasks, segment + 1);
    nullMasks[segment] = new byte[capacity];
    hashCodes = Arrays.copyOf(hashCodes, segment + 1);
    hashCodes[segment] = new int[capacity];
  }

  @Override
  protected void resetKeyStorage() {
    keys0 = new long[values.length][];
    stringKeys = new byte[values.length][][];
    nullMasks = new byte[values.length][];
    hashCodes = new int[values.length][];
    for (int segment = 0; segment < values.length; ++segment) {
      int capacity = values[segment].length;
      keys0[segment] = new long[capacity];
      stringKeys[segment] = new byte[capacity][];
      nullMasks[segment] = new byte[capacity];
      hashCodes[segment] = new int[capacity];
    }
    nullValue = null;
    keyMemorySize = 0;
  }

  @Override
  protected void rehash() {
    int mask = bucketCapacity - 1;
    int copiedEntries = 0;
    for (int slot = 0; slot < numUsed; ++slot) {
      int segment = entrySegment(slot);
      int offset = entryOffset(slot, segment);
      if (values[segment][offset] != null) {
        int bucket = hashCodes[segment][offset] & mask;
        next[segment][offset] = bucketHead(bucket);
        setBucketHead(bucket, slot + 1);
        ++copiedEntries;
      }
    }
    assert copiedEntries == numEntries;
    assert checkInvariants();
  }

  private boolean checkKeyStorage() {
    if (keys0.length != values.length ||
        stringKeys.length != values.length ||
        nullMasks.length != values.length ||
        hashCodes.length != values.length || next.length != values.length) {
      return false;
    }
    for (int segment = 0; segment < values.length; ++segment) {
      int capacity = values[segment].length;
      if (keys0[segment].length != capacity ||
          stringKeys[segment].length != capacity ||
          nullMasks[segment].length != capacity ||
          hashCodes[segment].length != capacity || next[segment].length != capacity) {
        return false;
      }
    }
    return true;
  }

  @Override
  boolean checkInvariants() {
    if (!checkStructure() || !checkKeyStorage()) {
      return false;
    }
    long memory = 0;
    for (int slot = 0; slot < entryCapacity; ++slot) {
      int segment = entrySegment(slot);
      int offset = entryOffset(slot, segment);
      byte[] bytes = stringKeys[segment][offset];
      if (values[segment][offset] == null) {
        if (bytes != null) {
          return false;
        }
      } else {
        if (nullMasks[segment][offset] < 0 || nullMasks[segment][offset] >= 3 ||
            (((nullMasks[segment][offset] & (1 << stringKeyIndex)) != 0) != (bytes == null))) {
          return false;
        }
        if ((nullMasks[segment][offset] & (1 << longKeyIndex0)) != 0 && keys0[segment][offset] != 0) {
          return false;
        }
        if (hashCodes[segment][offset] != spreadHash(hashCode(slot)) ||
            findSlot(keys0[segment][offset], nullMasks[segment][offset], bytes, 0,
                bytes == null ? -1 : bytes.length, hashCodes[segment][offset]) != slot) {
          return false;
        }
        if (bytes != null) {
          memory += JAVA_DATA_MODEL.lengthForByteArrayOfSize(bytes.length);
        }
      }
    }
    return keyMemorySize == memory;
  }

  @Override
  long getEstimatedFixedMemorySize() {
    long memory = getEstimatedArrayMemorySize()
        + JAVA_DATA_MODEL.lengthForObjectArrayOfSize(keys0.length)
        + JAVA_DATA_MODEL.lengthForObjectArrayOfSize(stringKeys.length)
        + JAVA_DATA_MODEL.lengthForObjectArrayOfSize(nullMasks.length)
        + JAVA_DATA_MODEL.lengthForObjectArrayOfSize(hashCodes.length);
    for (int segment = 0; segment < values.length; ++segment) {
      memory += JAVA_DATA_MODEL.lengthForLongArrayOfSize(keys0[segment].length)
          + JAVA_DATA_MODEL.lengthForObjectArrayOfSize(stringKeys[segment].length)
          + JAVA_DATA_MODEL.lengthForByteArrayOfSize(nullMasks[segment].length)
          + JAVA_DATA_MODEL.lengthForIntArrayOfSize(hashCodes[segment].length);
    }
    return memory;
  }

  @Override
  long getEstimatedMemorySize() {
    return getEstimatedFixedMemorySize() + keyMemorySize;
  }
}
