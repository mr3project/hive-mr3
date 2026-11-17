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
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperSingleLongTwoString;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Array-backed, separate-chaining aggregation hash table for one long and two string keys.
 */
final class VectorGroupByLongTwoStringHashTable extends VectorGroupByHashTable {
  private static final int ALL_NULL_MASK = 7;

  private final int longKeyIndex;
  private final int stringKeyIndex0;
  private final int stringKeyIndex1;
  private long[][] keys;
  // Cached spread hashes used for lookup and bucket selection.
  private int[][] hashCodes;
  private byte[][] nullMasks;
  // Each slot stores its two string references at consecutive offsets.
  private byte[][][] stringKeys;
  private long keyMemorySize;
  private VectorAggregationBufferRow nullValue;

  VectorGroupByLongTwoStringHashTable(int expectedEntries, VectorHashKeyWrapperBatch keyLayout) {
    super(expectedEntries);
    longKeyIndex = keyLayout.longIndices[0];
    stringKeyIndex0 = keyLayout.stringIndices[0];
    stringKeyIndex1 = keyLayout.stringIndices[1];
    resetKeyStorage();
    assert checkInvariants();
  }

  @Override
  int prepareBatch(VectorGroupByOperator.ProcessingModeHashAggregate context,
      VectorHashKeyWrapperBase[] keyWrappers, int n, VectorAggregationBufferBatch batchInfo)
      throws HiveException {
    int newEntries = 0;
    for (int i = 0; i < n; ++i) {
      VectorHashKeyWrapperSingleLongTwoString kw =
          (VectorHashKeyWrapperSingleLongTwoString) keyWrappers[i];
      int nullMask = (kw.isNull(0) ? 1 : 0) | (kw.isNull(1) ? 2 : 0) |
          (kw.isNull(2) ? 4 : 0);
      boolean isNull = nullMask == ALL_NULL_MASK;
      int hashCode = isNull ? 0 : spreadHash(kw.hashCode());
      int slot = isNull ? 0 : findSlot(kw.getLongValue(0), nullMask,
          kw.getBytes(0), kw.getByteStart(0), kw.getByteLength(0),
          kw.getBytes(1), kw.getByteStart(1), kw.getByteLength(1), hashCode);
      VectorAggregationBufferRow aggregationBuffer = isNull ? nullValue :
          (slot >= 0 ? getValue(slot) : null);
      if (aggregationBuffer == null) {
        aggregationBuffer = context.allocateAggregationBuffer();
        if (isNull) {
          assert nullValue == null;
          nullValue = aggregationBuffer;
        } else {
          put(kw.getLongValue(0), nullMask,
              kw.getBytes(0), kw.getByteStart(0), kw.getByteLength(0),
              kw.getBytes(1), kw.getByteStart(1), kw.getByteLength(1),
              hashCode, ~slot, aggregationBuffer);
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
    VectorHashKeyWrapperSingleLongTwoString keyWrapper =
        (VectorHashKeyWrapperSingleLongTwoString) outputKey;
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
        int nullMask = bucket == -1 ? ALL_NULL_MASK : nullMasks[segment][offset];
        int stringOffset = 2 * offset;
        if ((nullMask & (1 << longKeyIndex)) != 0) {
          keyWrapper.assignNullLong(longKeyIndex, 0);
        } else {
          keyWrapper.assignLong(longKeyIndex, 0, keys[segment][offset]);
        }
        if ((nullMask & (1 << stringKeyIndex0)) != 0) {
          keyWrapper.assignNullString(stringKeyIndex0, 0);
        } else {
          byte[] bytes = stringKeys[segment][stringOffset];
          keyWrapper.assignString(0, bytes, 0, bytes.length);
        }
        if ((nullMask & (1 << stringKeyIndex1)) != 0) {
          keyWrapper.assignNullString(stringKeyIndex1, 1);
        } else {
          byte[] bytes = stringKeys[segment][stringOffset + 1];
          keyWrapper.assignString(1, bytes, 0, bytes.length);
        }
        // writeSingleRow copies the key bytes before owned storage is released or reused.
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
    keyWrapper.assignNullString(stringKeyIndex0, 0);
    keyWrapper.assignNullString(stringKeyIndex1, 1);
    assert all || (checkInvariants() && rowsEmitted == entriesFlushed &&
        initialEntries - entriesFlushed == context.getHashTableSize() &&
        size() == context.getHashTableSize());
    assert !all || rowsEmitted == initialEntries;
    return entriesFlushed;
  }

  /**
   * Returns the matching slot, or the bitwise complement of an insertion slot.
   * The supplied hash code has already been spread for bucket selection.
   */
  int findSlot(long key, int nullMask, byte[] bytes0, int start0, int length0,
      byte[] bytes1, int start1, int length1, int hashCode) {
    assert checkKeyStorage();
    assert Integer.bitCount(bucketCapacity) == 1;
    assert numEntries >= 0 && numEntries <= numUsed && numUsed <= entryCapacity &&
        numEntries <= resizeThreshold && resizeThreshold < bucketCapacity;
    assert (freeList == 0) == (numEntries == numUsed);
    assert nullMask >= 0 && nullMask < ALL_NULL_MASK;
    assert ((nullMask & (1 << stringKeyIndex0)) != 0) == (length0 == -1);
    assert ((nullMask & (1 << stringKeyIndex1)) != 0) == (length1 == -1);
    assert hashCode == spreadHash(VectorHashKeyWrapperSingleLongTwoString.calculateHashCode(
        key, nullMask, bytes0, start0, length0, bytes1, start1, length1, null));
    int bucket = hashCode & (bucketCapacity - 1);
    for (int ref = bucketHead(bucket); ref != 0;) {
      int slot = ref - 1;
      int segment = entrySegment(slot);
      int offset = entryOffset(slot, segment);
      ref = next[segment][offset];
      int stringOffset = 2 * offset;
      if (hashCodes[segment][offset] == hashCode && keys[segment][offset] == key &&
          nullMasks[segment][offset] == nullMask &&
          (length0 == -1 || StringExpr.equal(
              bytes0, start0, length0, stringKeys[segment][stringOffset], 0,
              stringKeys[segment][stringOffset].length)) &&
          (length1 == -1 || StringExpr.equal(
              bytes1, start1, length1, stringKeys[segment][stringOffset + 1], 0,
              stringKeys[segment][stringOffset + 1].length))) {
        return slot;
      }
    }
    return ~(freeList == 0 ? numUsed : freeList - 1);
  }

  void put(long key, int nullMask, byte[] bytes0, int start0, int length0,
      byte[] bytes1, int start1, int length1, int hashCode, int insertionSlot,
      VectorAggregationBufferRow value) {
    assert value != null;
    assert insertionSlot == ~findSlot(key, nullMask,
        bytes0, start0, length0, bytes1, start1, length1, hashCode);
    allocateSlot(insertionSlot);
    int segment = entrySegment(insertionSlot);
    int offset = entryOffset(insertionSlot, segment);
    int stringOffset = 2 * offset;
    assert stringKeys[segment][stringOffset] == null && stringKeys[segment][stringOffset + 1] == null;
    hashCodes[segment][offset] = hashCode;
    keys[segment][offset] = key;
    nullMasks[segment][offset] = (byte) nullMask;
    if (length0 != -1) {
      stringKeys[segment][stringOffset] = Arrays.copyOfRange(bytes0, start0, start0 + length0);
      keyMemorySize += JAVA_DATA_MODEL.lengthForByteArrayOfSize(length0);
    }
    if (length1 != -1) {
      stringKeys[segment][stringOffset + 1] = Arrays.copyOfRange(bytes1, start1, start1 + length1);
      keyMemorySize += JAVA_DATA_MODEL.lengthForByteArrayOfSize(length1);
    }
    linkEntryWithSpreadHash(insertionSlot, hashCode, value);
    assert findSlot(key, nullMask,
        bytes0, start0, length0, bytes1, start1, length1, hashCode) == insertionSlot;
    assert getValue(insertionSlot) == value;
  }

  void remove(int bucket, int previousSlot, int slot) {
    int segment = entrySegment(slot);
    int offset = entryOffset(slot, segment);
    assert values[segment][offset] != null;
    int stringOffset = 2 * offset;
    byte[] bytes0 = stringKeys[segment][stringOffset];
    byte[] bytes1 = stringKeys[segment][stringOffset + 1];
    int length0 = bytes0 == null ? -1 : bytes0.length;
    int length1 = bytes1 == null ? -1 : bytes1.length;
    assert bucket == (hashCodes[segment][offset] & (bucketCapacity - 1));
    unlinkEntry(bucket, previousSlot, slot);
    assert findSlot(keys[segment][offset], nullMasks[segment][offset],
        bytes0, 0, length0, bytes1, 0, length1, hashCodes[segment][offset]) == ~slot;
    if (bytes0 != null) {
      keyMemorySize -= JAVA_DATA_MODEL.lengthForByteArrayOfSize(length0);
    }
    if (bytes1 != null) {
      keyMemorySize -= JAVA_DATA_MODEL.lengthForByteArrayOfSize(length1);
    }
    stringKeys[segment][stringOffset] = null;
    stringKeys[segment][stringOffset + 1] = null;
  }

  private int calculateHashCode(int slot) {
    int segment = entrySegment(slot);
    int offset = entryOffset(slot, segment);
    int stringOffset = 2 * offset;
    byte[] bytes0 = stringKeys[segment][stringOffset];
    byte[] bytes1 = stringKeys[segment][stringOffset + 1];
    return VectorHashKeyWrapperSingleLongTwoString.calculateHashCode(
        keys[segment][offset], nullMasks[segment][offset],
        bytes0, 0, bytes0 == null ? -1 : bytes0.length,
        bytes1, 0, bytes1 == null ? -1 : bytes1.length, null);
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
      Arrays.fill(stringKeys[segment], 0, 2 * count, null);
      remaining -= count;
    }
    clearEntries();
    keyMemorySize = 0;
    nullValue = null;
    assert checkInvariants();
  }

  @Override
  protected void growKeyStorage(int segment, int capacity) {
    keys = Arrays.copyOf(keys, segment + 1);
    keys[segment] = new long[capacity];
    stringKeys = Arrays.copyOf(stringKeys, segment + 1);
    stringKeys[segment] = new byte[Math.multiplyExact(capacity, 2)][];
    nullMasks = Arrays.copyOf(nullMasks, segment + 1);
    nullMasks[segment] = new byte[capacity];
    hashCodes = Arrays.copyOf(hashCodes, segment + 1);
    hashCodes[segment] = new int[capacity];
  }

  @Override
  protected void resetKeyStorage() {
    keys = new long[values.length][];
    stringKeys = new byte[values.length][][];
    nullMasks = new byte[values.length][];
    hashCodes = new int[values.length][];
    for (int segment = 0; segment < values.length; ++segment) {
      int capacity = values[segment].length;
      keys[segment] = new long[capacity];
      stringKeys[segment] = new byte[Math.multiplyExact(capacity, 2)][];
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
    if (keys.length != values.length ||
        stringKeys.length != values.length ||
        nullMasks.length != values.length ||
        hashCodes.length != values.length || next.length != values.length) {
      return false;
    }
    for (int segment = 0; segment < values.length; ++segment) {
      int capacity = values[segment].length;
      if (keys[segment].length != capacity ||
          stringKeys[segment].length != 2L * capacity ||
          nullMasks[segment].length != capacity ||
          hashCodes[segment].length != capacity || next[segment].length != capacity) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks bucket-chain reachability, entry counts, free-list membership, and owned key bytes.
   */
  @Override
  boolean checkInvariants() {
    if (!checkStructure() || !checkKeyStorage()) {
      return false;
    }
    long ownedMemorySize = 0;
    for (int slot = 0; slot < entryCapacity; ++slot) {
      int segment = entrySegment(slot);
      int offset = entryOffset(slot, segment);
      int stringOffset = 2 * offset;
      byte[] bytes0 = stringKeys[segment][stringOffset];
      byte[] bytes1 = stringKeys[segment][stringOffset + 1];
      if (values[segment][offset] == null) {
        if (bytes0 != null || bytes1 != null) {
          return false;
        }
      } else {
        int nullMask = nullMasks[segment][offset];
        if (nullMask < 0 || nullMask >= ALL_NULL_MASK ||
            ((nullMask & (1 << stringKeyIndex0)) != 0) != (bytes0 == null) ||
            ((nullMask & (1 << stringKeyIndex1)) != 0) != (bytes1 == null) ||
            ((nullMask & (1 << longKeyIndex)) != 0 && keys[segment][offset] != 0)) {
          return false;
        }
        int length0 = bytes0 == null ? -1 : bytes0.length;
        int length1 = bytes1 == null ? -1 : bytes1.length;
        if (hashCodes[segment][offset] != spreadHash(calculateHashCode(slot)) ||
            findSlot(keys[segment][offset], nullMask, bytes0, 0, length0, bytes1, 0, length1,
                hashCodes[segment][offset]) != slot) {
          return false;
        }
        if (bytes0 != null) {
          ownedMemorySize += JAVA_DATA_MODEL.lengthForByteArrayOfSize(length0);
        }
        if (bytes1 != null) {
          ownedMemorySize += JAVA_DATA_MODEL.lengthForByteArrayOfSize(length1);
        }
      }
    }
    return ownedMemorySize == keyMemorySize;
  }

  @Override
  long getEstimatedFixedMemorySize() {
    long memory = getEstimatedArrayMemorySize()
        + JAVA_DATA_MODEL.lengthForObjectArrayOfSize(keys.length)
        + JAVA_DATA_MODEL.lengthForObjectArrayOfSize(stringKeys.length)
        + JAVA_DATA_MODEL.lengthForObjectArrayOfSize(nullMasks.length)
        + JAVA_DATA_MODEL.lengthForObjectArrayOfSize(hashCodes.length);
    for (int segment = 0; segment < values.length; ++segment) {
      memory += JAVA_DATA_MODEL.lengthForLongArrayOfSize(keys[segment].length)
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
