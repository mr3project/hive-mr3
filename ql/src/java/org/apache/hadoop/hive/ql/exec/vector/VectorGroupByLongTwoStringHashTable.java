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
  private long[] keys;
  // Cached spread hashes used for lookup and bucket selection.
  private int[] hashCodes;
  private byte[] nullMasks;
  // Each slot stores its two string references at consecutive offsets.
  private byte[][] stringKeys;
  private long keyMemorySize;

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
          (slot >= 0 ? values[slot] : null);
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
    for (int bucket = -1; bucket < buckets.length; ++bucket) {
      if (bucket == -1 && nullValue == null) {
        continue;
      }
      int previousSlot = -1;
      int slot = bucket == -1 ? 0 : firstSlot(bucket);
      while (slot != -1) {
        int nextSlot = bucket == -1 ? -1 : nextSlot(slot);
        VectorAggregationBufferRow bufferRow = bucket == -1 ? nullValue : values[slot];
        assert bufferRow != null;
        if (!all && avgAccess >= 1 && bufferRow.getAccessCount() > avgAccess) {
          context.resetHashTableAccess(bufferRow);
          previousSlot = slot;
          slot = nextSlot;
          continue;
        }

        keyWrapper.clearIsNull();
        int nullMask = bucket == -1 ? ALL_NULL_MASK : nullMasks[slot];
        int stringOffset = 2 * slot;
        if ((nullMask & (1 << longKeyIndex)) != 0) {
          keyWrapper.assignNullLong(longKeyIndex, 0);
        } else {
          keyWrapper.assignLong(longKeyIndex, 0, keys[slot]);
        }
        if ((nullMask & (1 << stringKeyIndex0)) != 0) {
          keyWrapper.assignNullString(stringKeyIndex0, 0);
        } else {
          byte[] bytes = stringKeys[stringOffset];
          keyWrapper.assignString(0, bytes, 0, bytes.length);
        }
        if ((nullMask & (1 << stringKeyIndex1)) != 0) {
          keyWrapper.assignNullString(stringKeyIndex1, 1);
        } else {
          byte[] bytes = stringKeys[stringOffset + 1];
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
    assert keys.length == values.length && hashCodes.length == values.length &&
        nullMasks.length == values.length &&
        stringKeys.length == 2L * values.length &&
        next.length == values.length;
    assert Integer.bitCount(buckets.length) == 1;
    assert numEntries >= 0 && numEntries <= numUsed && numUsed <= values.length &&
        numEntries <= resizeThreshold && resizeThreshold < buckets.length;
    assert (freeList == 0) == (numEntries == numUsed);
    assert nullMask >= 0 && nullMask < ALL_NULL_MASK;
    assert ((nullMask & (1 << stringKeyIndex0)) != 0) == (length0 == -1);
    assert ((nullMask & (1 << stringKeyIndex1)) != 0) == (length1 == -1);
    assert hashCode == spreadHash(VectorHashKeyWrapperSingleLongTwoString.calculateHashCode(
        key, nullMask, bytes0, start0, length0, bytes1, start1, length1, null));
    int bucket = hashCode & (buckets.length - 1);
    for (int ref = buckets[bucket]; ref != 0; ref = next[ref - 1]) {
      int slot = ref - 1;
      int stringOffset = 2 * slot;
      if (hashCodes[slot] == hashCode && keys[slot] == key && nullMasks[slot] == nullMask &&
          (length0 == -1 || StringExpr.equal(
              bytes0, start0, length0, stringKeys[stringOffset], 0,
              stringKeys[stringOffset].length)) &&
          (length1 == -1 || StringExpr.equal(
              bytes1, start1, length1, stringKeys[stringOffset + 1], 0,
              stringKeys[stringOffset + 1].length))) {
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
    int stringOffset = 2 * insertionSlot;
    assert stringKeys[stringOffset] == null && stringKeys[stringOffset + 1] == null;
    hashCodes[insertionSlot] = hashCode;
    keys[insertionSlot] = key;
    nullMasks[insertionSlot] = (byte) nullMask;
    if (length0 != -1) {
      stringKeys[stringOffset] = Arrays.copyOfRange(bytes0, start0, start0 + length0);
      keyMemorySize += JAVA_DATA_MODEL.lengthForByteArrayOfSize(length0);
    }
    if (length1 != -1) {
      stringKeys[stringOffset + 1] = Arrays.copyOfRange(bytes1, start1, start1 + length1);
      keyMemorySize += JAVA_DATA_MODEL.lengthForByteArrayOfSize(length1);
    }
    linkEntryWithSpreadHash(insertionSlot, hashCode, value);
    assert findSlot(key, nullMask,
        bytes0, start0, length0, bytes1, start1, length1, hashCode) == insertionSlot;
    assert values[insertionSlot] == value;
  }

  void remove(int bucket, int previousSlot, int slot) {
    assert values[slot] != null;
    int stringOffset = 2 * slot;
    byte[] bytes0 = stringKeys[stringOffset];
    byte[] bytes1 = stringKeys[stringOffset + 1];
    int length0 = bytes0 == null ? -1 : bytes0.length;
    int length1 = bytes1 == null ? -1 : bytes1.length;
    assert bucket == (hashCodes[slot] & (buckets.length - 1));
    unlinkEntry(bucket, previousSlot, slot);
    assert findSlot(keys[slot], nullMasks[slot],
        bytes0, 0, length0, bytes1, 0, length1, hashCodes[slot]) == ~slot;
    if (bytes0 != null) {
      keyMemorySize -= JAVA_DATA_MODEL.lengthForByteArrayOfSize(length0);
    }
    if (bytes1 != null) {
      keyMemorySize -= JAVA_DATA_MODEL.lengthForByteArrayOfSize(length1);
    }
    stringKeys[stringOffset] = null;
    stringKeys[stringOffset + 1] = null;
  }

  private int calculateHashCode(int slot) {
    int stringOffset = 2 * slot;
    byte[] bytes0 = stringKeys[stringOffset];
    byte[] bytes1 = stringKeys[stringOffset + 1];
    return VectorHashKeyWrapperSingleLongTwoString.calculateHashCode(keys[slot], nullMasks[slot],
        bytes0, 0, bytes0 == null ? -1 : bytes0.length,
        bytes1, 0, bytes1 == null ? -1 : bytes1.length, null);
  }

  @Override
  void clear() {
    Arrays.fill(stringKeys, 0, 2 * numUsed, null);
    clearEntries();
    keyMemorySize = 0;
    assert checkInvariants();
  }

  @Override
  protected void growKeyStorage(int capacity) {
    keys = Arrays.copyOf(keys, capacity);
    hashCodes = Arrays.copyOf(hashCodes, capacity);
    nullMasks = Arrays.copyOf(nullMasks, capacity);
    stringKeys = Arrays.copyOf(stringKeys, Math.multiplyExact(capacity, 2));
  }

  @Override
  protected void resetKeyStorage() {
    keys = new long[values.length];
    hashCodes = new int[values.length];
    nullMasks = new byte[values.length];
    stringKeys = new byte[Math.multiplyExact(values.length, 2)][];
    keyMemorySize = 0;
  }

  @Override
  protected void rehash() {
    int mask = buckets.length - 1;
    int copiedEntries = 0;
    for (int slot = 0; slot < numUsed; ++slot) {
      if (values[slot] != null) {
        int bucket = hashCodes[slot] & mask;
        next[slot] = buckets[bucket];
        buckets[bucket] = slot + 1;
        ++copiedEntries;
      }
    }
    assert copiedEntries == numEntries;
    assert checkInvariants();
  }

  @Override
  long getBucketFixedMemorySize() {
    return getBucketBaseMemorySize()
        + JAVA_DATA_MODEL.lengthForLongArrayOfSize(keys.length)
        + JAVA_DATA_MODEL.lengthForIntArrayOfSize(hashCodes.length)
        + JAVA_DATA_MODEL.lengthForByteArrayOfSize(nullMasks.length)
        + JAVA_DATA_MODEL.lengthForObjectArrayOfSize(stringKeys.length);
  }

  @Override
  long getTotalMemorySize() {
    return getBucketFixedMemorySize() + keyMemorySize;
  }

  /**
   * Checks bucket-chain reachability, entry counts, free-list membership, and owned key bytes.
   */
  @Override
  boolean checkInvariants() {
    if (!checkStructure() || keys.length != values.length || nullMasks.length != values.length ||
        hashCodes.length != values.length || stringKeys.length != 2L * values.length) {
      return false;
    }
    long ownedMemorySize = 0;
    for (int slot = 0; slot < values.length; ++slot) {
      int stringOffset = 2 * slot;
      byte[] bytes0 = stringKeys[stringOffset];
      byte[] bytes1 = stringKeys[stringOffset + 1];
      if (values[slot] == null) {
        if (bytes0 != null || bytes1 != null) {
          return false;
        }
      } else {
        int nullMask = nullMasks[slot];
        if (nullMask < 0 || nullMask >= ALL_NULL_MASK ||
            ((nullMask & (1 << stringKeyIndex0)) != 0) != (bytes0 == null) ||
            ((nullMask & (1 << stringKeyIndex1)) != 0) != (bytes1 == null) ||
            ((nullMask & (1 << longKeyIndex)) != 0 && keys[slot] != 0)) {
          return false;
        }
        int length0 = bytes0 == null ? -1 : bytes0.length;
        int length1 = bytes1 == null ? -1 : bytes1.length;
        if (hashCodes[slot] != spreadHash(calculateHashCode(slot)) ||
            findSlot(keys[slot], nullMask, bytes0, 0, length0, bytes1, 0, length1,
                hashCodes[slot]) != slot) {
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
}
