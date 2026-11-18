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

import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperBase;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperThreeLong;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Array-backed, separate-chaining aggregation hash table for three long grouping keys.
 */
final class VectorGroupByThreeLongHashTable extends VectorGroupByHashTable {
  // The three long components of each slot are stored consecutively.
  private long[] keys;
  private byte[] nullMasks;
  // Cached wrapper hashes used for lookup and bucket selection.
  private int[] hashCodes;

  VectorGroupByThreeLongHashTable(int expectedEntries) {
    super(expectedEntries);
    resetKeyStorage();
    assert checkInvariants();
  }

  @Override
  int prepareBatch(VectorGroupByOperator.ProcessingModeHashAggregate context,
      VectorHashKeyWrapperBase[] keyWrappers, int n, VectorAggregationBufferBatch batchInfo)
      throws HiveException {
    int newEntries = 0;
    for (int i = 0; i < n; ++i) {
      VectorHashKeyWrapperThreeLong kw = (VectorHashKeyWrapperThreeLong) keyWrappers[i];
      int nullMask = (kw.isNull(0) ? 1 : 0) | (kw.isNull(1) ? 2 : 0) | (kw.isNull(2) ? 4 : 0);
      boolean isNull = nullMask == 7;
      int hashCode = isNull ? 0 : kw.hashCode();
      int slot = isNull ? 0 : findSlot(kw.getLongValue(0), kw.getLongValue(1),
          kw.getLongValue(2), nullMask, hashCode);
      VectorAggregationBufferRow aggregationBuffer = isNull ? getNullValue() :
          (slot >= 0 ? values[slot] : null);
      if (aggregationBuffer == null) {
        aggregationBuffer = context.allocateAggregationBuffer();
        if (isNull) {
          setNullValue(aggregationBuffer);
        } else {
          put(kw.getLongValue(0), kw.getLongValue(1), kw.getLongValue(2), nullMask,
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
    VectorHashKeyWrapperThreeLong keyWrapper =
        (VectorHashKeyWrapperThreeLong) outputKey;
    int entriesFlushed = 0;
    int rowsEmitted = 0;
    // Visit the separate NULL group before the bucket chains.
    flushEntries:
    for (int bucket = -1; bucket < buckets.length; ++bucket) {
      if (bucket == -1 && getNullValue() == null) {
        continue;
      }
      int previousSlot = -1;
      int slot = bucket == -1 ? 0 : firstSlot(bucket);
      while (slot != -1) {
        int nextSlot = bucket == -1 ? -1 : nextSlot(slot);
        VectorAggregationBufferRow bufferRow = bucket == -1 ? getNullValue() :
            values[slot];
        assert bufferRow != null;
        if (!all && avgAccess >= 1 && bufferRow.getAccessCount() > avgAccess) {
          context.resetHashTableAccess(bufferRow);
          previousSlot = slot;
          slot = nextSlot;
          continue;
        }

        if (bucket == -1) {
          keyWrapper.assignNullLong(0, 0);
          keyWrapper.assignNullLong(1, 1);
          keyWrapper.assignNullLong(2, 2);
        } else {
          int keyOffset = 3 * slot;
          if ((nullMasks[slot] & 1) != 0) {
            keyWrapper.assignNullLong(0, 0);
          } else {
            keyWrapper.assignLong(0, 0, keys[keyOffset]);
          }
          if ((nullMasks[slot] & 2) != 0) {
            keyWrapper.assignNullLong(1, 1);
          } else {
            keyWrapper.assignLong(1, 1, keys[keyOffset + 1]);
          }
          if ((nullMasks[slot] & 4) != 0) {
            keyWrapper.assignNullLong(2, 2);
          } else {
            keyWrapper.assignLong(2, 2, keys[keyOffset + 2]);
          }
        }
        // writeSingleRow copies the key into outputBatch before this wrapper is reused.
        context.writeHashTableRow(keyWrapper, bufferRow);
        ++rowsEmitted;

        if (!all) {
          context.recycleHashTableBuffer(bufferRow);
          if (bucket == -1) {
            removeNullValue();
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
    return entriesFlushed;
  }

  /**
   * Returns the matching slot, or the bitwise complement of an insertion slot.
   */
  int findSlot(long key0, long key1, long key2, int nullMask, int hashCode) {
    assert keys.length == 3L * values.length && nullMasks.length == values.length &&
        next.length == values.length && hashCodes.length == values.length;
    assert Integer.bitCount(buckets.length) == 1;
    assert numEntries >= 0 && numEntries <= numUsed && numUsed <= values.length &&
        numEntries <= resizeThreshold && resizeThreshold < buckets.length;
    assert (freeList == 0) == (numEntries == numUsed);
    assert nullMask >= 0 && nullMask < 7;
    assert hashCode == VectorHashKeyWrapperThreeLong.calculateHashCode(key0, key1, key2, nullMask);
    int bucket = hashCode & (buckets.length - 1);
    for (int ref = buckets[bucket]; ref != 0; ref = next[ref - 1]) {
      int slot = ref - 1;
      int keyOffset = 3 * slot;
      if (hashCodes[slot] == hashCode && nullMasks[slot] == nullMask &&
          keys[keyOffset] == key0 && keys[keyOffset + 1] == key1 && keys[keyOffset + 2] == key2) {
        return slot;
      }
    }
    return ~(freeList == 0 ? numUsed : freeList - 1);
  }

  void put(long key0, long key1, long key2, int nullMask, int hashCode, int insertionSlot,
      VectorAggregationBufferRow value) {
    assert value != null;
    assert insertionSlot == ~findSlot(key0, key1, key2, nullMask, hashCode);
    allocateSlot(insertionSlot);
    int keyOffset = 3 * insertionSlot;
    keys[keyOffset] = key0;
    keys[keyOffset + 1] = key1;
    keys[keyOffset + 2] = key2;
    nullMasks[insertionSlot] = (byte) nullMask;
    hashCodes[insertionSlot] = hashCode;
    linkEntry(insertionSlot, hashCode, value);
    assert findSlot(key0, key1, key2, nullMask, hashCode) == insertionSlot;
    assert values[insertionSlot] == value;
  }

  VectorAggregationBufferRow getNullValue() {
    return nullValue;
  }

  void setNullValue(VectorAggregationBufferRow value) {
    assert nullValue == null && value != null;
    nullValue = value;
  }

  void removeNullValue() {
    assert nullValue != null;
    nullValue = null;
  }

  private int calculateHashCode(int slot) {
    int keyOffset = 3 * slot;
    return VectorHashKeyWrapperThreeLong.calculateHashCode(
        keys[keyOffset], keys[keyOffset + 1], keys[keyOffset + 2], nullMasks[slot]);
  }

  void remove(int bucket, int previousSlot, int slot) {
    assert values[slot] != null;
    assert bucket == (hashCodes[slot] & (buckets.length - 1));
    unlinkEntry(bucket, previousSlot, slot);
    assert findSlot(keys[3 * slot], keys[3 * slot + 1], keys[3 * slot + 2], nullMasks[slot],
        hashCodes[slot]) == ~slot;
  }

  @Override
  void clear() {
    clearEntries();
    assert checkInvariants();
  }

  @Override
  protected void growKeyStorage(int capacity) {
    keys = Arrays.copyOf(keys, Math.multiplyExact(capacity, 3));
    nullMasks = Arrays.copyOf(nullMasks, capacity);
    hashCodes = Arrays.copyOf(hashCodes, capacity);
  }

  @Override
  protected void resetKeyStorage() {
    keys = new long[Math.multiplyExact(values.length, 3)];
    nullMasks = new byte[values.length];
    hashCodes = new int[values.length];
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
        + JAVA_DATA_MODEL.lengthForByteArrayOfSize(nullMasks.length)
        + JAVA_DATA_MODEL.lengthForIntArrayOfSize(hashCodes.length);
  }

  @Override
  long getTotalMemorySize() {
    return getBucketFixedMemorySize();
  }

  /**
   * Checks bucket-chain reachability, entry counts, and free-list membership.
   */
  @Override
  boolean checkInvariants() {
    if (!checkStructure() || keys.length != 3L * values.length || nullMasks.length != values.length ||
        hashCodes.length != values.length) {
      return false;
    }
    for (int slot = 0; slot < numUsed; ++slot) {
      int keyOffset = 3 * slot;
      if (values[slot] != null &&
          (hashCodes[slot] != calculateHashCode(slot) ||
              findSlot(keys[keyOffset], keys[keyOffset + 1], keys[keyOffset + 2], nullMasks[slot],
                  hashCodes[slot]) != slot)) {
        return false;
      }
    }
    return true;
  }
}
