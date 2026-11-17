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
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperTwoLong;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Array-backed, separate-chaining aggregation hash table for two long grouping keys.
 */
final class VectorGroupByTwoLongHashTable extends VectorGroupByHashTable {
  // The two long components of each slot are stored consecutively.
  private long[] keys;
  private byte[] nullMasks;
  // Cached spread hashes used for lookup and bucket selection.
  private int[] hashCodes;

  VectorGroupByTwoLongHashTable(int expectedEntries) {
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
      VectorHashKeyWrapperTwoLong kw = (VectorHashKeyWrapperTwoLong) keyWrappers[i];
      int nullMask = (kw.isNull(0) ? 1 : 0) | (kw.isNull(1) ? 2 : 0);
      boolean isNull = nullMask == 3;
      int hashCode = isNull ? 0 : spreadHash(kw.hashCode());
      int slot = isNull ? 0 : findSlot(kw.getLongValue(0), kw.getLongValue(1), nullMask, hashCode);
      VectorAggregationBufferRow aggregationBuffer = isNull ? getNullValue() :
          (slot >= 0 ? values[slot] : null);
      if (aggregationBuffer == null) {
        aggregationBuffer = context.allocateAggregationBuffer();
        if (isNull) {
          setNullValue(aggregationBuffer);
        } else {
          put(kw.getLongValue(0), kw.getLongValue(1), nullMask, hashCode, ~slot, aggregationBuffer);
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
    VectorHashKeyWrapperTwoLong keyWrapper =
        (VectorHashKeyWrapperTwoLong) outputKey;
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
        } else {
          int keyOffset = 2 * slot;
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
   * The supplied hash code has already been spread for bucket selection.
   */
  int findSlot(long key0, long key1, int nullMask, int hashCode) {
    assert keys.length == 2L * values.length &&
        nullMasks.length == values.length && next.length == values.length &&
        hashCodes.length == values.length;
    assert Integer.bitCount(buckets.length) == 1;
    assert numEntries >= 0 && numEntries <= numUsed && numUsed <= values.length &&
        numEntries <= resizeThreshold && resizeThreshold < buckets.length;
    assert (freeList == 0) == (numEntries == numUsed);
    assert nullMask >= 0 && nullMask < 3;
    assert hashCode == spreadHash(VectorHashKeyWrapperTwoLong.calculateHashCode(key0, key1, nullMask));
    int bucket = hashCode & (buckets.length - 1);
    for (int ref = buckets[bucket]; ref != 0; ref = next[ref - 1]) {
      int slot = ref - 1;
      int keyOffset = 2 * slot;
      if (hashCodes[slot] == hashCode && nullMasks[slot] == nullMask &&
          keys[keyOffset] == key0 && keys[keyOffset + 1] == key1) {
        return slot;
      }
    }
    return ~(freeList == 0 ? numUsed : freeList - 1);
  }

  void put(long key0, long key1, int nullMask, int hashCode, int insertionSlot,
      VectorAggregationBufferRow value) {
    assert value != null;
    assert insertionSlot == ~findSlot(key0, key1, nullMask, hashCode);
    allocateSlot(insertionSlot);
    int keyOffset = 2 * insertionSlot;
    keys[keyOffset] = key0;
    keys[keyOffset + 1] = key1;
    nullMasks[insertionSlot] = (byte) nullMask;
    hashCodes[insertionSlot] = hashCode;
    linkEntryWithSpreadHash(insertionSlot, hashCode, value);
    assert findSlot(key0, key1, nullMask, hashCode) == insertionSlot;
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
    int keyOffset = 2 * slot;
    return VectorHashKeyWrapperTwoLong.calculateHashCode(
        keys[keyOffset], keys[keyOffset + 1], nullMasks[slot]);
  }

  void remove(int bucket, int previousSlot, int slot) {
    assert values[slot] != null;
    assert bucket == (hashCodes[slot] & (buckets.length - 1));
    unlinkEntry(bucket, previousSlot, slot);
    assert findSlot(keys[2 * slot], keys[2 * slot + 1], nullMasks[slot], hashCodes[slot]) == ~slot;
  }

  @Override
  void clear() {
    clearEntries();
    assert checkInvariants();
  }

  @Override
  protected void growKeyStorage(int capacity) {
    keys = Arrays.copyOf(keys, Math.multiplyExact(capacity, 2));
    nullMasks = Arrays.copyOf(nullMasks, capacity);
    hashCodes = Arrays.copyOf(hashCodes, capacity);
  }

  @Override
  protected void resetKeyStorage() {
    keys = new long[Math.multiplyExact(values.length, 2)];
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
    if (!checkStructure() || keys.length != 2L * values.length ||
        nullMasks.length != values.length || hashCodes.length != values.length) {
      return false;
    }
    for (int slot = 0; slot < numUsed; ++slot) {
      int keyOffset = 2 * slot;
      if (values[slot] != null &&
          (hashCodes[slot] != spreadHash(calculateHashCode(slot)) ||
              findSlot(keys[keyOffset], keys[keyOffset + 1], nullMasks[slot], hashCodes[slot]) != slot)) {
        return false;
      }
    }
    return true;
  }
}
