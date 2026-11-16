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
  private long[] keys0;
  private long[] keys1;
  private long[] keys2;
  private byte[] nullMasks;
  private VectorAggregationBufferRow nullValue;

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
      int slot = isNull ? 0 : findSlot(kw.getLongValue(0), kw.getLongValue(1),
          kw.getLongValue(2), nullMask, kw.hashCode());
      VectorAggregationBufferRow aggregationBuffer = isNull ? getNullValue() :
          (slot >= 0 ? getValue(slot) : null);
      if (aggregationBuffer == null) {
        aggregationBuffer = context.allocateAggregationBuffer();
        if (isNull) {
          setNullValue(aggregationBuffer);
        } else {
          put(kw.getLongValue(0), kw.getLongValue(1), kw.getLongValue(2), nullMask,
              kw.hashCode(), ~slot, aggregationBuffer);
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
    for (int bucket = -1; bucket < bucketCount(); ++bucket) {
      if (bucket == -1 && getNullValue() == null) {
        continue;
      }
      int previousSlot = -1;
      int slot = bucket == -1 ? 0 : firstSlot(bucket);
      while (slot != -1) {
        int nextSlot = bucket == -1 ? -1 : nextSlot(slot);
        VectorAggregationBufferRow bufferRow = bucket == -1 ? getNullValue() :
            getValue(slot);
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
          if ((nullMasks[slot] & 1) != 0) {
            keyWrapper.assignNullLong(0, 0);
          } else {
            keyWrapper.assignLong(0, 0, keys0[slot]);
          }
          if ((nullMasks[slot] & 2) != 0) {
            keyWrapper.assignNullLong(1, 1);
          } else {
            keyWrapper.assignLong(1, 1, keys1[slot]);
          }
          if ((nullMasks[slot] & 4) != 0) {
            keyWrapper.assignNullLong(2, 2);
          } else {
            keyWrapper.assignLong(2, 2, keys2[slot]);
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
    assert keys0.length == values.length && keys1.length == values.length &&
        keys2.length == values.length && nullMasks.length == values.length &&
        next.length == values.length;
    assert Integer.bitCount(buckets.length) == 1;
    assert numEntries >= 0 && numEntries <= numUsed && numUsed <= values.length &&
        numEntries <= resizeThreshold && resizeThreshold < buckets.length;
    assert (freeList == 0) == (numEntries == numUsed);
    assert nullMask >= 0 && nullMask < 7;
    assert hashCode == VectorHashKeyWrapperThreeLong.calculateHashCode(key0, key1, key2, nullMask);
    int bucket = (hashCode ^ (hashCode >>> 16)) & (buckets.length - 1);
    for (int ref = buckets[bucket]; ref != 0; ref = next[ref - 1]) {
      int slot = ref - 1;
      if (nullMasks[slot] == nullMask && keys0[slot] == key0 &&
          keys1[slot] == key1 && keys2[slot] == key2) {
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
    keys0[insertionSlot] = key0;
    keys1[insertionSlot] = key1;
    keys2[insertionSlot] = key2;
    nullMasks[insertionSlot] = (byte) nullMask;
    linkEntry(insertionSlot, hashCode, value);
    assert findSlot(key0, key1, key2, nullMask, hashCode) == insertionSlot;
    assert getValue(insertionSlot) == value;
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

  private int getHashCode(int slot) {
    return VectorHashKeyWrapperThreeLong.calculateHashCode(
        keys0[slot], keys1[slot], keys2[slot], nullMasks[slot]);
  }

  void remove(int bucket, int previousSlot, int slot) {
    assert values[slot] != null;
    assert bucket == ((getHashCode(slot) ^ (getHashCode(slot) >>> 16)) & (buckets.length - 1));
    unlinkEntry(bucket, previousSlot, slot);
    assert findSlot(keys0[slot], keys1[slot], keys2[slot], nullMasks[slot],
        getHashCode(slot)) == ~slot;
  }

  @Override
  int size() {
    return numEntries + (nullValue == null ? 0 : 1);
  }

  @Override
  void clear() {
    clearEntries();
    nullValue = null;
    assert checkInvariants();
  }

  @Override
  protected void growKeyStorage(int capacity) {
    keys0 = Arrays.copyOf(keys0, capacity);
    keys1 = Arrays.copyOf(keys1, capacity);
    keys2 = Arrays.copyOf(keys2, capacity);
    nullMasks = Arrays.copyOf(nullMasks, capacity);
  }

  @Override
  protected void resetKeyStorage() {
    keys0 = new long[values.length];
    keys1 = new long[values.length];
    keys2 = new long[values.length];
    nullMasks = new byte[values.length];
    nullValue = null;
  }

  @Override
  protected void rehash() {
    resizeBuckets();
    int mask = buckets.length - 1;
    int copiedEntries = 0;
    for (int slot = 0; slot < numUsed; ++slot) {
      if (values[slot] != null) {
        int hashCode = getHashCode(slot);
        int bucket = (hashCode ^ (hashCode >>> 16)) & mask;
        next[slot] = buckets[bucket];
        buckets[bucket] = slot + 1;
        ++copiedEntries;
      }
    }
    assert copiedEntries == numEntries;
    assert checkInvariants();
  }

  /**
   * Checks bucket-chain reachability, entry counts, and free-list membership.
   */
  @Override
  boolean checkInvariants() {
    if (!checkStructure() || keys0.length != values.length || keys1.length != values.length ||
        keys2.length != values.length || nullMasks.length != values.length) {
      return false;
    }
    for (int slot = 0; slot < numUsed; ++slot) {
      if (values[slot] != null &&
          findSlot(keys0[slot], keys1[slot], keys2[slot], nullMasks[slot],
              getHashCode(slot)) != slot) {
        return false;
      }
    }
    return true;
  }

  @Override
  long getEstimatedFixedMemorySize() {
    return getEstimatedArrayMemorySize()
        + JAVA_DATA_MODEL.lengthForLongArrayOfSize(keys0.length)
        + JAVA_DATA_MODEL.lengthForLongArrayOfSize(keys1.length)
        + JAVA_DATA_MODEL.lengthForLongArrayOfSize(keys2.length)
        + JAVA_DATA_MODEL.lengthForByteArrayOfSize(nullMasks.length);
  }
}
