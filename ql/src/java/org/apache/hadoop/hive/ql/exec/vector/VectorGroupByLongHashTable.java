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
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperSingleLong;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hive.common.util.HashCodeUtil;

/**
 * Array-backed, separate-chaining aggregation hash table for a single long grouping key.
 */
final class VectorGroupByLongHashTable extends VectorGroupByHashTable {
  private long[] keys;
  private VectorAggregationBufferRow nullValue;

  VectorGroupByLongHashTable(int expectedEntries) {
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
      VectorHashKeyWrapperSingleLong kw = (VectorHashKeyWrapperSingleLong) keyWrappers[i];
      boolean isNull = kw.isNull(0);
      int slot = isNull ? 0 : findSlot(kw.getLongValue(0), kw.hashCode());
      VectorAggregationBufferRow aggregationBuffer = isNull ? getNullValue() :
          (slot >= 0 ? getValue(slot) : null);
      if (aggregationBuffer == null) {
        aggregationBuffer = context.allocateAggregationBuffer();
        if (isNull) {
          setNullValue(aggregationBuffer);
        } else {
          put(kw.getLongValue(0), kw.hashCode(), ~slot, aggregationBuffer);
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
    VectorHashKeyWrapperSingleLong keyWrapper =
        (VectorHashKeyWrapperSingleLong) outputKey;
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
        } else {
          keyWrapper.assignLong(0, 0, getKey(slot));
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
  int findSlot(long key, int hashCode) {
    assert keys.length == values.length && next.length == values.length;
    assert Integer.bitCount(buckets.length) == 1;
    assert numEntries >= 0 && numEntries <= numUsed && numUsed <= values.length &&
        numEntries <= resizeThreshold && resizeThreshold < buckets.length;
    assert (freeList == 0) == (numEntries == numUsed);
    assert hashCode == HashCodeUtil.calculateLongHashCode(key);
    int bucket = (hashCode ^ (hashCode >>> 16)) & (buckets.length - 1);
    for (int ref = buckets[bucket]; ref != 0; ref = next[ref - 1]) {
      int slot = ref - 1;
      if (keys[slot] == key) {
        return slot;
      }
    }
    return ~(freeList == 0 ? numUsed : freeList - 1);
  }

  void put(long key, int hashCode, int insertionSlot, VectorAggregationBufferRow value) {
    assert value != null;
    assert insertionSlot == ~findSlot(key, hashCode);
    allocateSlot(insertionSlot);
    keys[insertionSlot] = key;
    linkEntry(insertionSlot, hashCode, value);
    assert findSlot(key, hashCode) == insertionSlot;
    assert getValue(insertionSlot) == value;
  }

  long getKey(int slot) {
    assert values[slot] != null;
    return keys[slot];
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

  void remove(int bucket, int previousSlot, int slot) {
    assert values[slot] != null;
    assert bucket == ((HashCodeUtil.calculateLongHashCode(keys[slot]) ^
        (HashCodeUtil.calculateLongHashCode(keys[slot]) >>> 16)) & (buckets.length - 1));
    unlinkEntry(bucket, previousSlot, slot);
    assert findSlot(keys[slot], HashCodeUtil.calculateLongHashCode(keys[slot])) == ~slot;
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
    keys = Arrays.copyOf(keys, capacity);
  }

  @Override
  protected void resetKeyStorage() {
    keys = new long[values.length];
    nullValue = null;
  }

  @Override
  protected void rehash() {
    resizeBuckets();
    int mask = buckets.length - 1;
    int copiedEntries = 0;
    for (int slot = 0; slot < numUsed; ++slot) {
      if (values[slot] != null) {
        int hashCode = HashCodeUtil.calculateLongHashCode(keys[slot]);
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
    if (!checkStructure() || keys.length != values.length) {
      return false;
    }
    for (int slot = 0; slot < numUsed; ++slot) {
      if (values[slot] != null &&
          findSlot(keys[slot], HashCodeUtil.calculateLongHashCode(keys[slot])) != slot) {
        return false;
      }
    }
    return true;
  }

  @Override
  long getEstimatedFixedMemorySize() {
    JavaDataModel model = JavaDataModel.get();
    return JavaDataModel.alignUp(model.object() + 5L * model.ref() + 4L * model.primitive1(),
        model.memoryAlign()) + getEstimatedArrayMemorySize()
        + model.lengthForLongArrayOfSize(keys.length);
  }
}
