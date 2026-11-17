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
import org.apache.hive.common.util.HashCodeUtil;

/**
 * Array-backed, separate-chaining aggregation hash table for a single long grouping key.
 */
final class VectorGroupByLongHashTable extends VectorGroupByHashTable {
  private long[][] keys;
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
    assert Integer.bitCount(bucketCapacity) == 1;
    assert numEntries >= 0 && numEntries <= numUsed && numUsed <= entryCapacity &&
        numEntries <= resizeThreshold && resizeThreshold < bucketCapacity;
    assert (freeList == 0) == (numEntries == numUsed);
    assert hashCode == HashCodeUtil.calculateLongHashCode(key);
    int bucket = spreadHash(hashCode) & (bucketCapacity - 1);
    for (int ref = bucketHead(bucket); ref != 0;) {
      int slot = ref - 1;
      int segment = entrySegment(slot);
      int offset = entryOffset(slot, segment);
      if (keys[segment][offset] == key) {
        return slot;
      }
      ref = next[segment][offset];
    }
    return ~(freeList == 0 ? numUsed : freeList - 1);
  }

  void put(long key, int hashCode, int insertionSlot, VectorAggregationBufferRow value) {
    assert value != null;
    assert insertionSlot == ~findSlot(key, hashCode);
    allocateSlot(insertionSlot);
    int segment = entrySegment(insertionSlot);
    int offset = entryOffset(insertionSlot, segment);
    keys[segment][offset] = key;
    linkEntryWithSpreadHash(insertionSlot, spreadHash(hashCode), value);
    assert findSlot(key, hashCode) == insertionSlot;
    assert getValue(insertionSlot) == value;
  }

  long getKey(int slot) {
    int segment = entrySegment(slot);
    int offset = entryOffset(slot, segment);
    assert values[segment][offset] != null;
    return keys[segment][offset];
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
    int segment = entrySegment(slot);
    int offset = entryOffset(slot, segment);
    assert values[segment][offset] != null;
    assert bucket == (spreadHash(HashCodeUtil.calculateLongHashCode(keys[segment][offset])) &
        (bucketCapacity - 1));
    unlinkEntry(bucket, previousSlot, slot);
    assert findSlot(keys[segment][offset],
        HashCodeUtil.calculateLongHashCode(keys[segment][offset])) == ~slot;
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
  protected void growKeyStorage(int segment, int capacity) {
    keys = Arrays.copyOf(keys, segment + 1);
    keys[segment] = new long[capacity];
  }

  @Override
  protected void resetKeyStorage() {
    keys = new long[values.length][];
    for (int segment = 0; segment < values.length; ++segment) {
      keys[segment] = new long[values[segment].length];
    }
    nullValue = null;
  }

  @Override
  protected void rehash() {
    int mask = bucketCapacity - 1;
    int copiedEntries = 0;
    for (int slot = 0; slot < numUsed; ++slot) {
      int segment = entrySegment(slot);
      int offset = entryOffset(slot, segment);
      if (values[segment][offset] != null) {
        int hashCode = HashCodeUtil.calculateLongHashCode(keys[segment][offset]);
        int bucket = spreadHash(hashCode) & mask;
        next[segment][offset] = bucketHead(bucket);
        setBucketHead(bucket, slot + 1);
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
    for (int segment = 0; segment < values.length; ++segment) {
      if (keys[segment].length != values[segment].length) {
        return false;
      }
    }
    for (int slot = 0; slot < numUsed; ++slot) {
      int segment = entrySegment(slot);
      int offset = entryOffset(slot, segment);
      if (values[segment][offset] != null && findSlot(keys[segment][offset],
          HashCodeUtil.calculateLongHashCode(keys[segment][offset])) != slot) {
        return false;
      }
    }
    return true;
  }

  @Override
  long getEstimatedFixedMemorySize() {
    long memorySize = getEstimatedArrayMemorySize()
        + JAVA_DATA_MODEL.lengthForObjectArrayOfSize(keys.length);
    for (long[] segment : keys) {
      memorySize += JAVA_DATA_MODEL.lengthForLongArrayOfSize(segment.length);
    }
    return memorySize;
  }
}
