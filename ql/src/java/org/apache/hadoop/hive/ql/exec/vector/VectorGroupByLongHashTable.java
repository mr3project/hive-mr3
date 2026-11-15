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

import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hive.common.util.HashCodeUtil;

/**
 * Linear-probing aggregation hash table for a single long grouping key.
 */
final class VectorGroupByLongHashTable {
  private static final double MAX_LOAD_FACTOR = 0.60;
  private static final int INITIAL_CAPACITY = 16;
  private static final VectorAggregationBufferRow DELETED = new VectorAggregationBufferRow(null);

  private long[] keys;
  private VectorAggregationBufferRow[] values;
  private VectorAggregationBufferRow nullValue;
  private int numEntries;
  private int numDeleted;
  private int resizeThreshold;

  VectorGroupByLongHashTable(int expectedEntries) {
    int capacity = INITIAL_CAPACITY;
    while (expectedEntries > (int) (capacity * MAX_LOAD_FACTOR)) {
      capacity = Math.multiplyExact(capacity, 2);
    }
    keys = new long[capacity];
    values = new VectorAggregationBufferRow[capacity];
    resizeThreshold = (int) (capacity * MAX_LOAD_FACTOR);
  }

  /**
   * Returns the matching slot, or the bitwise complement of an insertion slot.
   */
  int findSlot(long key, int hashCode) {
    assert keys.length == values.length && Integer.bitCount(values.length) == 1;
    assert numEntries >= 0 && numDeleted >= 0 &&
        numEntries + numDeleted <= resizeThreshold && resizeThreshold < values.length;
    assert hashCode == HashCodeUtil.calculateLongHashCode(key);
    int mask = values.length - 1;
    int slot = (hashCode ^ (hashCode >>> 16)) & mask;
    int firstDeleted = -1;
    // Occupancy stays below capacity, so every probe reaches an empty slot or its key.
    while (true) {
      VectorAggregationBufferRow value = values[slot];
      if (value == null) {
        return ~(firstDeleted == -1 ? slot : firstDeleted);
      }
      if (value == DELETED) {
        if (firstDeleted == -1) {
          firstDeleted = slot;
        }
      } else if (keys[slot] == key) {
        return slot;
      }
      slot = (slot + 1) & mask;
    }
  }

  void put(long key, int hashCode, int insertionSlot, VectorAggregationBufferRow value) {
    assert value != null && value != DELETED;
    assert values[insertionSlot] == null || values[insertionSlot] == DELETED;
    assert insertionSlot == ~findSlot(key, hashCode);
    if (numEntries + 1 > resizeThreshold) {
      rehash(Math.multiplyExact(values.length, 2));
      insertionSlot = ~findSlot(key, hashCode);
    } else if (values[insertionSlot] == null && numEntries + numDeleted + 1 > resizeThreshold) {
      rehash(values.length);
      insertionSlot = ~findSlot(key, hashCode);
    }
    assert values[insertionSlot] == null || values[insertionSlot] == DELETED;
    if (values[insertionSlot] == DELETED) {
      --numDeleted;
    }
    keys[insertionSlot] = key;
    values[insertionSlot] = value;
    ++numEntries;
    assert numEntries + numDeleted <= resizeThreshold;
    assert findSlot(key, hashCode) == insertionSlot;
    assert getValue(insertionSlot) == value;
  }

  VectorAggregationBufferRow getValue(int slot) {
    VectorAggregationBufferRow value = values[slot];
    return value == DELETED ? null : value;
  }

  long getKey(int slot) {
    assert values[slot] != null && values[slot] != DELETED;
    return keys[slot];
  }

  VectorAggregationBufferRow getNullValue() {
    return nullValue;
  }

  void setNullValue(VectorAggregationBufferRow value) {
    assert nullValue == null && value != null && value != DELETED;
    nullValue = value;
  }

  void removeNullValue() {
    assert nullValue != null;
    nullValue = null;
  }

  void remove(int slot) {
    assert values[slot] != null && values[slot] != DELETED;
    values[slot] = DELETED;
    --numEntries;
    ++numDeleted;
    assert numEntries >= 0 && numEntries + numDeleted <= resizeThreshold;
  }

  int size() {
    return numEntries + (nullValue == null ? 0 : 1);
  }

  int capacity() {
    return values.length;
  }

  void clear() {
    Arrays.fill(values, null);
    nullValue = null;
    numEntries = 0;
    numDeleted = 0;
  }

  /**
   * Cleans up tombstones after an iteration that may have removed entries.
   */
  void compact() {
    if (numEntries == 0) {
      Arrays.fill(values, null);
      numDeleted = 0;
      assert checkInvariants();
    } else if (numDeleted > numEntries) {
      rehash(values.length);
    }
  }

  private void rehash(int capacity) {
    long[] oldKeys = keys;
    VectorAggregationBufferRow[] oldValues = values;
    keys = new long[capacity];
    values = new VectorAggregationBufferRow[capacity];
    resizeThreshold = (int) (capacity * MAX_LOAD_FACTOR);
    numDeleted = 0;
    int mask = capacity - 1;
    int copiedEntries = 0;
    for (int i = 0; i < oldValues.length; ++i) {
      VectorAggregationBufferRow value = oldValues[i];
      if (value != null && value != DELETED) {
        long key = oldKeys[i];
        int hashCode = HashCodeUtil.calculateLongHashCode(key);
        int slot = (hashCode ^ (hashCode >>> 16)) & mask;
        while (values[slot] != null) {
          slot = (slot + 1) & mask;
        }
        keys[slot] = key;
        values[slot] = value;
        ++copiedEntries;
      }
    }
    assert copiedEntries == numEntries;
    assert checkInvariants();
  }

  /**
   * Checks probe reachability and the live/tombstone counts after rehashing.
   */
  private boolean checkInvariants() {
    if (keys.length != values.length || Integer.bitCount(values.length) != 1 ||
        numEntries < 0 || numDeleted < 0 ||
        numEntries + numDeleted > resizeThreshold || resizeThreshold >= values.length) {
      return false;
    }
    int liveEntries = 0;
    int deletedEntries = 0;
    for (int slot = 0; slot < values.length; ++slot) {
      VectorAggregationBufferRow value = values[slot];
      if (value == DELETED) {
        ++deletedEntries;
      } else if (value != null) {
        ++liveEntries;
        int hashCode = HashCodeUtil.calculateLongHashCode(keys[slot]);
        if (findSlot(keys[slot], hashCode) != slot) {
          return false;
        }
      }
    }
    return liveEntries == numEntries && deletedEntries == numDeleted;
  }

  long getEstimatedMemorySize() {
    JavaDataModel model = JavaDataModel.get();
    return JavaDataModel.alignUp(model.object() + 3L * model.ref() + 3L * model.primitive1(),
        model.memoryAlign()) + model.lengthForLongArrayOfSize(keys.length)
        + model.lengthForObjectArrayOfSize(values.length);
  }
}
