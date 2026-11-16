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
 * Array-backed, separate-chaining aggregation hash table for a single long grouping key.
 */
final class VectorGroupByLongHashTable {
  private static final double MAX_LOAD_FACTOR = 0.75;
  private static final int INITIAL_CAPACITY = 16;

  // Bucket heads and next links store slot + 1; zero terminates a chain.
  private int[] buckets;
  private int[] next;
  private long[] keys;
  private VectorAggregationBufferRow[] values;
  private VectorAggregationBufferRow nullValue;
  private int numEntries;
  private int numUsed;
  private int freeList;
  private int resizeThreshold;

  VectorGroupByLongHashTable(int expectedEntries) {
    int capacity = INITIAL_CAPACITY;
    while (expectedEntries > (int) (capacity * MAX_LOAD_FACTOR)) {
      capacity = Math.multiplyExact(capacity, 2);
    }
    buckets = new int[capacity];
    resizeThreshold = (int) (capacity * MAX_LOAD_FACTOR);
    next = new int[resizeThreshold];
    keys = new long[resizeThreshold];
    values = new VectorAggregationBufferRow[resizeThreshold];
    assert checkInvariants();
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
    if (numEntries + 1 > resizeThreshold) {
      rehash(Math.multiplyExact(buckets.length, 2));
    }
    if (freeList != 0) {
      assert insertionSlot == freeList - 1 && values[insertionSlot] == null;
      freeList = next[insertionSlot];
    } else {
      assert insertionSlot == numUsed;
      if (numUsed == values.length) {
        growEntries();
      }
      ++numUsed;
    }
    int bucket = (hashCode ^ (hashCode >>> 16)) & (buckets.length - 1);
    keys[insertionSlot] = key;
    values[insertionSlot] = value;
    next[insertionSlot] = buckets[bucket];
    buckets[bucket] = insertionSlot + 1;
    ++numEntries;
    assert numEntries <= resizeThreshold;
    assert (freeList == 0) == (numEntries == numUsed);
    assert findSlot(key, hashCode) == insertionSlot;
    assert getValue(insertionSlot) == value;
  }

  VectorAggregationBufferRow getValue(int slot) {
    assert values[slot] != null;
    return values[slot];
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
    assert previousSlot == -1 ? buckets[bucket] == slot + 1 :
        values[previousSlot] != null && next[previousSlot] == slot + 1;
    if (previousSlot == -1) {
      buckets[bucket] = next[slot];
    } else {
      next[previousSlot] = next[slot];
    }
    values[slot] = null;
    next[slot] = freeList;
    freeList = slot + 1;
    --numEntries;
    assert numEntries >= 0 && numEntries <= resizeThreshold;
    assert findSlot(keys[slot], HashCodeUtil.calculateLongHashCode(keys[slot])) == ~slot;
  }

  int size() {
    return numEntries + (nullValue == null ? 0 : 1);
  }

  int bucketCount() {
    return buckets.length;
  }

  int firstSlot(int bucket) {
    return buckets[bucket] - 1;
  }

  int nextSlot(int slot) {
    assert values[slot] != null;
    return next[slot] - 1;
  }

  void clear() {
    Arrays.fill(buckets, 0);
    Arrays.fill(values, 0, numUsed, null);
    nullValue = null;
    numEntries = 0;
    numUsed = 0;
    freeList = 0;
    assert checkInvariants();
  }

  private void growEntries() {
    int capacity = Math.multiplyExact(values.length, 2);
    next = Arrays.copyOf(next, capacity);
    keys = Arrays.copyOf(keys, capacity);
    values = Arrays.copyOf(values, capacity);
    assert checkInvariants();
  }

  private void rehash(int capacity) {
    buckets = new int[capacity];
    resizeThreshold = (int) (capacity * MAX_LOAD_FACTOR);
    int mask = capacity - 1;
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
  boolean checkInvariants() {
    if (keys.length != values.length || next.length != values.length ||
        Integer.bitCount(buckets.length) != 1 ||
        resizeThreshold != (int) (buckets.length * MAX_LOAD_FACTOR) ||
        numEntries < 0 || numEntries > resizeThreshold ||
        numEntries > numUsed || numUsed > values.length) {
      return false;
    }
    boolean[] visited = new boolean[numUsed];
    int liveEntries = 0;
    for (int bucket = 0; bucket < buckets.length; ++bucket) {
      for (int ref = buckets[bucket]; ref != 0; ref = next[ref - 1]) {
        int slot = ref - 1;
        if (slot < 0 || slot >= numUsed || visited[slot] || values[slot] == null) {
          return false;
        }
        visited[slot] = true;
        ++liveEntries;
        int hashCode = HashCodeUtil.calculateLongHashCode(keys[slot]);
        if (((hashCode ^ (hashCode >>> 16)) & (buckets.length - 1)) != bucket ||
            findSlot(keys[slot], hashCode) != slot) {
          return false;
        }
      }
    }
    int freeEntries = 0;
    for (int ref = freeList; ref != 0; ref = next[ref - 1]) {
      int slot = ref - 1;
      if (slot < 0 || slot >= numUsed || visited[slot] || values[slot] != null) {
        return false;
      }
      visited[slot] = true;
      ++freeEntries;
    }
    for (int slot = numUsed; slot < values.length; ++slot) {
      if (values[slot] != null) {
        return false;
      }
    }
    return liveEntries == numEntries && liveEntries + freeEntries == numUsed;
  }

  long getEstimatedMemorySize() {
    JavaDataModel model = JavaDataModel.get();
    return JavaDataModel.alignUp(model.object() + 5L * model.ref() + 4L * model.primitive1(),
        model.memoryAlign()) + model.lengthForIntArrayOfSize(buckets.length)
        + model.lengthForIntArrayOfSize(next.length) + model.lengthForLongArrayOfSize(keys.length)
        + model.lengthForObjectArrayOfSize(values.length);
  }
}
