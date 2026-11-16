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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.util.JavaDataModel;

/**
 * Shared array-backed bucket chains and entry allocation for specialized aggregation tables.
 * Key lookup, batch preparation, and flushing remain in the concrete implementations.
 */
abstract class VectorGroupByHashTable {
  protected static final JavaDataModel JAVA_DATA_MODEL = JavaDataModel.get();
  protected static final double MAX_LOAD_FACTOR = 0.75;
  private static final int INITIAL_CAPACITY = 16;

  // Bucket heads and next links store slot + 1; zero terminates a chain or the free list.
  protected int[] buckets;
  protected int[] next;
  protected VectorAggregationBufferRow[] values;
  protected int numEntries;
  protected int numUsed;
  protected int freeList;
  protected int resizeThreshold;

  VectorGroupByHashTable(int expectedEntries) {
    int capacity = INITIAL_CAPACITY;
    while (expectedEntries > (int) (capacity * MAX_LOAD_FACTOR)) {
      capacity = Math.multiplyExact(capacity, 2);
    }
    initializeEntries(capacity);
  }

  private void initializeEntries(int capacity) {
    buckets = new int[capacity];
    resizeThreshold = (int) (capacity * MAX_LOAD_FACTOR);
    next = new int[resizeThreshold];
    values = new VectorAggregationBufferRow[resizeThreshold];
    numEntries = 0;
    numUsed = 0;
    freeList = 0;
  }

  abstract int prepareBatch(VectorGroupByOperator.ProcessingModeHashAggregate context,
      VectorHashKeyWrapperBase[] keyWrappers, int size, VectorAggregationBufferBatch batchInfo)
      throws HiveException;

  abstract int flush(VectorGroupByOperator.ProcessingModeHashAggregate context, boolean all,
      int entriesToFlush, int avgAccess, VectorHashKeyWrapperBase outputKey) throws HiveException;

  abstract void clear();

  /**
   * Discards all entries and replaces their storage at initial capacity after a full flush.
   */
  final void reset() {
    initializeEntries(INITIAL_CAPACITY);
    resetKeyStorage();
    assert size() == 0;
    assert checkInvariants();
  }

  abstract long getEstimatedFixedMemorySize();

  long getEstimatedMemorySize() {
    return getEstimatedFixedMemorySize();
  }

  abstract boolean checkInvariants();

  // Storage hooks are called only when arrays are allocated or resized.
  protected abstract void growKeyStorage(int capacity);

  protected abstract void resetKeyStorage();

  protected abstract void rehash();

  protected final void allocateSlot(int insertionSlot) {
    if (numEntries + 1 > resizeThreshold) {
      rehash();
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
  }

  protected final void linkEntry(int slot, int hash, VectorAggregationBufferRow value) {
    assert value != null && values[slot] == null;
    int bucket = (hash ^ (hash >>> 16)) & (buckets.length - 1);
    values[slot] = value;
    next[slot] = buckets[bucket];
    buckets[bucket] = slot + 1;
    ++numEntries;
    assert numEntries <= resizeThreshold;
    assert (freeList == 0) == (numEntries == numUsed);
  }

  protected final void unlinkEntry(int bucket, int previousSlot, int slot) {
    assert values[slot] != null;
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
    assert (freeList == 0) == (numEntries == numUsed);
  }

  final VectorAggregationBufferRow getValue(int slot) {
    assert values[slot] != null;
    return values[slot];
  }

  int size() {
    return numEntries;
  }

  final int bucketCount() {
    return buckets.length;
  }

  final int firstSlot(int bucket) {
    return buckets[bucket] - 1;
  }

  final int nextSlot(int slot) {
    assert values[slot] != null;
    return next[slot] - 1;
  }

  protected final void clearEntries() {
    Arrays.fill(buckets, 0);
    Arrays.fill(values, 0, numUsed, null);
    numEntries = 0;
    numUsed = 0;
    freeList = 0;
  }

  private void growEntries() {
    int capacity = Math.multiplyExact(values.length, 2);
    next = Arrays.copyOf(next, capacity);
    values = Arrays.copyOf(values, capacity);
    growKeyStorage(capacity);
    assert checkInvariants();
  }

  protected final void resizeBuckets() {
    int capacity = Math.multiplyExact(buckets.length, 2);
    buckets = new int[capacity];
    resizeThreshold = (int) (capacity * MAX_LOAD_FACTOR);
  }

  /**
   * Checks unique chain/free-list membership and the live and allocated slot counts.
   */
  protected final boolean checkStructure() {
    if (next.length != values.length || Integer.bitCount(buckets.length) != 1 ||
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

  protected final long getEstimatedArrayMemorySize() {
    return JAVA_DATA_MODEL.lengthForIntArrayOfSize(buckets.length)
        + JAVA_DATA_MODEL.lengthForIntArrayOfSize(next.length)
        + JAVA_DATA_MODEL.lengthForObjectArrayOfSize(values.length);
  }
}
