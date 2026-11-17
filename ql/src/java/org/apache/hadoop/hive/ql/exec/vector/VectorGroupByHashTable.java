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
 * Shared segmented bucket chains and entry allocation for specialized aggregation tables.
 * Key lookup, batch preparation, and flushing remain in the concrete implementations.
 */
abstract class VectorGroupByHashTable {
  protected static final JavaDataModel JAVA_DATA_MODEL = JavaDataModel.get();
  protected static final double MAX_LOAD_FACTOR = 0.75;
  private static final int INITIAL_CAPACITY = 16;
  private static final int INITIAL_ENTRY_CAPACITY = (int) (INITIAL_CAPACITY * MAX_LOAD_FACTOR);

  // Bucket heads and next links store slot + 1; zero terminates a chain or the free list.
  protected int[][] buckets;
  protected int[][] next;
  protected VectorAggregationBufferRow[][] values;
  protected int bucketCapacity;
  protected int entryCapacity;
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
    bucketCapacity = capacity;
    resizeThreshold = (int) (capacity * MAX_LOAD_FACTOR);
    entryCapacity = resizeThreshold;
    int segmentCount = bucketSegment(capacity - 1) + 1;
    buckets = new int[segmentCount][];
    next = new int[segmentCount][];
    values = new VectorAggregationBufferRow[segmentCount][];
    for (int segment = 0; segment < segmentCount; ++segment) {
      buckets[segment] = new int[bucketSegmentSize(segment)];
      int entries = entrySegmentSize(segment);
      next[segment] = new int[entries];
      values[segment] = new VectorAggregationBufferRow[entries];
    }
    numEntries = 0;
    numUsed = 0;
    freeList = 0;
  }

  // Segment lengths are base, base, 2 * base, 4 * base, and so on.
  protected static int bucketSegment(int bucket) {
    int quotient = bucket / INITIAL_CAPACITY;
    return quotient == 0 ? 0 : Integer.SIZE - Integer.numberOfLeadingZeros(quotient);
  }

  protected static int bucketOffset(int bucket, int segment) {
    return bucket - (segment == 0 ? 0 : INITIAL_CAPACITY << (segment - 1));
  }

  private static int bucketSegmentSize(int segment) {
    return INITIAL_CAPACITY << (segment == 0 ? 0 : segment - 1);
  }

  protected static int entrySegment(int slot) {
    int quotient = slot / INITIAL_ENTRY_CAPACITY;
    return quotient == 0 ? 0 : Integer.SIZE - Integer.numberOfLeadingZeros(quotient);
  }

  protected static int entryOffset(int slot, int segment) {
    return slot - (segment == 0 ? 0 : INITIAL_ENTRY_CAPACITY << (segment - 1));
  }

  protected static int entrySegmentSize(int segment) {
    return INITIAL_ENTRY_CAPACITY << (segment == 0 ? 0 : segment - 1);
  }

  protected final int bucketHead(int bucket) {
    int segment = bucketSegment(bucket);
    return buckets[segment][bucketOffset(bucket, segment)];
  }

  protected final void setBucketHead(int bucket, int reference) {
    int segment = bucketSegment(bucket);
    buckets[segment][bucketOffset(bucket, segment)] = reference;
  }

  protected final int nextReference(int slot) {
    int segment = entrySegment(slot);
    return next[segment][entryOffset(slot, segment)];
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

  // Storage hooks are called only when segments are allocated.
  protected abstract void growKeyStorage(int segment, int capacity);

  protected abstract void resetKeyStorage();

  protected abstract void rehash();

  protected final void allocateSlot(int insertionSlot) {
    if (numEntries + 1 > resizeThreshold) {
      assert freeList == 0 && numUsed == entryCapacity;
      int newBucketCapacity = Math.multiplyExact(bucketCapacity, 2);
      int newEntryCapacity = Math.multiplyExact(entryCapacity, 2);
      int segment = buckets.length;
      buckets = Arrays.copyOf(buckets, segment + 1);
      next = Arrays.copyOf(next, segment + 1);
      values = Arrays.copyOf(values, segment + 1);
      buckets[segment] = new int[bucketCapacity];
      next[segment] = new int[entryCapacity];
      values[segment] = new VectorAggregationBufferRow[entryCapacity];
      growKeyStorage(segment, entryCapacity);
      bucketCapacity = newBucketCapacity;
      entryCapacity = newEntryCapacity;
      resizeThreshold = (int) (bucketCapacity * MAX_LOAD_FACTOR);
      // Rehash scans slots and overwrites retained next links without reading old chains.
      for (int oldSegment = 0; oldSegment < segment; ++oldSegment) {
        Arrays.fill(buckets[oldSegment], 0);
      }
      rehash();
      assert checkInvariants();
    }
    int segment = entrySegment(insertionSlot);
    int offset = entryOffset(insertionSlot, segment);
    if (freeList != 0) {
      assert insertionSlot == freeList - 1 && values[segment][offset] == null;
      freeList = next[segment][offset];
    } else {
      assert insertionSlot == numUsed && numUsed < entryCapacity;
      ++numUsed;
    }
  }

  protected static int spreadHash(int hashCode) {
    return hashCode ^ (hashCode >>> 16);
  }

  protected final void linkEntryWithSpreadHash(int slot, int spreadHashCode,
      VectorAggregationBufferRow value) {
    int segment = entrySegment(slot);
    int offset = entryOffset(slot, segment);
    assert value != null && values[segment][offset] == null;
    int bucket = spreadHashCode & (bucketCapacity - 1);
    values[segment][offset] = value;
    next[segment][offset] = bucketHead(bucket);
    setBucketHead(bucket, slot + 1);
    ++numEntries;
    assert numEntries <= resizeThreshold;
    assert (freeList == 0) == (numEntries == numUsed);
  }

  protected final void unlinkEntry(int bucket, int previousSlot, int slot) {
    int segment = entrySegment(slot);
    int offset = entryOffset(slot, segment);
    assert values[segment][offset] != null;
    assert previousSlot == -1 ? bucketHead(bucket) == slot + 1 :
        getValue(previousSlot) != null && nextReference(previousSlot) == slot + 1;
    if (previousSlot == -1) {
      setBucketHead(bucket, next[segment][offset]);
    } else {
      int previousSegment = entrySegment(previousSlot);
      next[previousSegment][entryOffset(previousSlot, previousSegment)] = next[segment][offset];
    }
    values[segment][offset] = null;
    next[segment][offset] = freeList;
    freeList = slot + 1;
    --numEntries;
    assert numEntries >= 0 && numEntries <= resizeThreshold;
    assert (freeList == 0) == (numEntries == numUsed);
  }

  final VectorAggregationBufferRow getValue(int slot) {
    int segment = entrySegment(slot);
    int offset = entryOffset(slot, segment);
    assert values[segment][offset] != null;
    return values[segment][offset];
  }

  int size() {
    return numEntries;
  }

  final int bucketCount() {
    return bucketCapacity;
  }

  final int firstSlot(int bucket) {
    return bucketHead(bucket) - 1;
  }

  final int nextSlot(int slot) {
    assert getValue(slot) != null;
    return nextReference(slot) - 1;
  }

  protected final void clearEntries() {
    int remaining = numUsed;
    for (int segment = 0; segment < buckets.length; ++segment) {
      Arrays.fill(buckets[segment], 0);
      int used = Math.min(values[segment].length, remaining);
      Arrays.fill(values[segment], 0, used, null);
      remaining -= used;
    }
    assert remaining == 0;
    numEntries = 0;
    numUsed = 0;
    freeList = 0;
  }

  /**
   * Checks unique chain/free-list membership and the live and allocated slot counts.
   */
  protected final boolean checkStructure() {
    if (next.length != values.length || buckets.length != values.length ||
        Integer.bitCount(bucketCapacity) != 1 ||
        buckets.length != bucketSegment(bucketCapacity - 1) + 1 ||
        values.length != entrySegment(entryCapacity - 1) + 1 ||
        resizeThreshold != (int) (bucketCapacity * MAX_LOAD_FACTOR) ||
        entryCapacity != resizeThreshold ||
        numEntries < 0 || numEntries > resizeThreshold ||
        numEntries > numUsed || numUsed > entryCapacity) {
      return false;
    }
    for (int segment = 0; segment < buckets.length; ++segment) {
      if (buckets[segment].length != bucketSegmentSize(segment) ||
          next[segment].length != entrySegmentSize(segment) ||
          values[segment].length != next[segment].length) {
        return false;
      }
    }
    boolean[] visited = new boolean[numUsed];
    int liveEntries = 0;
    for (int bucket = 0; bucket < bucketCapacity; ++bucket) {
      for (int ref = bucketHead(bucket); ref != 0; ref = nextReference(ref - 1)) {
        int slot = ref - 1;
        if (slot < 0 || slot >= numUsed || visited[slot]) {
          return false;
        }
        int segment = entrySegment(slot);
        if (values[segment][entryOffset(slot, segment)] == null) {
          return false;
        }
        visited[slot] = true;
        ++liveEntries;
      }
    }
    int freeEntries = 0;
    for (int ref = freeList; ref != 0; ref = nextReference(ref - 1)) {
      int slot = ref - 1;
      if (slot < 0 || slot >= numUsed || visited[slot]) {
        return false;
      }
      int segment = entrySegment(slot);
      if (values[segment][entryOffset(slot, segment)] != null) {
        return false;
      }
      visited[slot] = true;
      ++freeEntries;
    }
    for (int slot = numUsed; slot < entryCapacity; ++slot) {
      int segment = entrySegment(slot);
      if (values[segment][entryOffset(slot, segment)] != null) {
        return false;
      }
    }
    return liveEntries == numEntries && liveEntries + freeEntries == numUsed;
  }

  protected final long getEstimatedArrayMemorySize() {
    long memory = JAVA_DATA_MODEL.lengthForObjectArrayOfSize(buckets.length)
        + JAVA_DATA_MODEL.lengthForObjectArrayOfSize(next.length)
        + JAVA_DATA_MODEL.lengthForObjectArrayOfSize(values.length);
    for (int segment = 0; segment < buckets.length; ++segment) {
      memory += JAVA_DATA_MODEL.lengthForIntArrayOfSize(buckets[segment].length)
          + JAVA_DATA_MODEL.lengthForIntArrayOfSize(next[segment].length)
          + JAVA_DATA_MODEL.lengthForObjectArrayOfSize(values[segment].length);
    }
    return memory;
  }
}
