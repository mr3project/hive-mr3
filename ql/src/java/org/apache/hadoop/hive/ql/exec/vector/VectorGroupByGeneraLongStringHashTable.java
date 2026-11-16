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
import org.apache.hadoop.hive.ql.util.JavaDataModel;

/**
 * Array-backed, separate-chaining aggregation hash table for long and string grouping keys.
 */
final class VectorGroupByGeneraLongStringHashTable {
  private static final double MAX_LOAD_FACTOR = 0.75;
  private static final int INITIAL_CAPACITY = 16;
  private static final byte[] EMPTY_BYTES = new byte[0];

  private final int[] longIndices;
  private final int[] stringIndices;
  private final int keyCount;
  private final int nullWordCount;

  // Bucket heads and next links store slot + 1; zero terminates a chain or the free list.
  private int[] buckets;
  private int[] next;
  private int[] hashes;
  private VectorAggregationBufferRow[] values;
  // Entry-major storage keeps each key's offsets stable when entry arrays grow.
  private long[] longValues;
  private byte[][] stringValues;
  private long[] nullWords;
  private int numEntries;
  private int numUsed;
  private int freeList;
  private int resizeThreshold;
  private long stringMemorySize;

  VectorGroupByGeneraLongStringHashTable(int expectedEntries, VectorColumnSetInfo keyInfo) {
    longIndices = keyInfo.longIndices;
    stringIndices = keyInfo.stringIndices;
    keyCount = keyInfo.keyCount;
    nullWordCount = (keyCount + Long.SIZE - 1) / Long.SIZE;
    assert keyCount == longIndices.length + stringIndices.length;
    int capacity = INITIAL_CAPACITY;
    while (expectedEntries > (int) (capacity * MAX_LOAD_FACTOR)) {
      capacity = Math.multiplyExact(capacity, 2);
    }
    buckets = new int[capacity];
    resizeThreshold = (int) (capacity * MAX_LOAD_FACTOR);
    next = new int[resizeThreshold];
    hashes = new int[resizeThreshold];
    values = new VectorAggregationBufferRow[resizeThreshold];
    longValues = new long[Math.multiplyExact(resizeThreshold, longIndices.length)];
    stringValues = new byte[Math.multiplyExact(resizeThreshold, stringIndices.length)][];
    nullWords = new long[Math.multiplyExact(resizeThreshold, nullWordCount)];
    assert checkInvariants();
  }

  /**
   * Returns the matching slot, or the bitwise complement of an insertion slot.
   */
  int findSlot(VectorHashKeyWrapperBase key) {
    assert numEntries >= 0 && numEntries <= numUsed && numUsed <= values.length;
    assert numEntries <= resizeThreshold && Integer.bitCount(buckets.length) == 1;
    assert (freeList == 0) == (numEntries == numUsed);
    int hash = key.hashCode();
    int bucket = (hash ^ (hash >>> 16)) & (buckets.length - 1);
    for (int ref = buckets[bucket]; ref != 0; ref = next[ref - 1]) {
      int slot = ref - 1;
      if (hashes[slot] == hash && equalKey(slot, key)) {
        return slot;
      }
    }
    return ~(freeList == 0 ? numUsed : freeList - 1);
  }

  private boolean isNull(int slot, int keyIndex) {
    return (nullWords[slot * nullWordCount + keyIndex / Long.SIZE] &
        (1L << (keyIndex % Long.SIZE))) != 0;
  }

  private boolean equalKey(int slot, VectorHashKeyWrapperBase key) {
    for (int i = 0; i < keyCount; ++i) {
      if (isNull(slot, i) != key.isNull(i)) {
        return false;
      }
    }
    for (int i = 0, offset = slot * longIndices.length; i < longIndices.length; ++i) {
      if (longValues[offset + i] != key.getLongValue(i)) {
        return false;
      }
    }
    for (int i = 0, offset = slot * stringIndices.length; i < stringIndices.length; ++i) {
      byte[] bytes = stringValues[offset + i];
      if (bytes != null && !StringExpr.equal(bytes, 0, bytes.length,
          key.getBytes(i), key.getByteStart(i), key.getByteLength(i))) {
        return false;
      }
    }
    return true;
  }

  void put(VectorHashKeyWrapperBase key, int insertionSlot, VectorAggregationBufferRow value) {
    assert value != null;
    assert insertionSlot == ~findSlot(key);
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
    int nullOffset = insertionSlot * nullWordCount;
    Arrays.fill(nullWords, nullOffset, nullOffset + nullWordCount, 0);
    for (int i = 0; i < keyCount; ++i) {
      if (key.isNull(i)) {
        nullWords[nullOffset + i / Long.SIZE] |= 1L << (i % Long.SIZE);
      }
    }
    for (int i = 0, offset = insertionSlot * longIndices.length; i < longIndices.length; ++i) {
      longValues[offset + i] = key.getLongValue(i);
    }
    JavaDataModel model = JavaDataModel.get();
    for (int i = 0, offset = insertionSlot * stringIndices.length; i < stringIndices.length; ++i) {
      assert stringValues[offset + i] == null;
      if (!key.isNull(stringIndices[i])) {
        int length = key.getByteLength(i);
        byte[] bytes = length == 0 ? EMPTY_BYTES : Arrays.copyOfRange(
            key.getBytes(i), key.getByteStart(i), key.getByteStart(i) + length);
        stringValues[offset + i] = bytes;
        if (length != 0) {
          stringMemorySize += model.lengthForByteArrayOfSize(length);
        }
      }
    }
    int hash = key.hashCode();
    int bucket = (hash ^ (hash >>> 16)) & (buckets.length - 1);
    hashes[insertionSlot] = hash;
    values[insertionSlot] = value;
    next[insertionSlot] = buckets[bucket];
    buckets[bucket] = insertionSlot + 1;
    ++numEntries;
    assert numEntries <= resizeThreshold;
    assert findSlot(key) == insertionSlot && equalKey(insertionSlot, key);
    assert getValue(insertionSlot) == value;
  }

  VectorAggregationBufferRow getValue(int slot) {
    assert values[slot] != null;
    return values[slot];
  }

  void copyKey(int slot, VectorHashKeyWrapperBase target) {
    assert values[slot] != null;
    target.clearIsNull();
    for (int i = 0, offset = slot * longIndices.length; i < longIndices.length; ++i) {
      if (isNull(slot, longIndices[i])) {
        target.assignNullLong(longIndices[i], i);
      } else {
        target.assignLong(longIndices[i], i, longValues[offset + i]);
      }
    }
    for (int i = 0, offset = slot * stringIndices.length; i < stringIndices.length; ++i) {
      byte[] bytes = stringValues[offset + i];
      if (bytes == null) {
        target.assignNullString(stringIndices[i], i);
      } else {
        target.assignString(i, bytes, 0, bytes.length);
      }
    }
  }

  void remove(int bucket, int previousSlot, int slot) {
    assert values[slot] != null;
    assert bucket == ((hashes[slot] ^ (hashes[slot] >>> 16)) & (buckets.length - 1));
    assert previousSlot == -1 ? buckets[bucket] == slot + 1 :
        values[previousSlot] != null && next[previousSlot] == slot + 1;
    if (previousSlot == -1) {
      buckets[bucket] = next[slot];
    } else {
      next[previousSlot] = next[slot];
    }
    JavaDataModel model = JavaDataModel.get();
    for (int i = 0, offset = slot * stringIndices.length; i < stringIndices.length; ++i) {
      byte[] bytes = stringValues[offset + i];
      if (bytes != null && bytes.length != 0) {
        stringMemorySize -= model.lengthForByteArrayOfSize(bytes.length);
      }
      stringValues[offset + i] = null;
    }
    Arrays.fill(nullWords, slot * nullWordCount, (slot + 1) * nullWordCount, 0);
    values[slot] = null;
    next[slot] = freeList;
    freeList = slot + 1;
    --numEntries;
    assert numEntries >= 0 && numEntries <= resizeThreshold && stringMemorySize >= 0;
    assert (freeList == 0) == (numEntries == numUsed);
  }

  int size() {
    return numEntries;
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
    Arrays.fill(stringValues, 0, numUsed * stringIndices.length, null);
    Arrays.fill(nullWords, 0, numUsed * nullWordCount, 0);
    numEntries = 0;
    numUsed = 0;
    freeList = 0;
    stringMemorySize = 0;
    assert checkInvariants();
  }

  private void growEntries() {
    int capacity = Math.multiplyExact(values.length, 2);
    next = Arrays.copyOf(next, capacity);
    hashes = Arrays.copyOf(hashes, capacity);
    values = Arrays.copyOf(values, capacity);
    longValues = Arrays.copyOf(longValues, Math.multiplyExact(capacity, longIndices.length));
    stringValues = Arrays.copyOf(stringValues, Math.multiplyExact(capacity, stringIndices.length));
    nullWords = Arrays.copyOf(nullWords, Math.multiplyExact(capacity, nullWordCount));
    assert checkInvariants();
  }

  private void rehash(int capacity) {
    buckets = new int[capacity];
    resizeThreshold = (int) (capacity * MAX_LOAD_FACTOR);
    int copiedEntries = 0;
    for (int slot = 0; slot < numUsed; ++slot) {
      if (values[slot] != null) {
        int hash = hashes[slot];
        int bucket = (hash ^ (hash >>> 16)) & (capacity - 1);
        next[slot] = buckets[bucket];
        buckets[bucket] = slot + 1;
        ++copiedEntries;
      }
    }
    assert copiedEntries == numEntries;
    assert checkInvariants();
  }

  private boolean equalKeys(int slot, int otherSlot) {
    for (int i = 0; i < nullWordCount; ++i) {
      if (nullWords[slot * nullWordCount + i] != nullWords[otherSlot * nullWordCount + i]) {
        return false;
      }
    }
    for (int i = 0; i < longIndices.length; ++i) {
      if (longValues[slot * longIndices.length + i] !=
          longValues[otherSlot * longIndices.length + i]) {
        return false;
      }
    }
    for (int i = 0; i < stringIndices.length; ++i) {
      if (!Arrays.equals(stringValues[slot * stringIndices.length + i],
          stringValues[otherSlot * stringIndices.length + i])) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks chain reachability, unique key membership, the free list, and owned string bytes.
   */
  boolean checkInvariants() {
    if (next.length != values.length || hashes.length != values.length ||
        longValues.length != (long) values.length * longIndices.length ||
        stringValues.length != (long) values.length * stringIndices.length ||
        nullWords.length != (long) values.length * nullWordCount ||
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
        if (slot < 0 || slot >= numUsed || visited[slot] || values[slot] == null ||
            ((hashes[slot] ^ (hashes[slot] >>> 16)) & (buckets.length - 1)) != bucket) {
          return false;
        }
        visited[slot] = true;
        ++liveEntries;
        for (int other = buckets[bucket]; other != ref; other = next[other - 1]) {
          if (hashes[slot] == hashes[other - 1] && equalKeys(slot, other - 1)) {
            return false;
          }
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
    long stringBytes = 0;
    JavaDataModel model = JavaDataModel.get();
    for (int slot = 0; slot < values.length; ++slot) {
      if (slot >= numUsed && values[slot] != null) {
        return false;
      }
      for (int i = 0; i < stringIndices.length; ++i) {
        byte[] bytes = stringValues[slot * stringIndices.length + i];
        if (values[slot] == null ? bytes != null :
            (bytes == null) != isNull(slot, stringIndices[i])) {
          return false;
        }
        if (bytes != null && bytes.length != 0) {
          stringBytes += model.lengthForByteArrayOfSize(bytes.length);
        }
      }
      if (values[slot] == null) {
        for (int i = 0; i < nullWordCount; ++i) {
          if (nullWords[slot * nullWordCount + i] != 0) {
            return false;
          }
        }
      }
    }
    return liveEntries == numEntries && liveEntries + freeEntries == numUsed &&
        stringBytes == stringMemorySize;
  }

  long getEstimatedFixedMemorySize() {
    JavaDataModel model = JavaDataModel.get();
    // Column mappings are shared with the compiled key layout.
    return JavaDataModel.alignUp(model.object() + 9L * model.ref() + 6L * model.primitive1() +
        model.primitive2(), model.memoryAlign()) + model.lengthForIntArrayOfSize(buckets.length)
        + model.lengthForIntArrayOfSize(next.length) + model.lengthForIntArrayOfSize(hashes.length)
        + model.lengthForObjectArrayOfSize(values.length)
        + model.lengthForLongArrayOfSize(longValues.length)
        + model.lengthForObjectArrayOfSize(stringValues.length)
        + model.lengthForLongArrayOfSize(nullWords.length);
  }

  long getEstimatedMemorySize() {
    return getEstimatedFixedMemorySize() + stringMemorySize;
  }
}
