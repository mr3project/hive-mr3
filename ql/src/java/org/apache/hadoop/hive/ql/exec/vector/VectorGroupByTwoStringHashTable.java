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
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperTwoString;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Array-backed, separate-chaining aggregation hash table for two string grouping keys.
 */
final class VectorGroupByTwoStringHashTable extends VectorGroupByHashTable {
  // Each slot stores its two string references consecutively.
  private byte[][] keys;
  // Cached hashes used for lookup and bucket selection.
  private int[] hashCodes;
  private long keyMemorySize;

  VectorGroupByTwoStringHashTable(int expectedEntries) {
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
      VectorHashKeyWrapperTwoString kw = (VectorHashKeyWrapperTwoString) keyWrappers[i];
      boolean isNull = kw.isNull(0) && kw.isNull(1);
      int hashCode = isNull ? 0 : kw.hashCode();
      int slot = isNull ? 0 : findSlot(kw.getBytes(0), kw.getByteStart(0), kw.getByteLength(0),
          kw.getBytes(1), kw.getByteStart(1), kw.getByteLength(1), hashCode);
      VectorAggregationBufferRow aggregationBuffer = isNull ? nullValue :
          (slot >= 0 ? values[slot] : null);
      if (aggregationBuffer == null) {
        aggregationBuffer = context.allocateAggregationBuffer();
        if (isNull) {
          assert nullValue == null;
          nullValue = aggregationBuffer;
        } else {
          put(kw.getBytes(0), kw.getByteStart(0), kw.getByteLength(0),
              kw.getBytes(1), kw.getByteStart(1), kw.getByteLength(1),
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
    VectorHashKeyWrapperTwoString keyWrapper = (VectorHashKeyWrapperTwoString) outputKey;
    int entriesFlushed = 0;
    int rowsEmitted = 0;
    // Visit the separate all-NULL group before the bucket chains.
    flushEntries:
    for (int bucket = -1; bucket < buckets.length; ++bucket) {
      if (bucket == -1 && nullValue == null) {
        continue;
      }
      int previousSlot = -1;
      int slot = bucket == -1 ? 0 : firstSlot(bucket);
      while (slot != -1) {
        int nextSlot = bucket == -1 ? -1 : nextSlot(slot);
        VectorAggregationBufferRow bufferRow = bucket == -1 ? nullValue : values[slot];
        assert bufferRow != null;
        if (!all && avgAccess >= 1 && bufferRow.getAccessCount() > avgAccess) {
          context.resetHashTableAccess(bufferRow);
          previousSlot = slot;
          slot = nextSlot;
          continue;
        }

        keyWrapper.clearIsNull();
        int keyOffset = 2 * slot;
        if (bucket == -1 || keys[keyOffset] == null) {
          keyWrapper.assignNullString(0, 0);
        } else {
          keyWrapper.assignString(0, keys[keyOffset], 0, keys[keyOffset].length);
        }
        if (bucket == -1 || keys[keyOffset + 1] == null) {
          keyWrapper.assignNullString(1, 1);
        } else {
          keyWrapper.assignString(1, keys[keyOffset + 1], 0, keys[keyOffset + 1].length);
        }
        // writeSingleRow copies key bytes into outputBatch before storage is released.
        context.writeHashTableRow(keyWrapper, bufferRow);
        ++rowsEmitted;

        if (!all) {
          context.recycleHashTableBuffer(bufferRow);
          if (bucket == -1) {
            nullValue = null;
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
    // Release the reusable wrapper's references to emitted key bytes.
    keyWrapper.assignNullString(0, 0);
    keyWrapper.assignNullString(1, 1);
    assert all || (checkInvariants() && rowsEmitted == entriesFlushed &&
        initialEntries - entriesFlushed == context.getHashTableSize() &&
        size() == context.getHashTableSize());
    assert !all || rowsEmitted == initialEntries;
    return entriesFlushed;
  }

  /**
   * Returns the matching slot, or the bitwise complement of an insertion slot.
   */
  int findSlot(byte[] key0, int start0, int length0,
      byte[] key1, int start1, int length1, int hashCode) {
    assert keys.length == 2L * values.length &&
        hashCodes.length == values.length && next.length == values.length;
    assert Integer.bitCount(buckets.length) == 1;
    assert numEntries >= 0 && numEntries <= numUsed && numUsed <= values.length &&
        numEntries <= resizeThreshold && resizeThreshold < buckets.length;
    assert (freeList == 0) == (numEntries == numUsed);
    assert hashCode == VectorHashKeyWrapperTwoString.calculateHashCode(
        (length0 == -1 ? 1 : 0) | (length1 == -1 ? 2 : 0),
        key0, start0, length0, key1, start1, length1, null);
    int bucket = hashCode & (buckets.length - 1);
    for (int ref = buckets[bucket]; ref != 0; ref = next[ref - 1]) {
      int slot = ref - 1;
      int keyOffset = 2 * slot;
      if (hashCodes[slot] == hashCode &&
          ((key0 == null && keys[keyOffset] == null) ||
          (key0 != null && keys[keyOffset] != null && StringExpr.equal(
              key0, start0, length0, keys[keyOffset], 0, keys[keyOffset].length))) &&
          ((key1 == null && keys[keyOffset + 1] == null) ||
          (key1 != null && keys[keyOffset + 1] != null && StringExpr.equal(
              key1, start1, length1, keys[keyOffset + 1], 0, keys[keyOffset + 1].length)))) {
        return slot;
      }
    }
    return ~(freeList == 0 ? numUsed : freeList - 1);
  }

  void put(byte[] key0, int start0, int length0,
      byte[] key1, int start1, int length1,
      int hashCode, int insertionSlot, VectorAggregationBufferRow value) {
    assert value != null;
    assert insertionSlot == ~findSlot(key0, start0, length0, key1, start1, length1, hashCode);
    allocateSlot(insertionSlot);
    int keyOffset = 2 * insertionSlot;
    assert keys[keyOffset] == null && keys[keyOffset + 1] == null;
    hashCodes[insertionSlot] = hashCode;
    if (length0 != -1) {
      keys[keyOffset] = Arrays.copyOfRange(key0, start0, start0 + length0);
      keyMemorySize += JAVA_DATA_MODEL.lengthForByteArrayOfSize(length0);
    }
    if (length1 != -1) {
      keys[keyOffset + 1] = Arrays.copyOfRange(key1, start1, start1 + length1);
      keyMemorySize += JAVA_DATA_MODEL.lengthForByteArrayOfSize(length1);
    }
    linkEntry(insertionSlot, hashCode, value);
    assert findSlot(key0, start0, length0, key1, start1, length1, hashCode) == insertionSlot;
    assert values[insertionSlot] == value;
  }

  private int hashCode(int slot) {
    assert values[slot] != null;
    int keyOffset = 2 * slot;
    byte[] key0 = keys[keyOffset];
    byte[] key1 = keys[keyOffset + 1];
    return VectorHashKeyWrapperTwoString.calculateHashCode(
        (key0 == null ? 1 : 0) | (key1 == null ? 2 : 0),
        key0, 0, key0 == null ? -1 : key0.length,
        key1, 0, key1 == null ? -1 : key1.length, null);
  }

  void remove(int bucket, int previousSlot, int slot) {
    assert values[slot] != null;
    assert bucket == (hashCodes[slot] & (buckets.length - 1));
    unlinkEntry(bucket, previousSlot, slot);
    int keyOffset = 2 * slot;
    byte[] key0 = keys[keyOffset];
    byte[] key1 = keys[keyOffset + 1];
    assert findSlot(key0, 0, key0 == null ? -1 : key0.length,
        key1, 0, key1 == null ? -1 : key1.length, hashCodes[slot]) == ~slot;
    if (key0 != null) {
      keyMemorySize -= JAVA_DATA_MODEL.lengthForByteArrayOfSize(key0.length);
    }
    if (key1 != null) {
      keyMemorySize -= JAVA_DATA_MODEL.lengthForByteArrayOfSize(key1.length);
    }
    keys[keyOffset] = null;
    keys[keyOffset + 1] = null;
  }

  @Override
  void clear() {
    Arrays.fill(keys, 0, 2 * numUsed, null);
    clearEntries();
    keyMemorySize = 0;
    assert checkInvariants();
  }

  @Override
  protected void growKeyStorage(int capacity) {
    keys = Arrays.copyOf(keys, Math.multiplyExact(capacity, 2));
    hashCodes = Arrays.copyOf(hashCodes, capacity);
  }

  @Override
  protected void resetKeyStorage() {
    keys = new byte[Math.multiplyExact(values.length, 2)][];
    hashCodes = new int[values.length];
    keyMemorySize = 0;
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
        + JAVA_DATA_MODEL.lengthForObjectArrayOfSize(keys.length)
        + JAVA_DATA_MODEL.lengthForIntArrayOfSize(hashCodes.length);
  }

  @Override
  long getTotalMemorySize() {
    return getBucketFixedMemorySize() + keyMemorySize;
  }

  @Override
  boolean checkInvariants() {
    if (!checkStructure() || keys.length != 2L * values.length ||
        hashCodes.length != values.length) {
      return false;
    }
    long memorySize = 0;
    for (int slot = 0; slot < values.length; ++slot) {
      int keyOffset = 2 * slot;
      byte[] key0 = keys[keyOffset];
      byte[] key1 = keys[keyOffset + 1];
      if (values[slot] != null) {
        if (key0 == null && key1 == null) {
          return false;
        }
        if (hashCodes[slot] != hashCode(slot) ||
            findSlot(key0, 0, key0 == null ? -1 : key0.length,
                key1, 0, key1 == null ? -1 : key1.length, hashCodes[slot]) != slot) {
          return false;
        }
        if (key0 != null) {
          memorySize += JAVA_DATA_MODEL.lengthForByteArrayOfSize(key0.length);
        }
        if (key1 != null) {
          memorySize += JAVA_DATA_MODEL.lengthForByteArrayOfSize(key1.length);
        }
      } else if (key0 != null || key1 != null) {
        return false;
      }
    }
    return memorySize == keyMemorySize;
  }
}
