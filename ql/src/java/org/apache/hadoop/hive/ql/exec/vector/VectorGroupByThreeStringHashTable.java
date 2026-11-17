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
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperThreeString;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Array-backed, separate-chaining aggregation hash table for three string grouping keys.
 */
final class VectorGroupByThreeStringHashTable extends VectorGroupByHashTable {
  // Each slot stores its three string references consecutively.
  private byte[][][] keys;
  // Cached spread hashes used for lookup and bucket selection.
  private int[][] hashCodes;
  private VectorAggregationBufferRow nullValue;
  private long keyMemorySize;

  VectorGroupByThreeStringHashTable(int expectedEntries) {
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
      VectorHashKeyWrapperThreeString kw = (VectorHashKeyWrapperThreeString) keyWrappers[i];
      boolean isNull = kw.isNull(0) && kw.isNull(1) && kw.isNull(2);
      int hashCode = isNull ? 0 : spreadHash(kw.hashCode());
      int slot = isNull ? 0 : findSlot(kw.getBytes(0), kw.getByteStart(0), kw.getByteLength(0),
          kw.getBytes(1), kw.getByteStart(1), kw.getByteLength(1),
          kw.getBytes(2), kw.getByteStart(2), kw.getByteLength(2), hashCode);
      VectorAggregationBufferRow aggregationBuffer = isNull ? nullValue :
          (slot >= 0 ? getValue(slot) : null);
      if (aggregationBuffer == null) {
        aggregationBuffer = context.allocateAggregationBuffer();
        if (isNull) {
          assert nullValue == null;
          nullValue = aggregationBuffer;
        } else {
          put(kw.getBytes(0), kw.getByteStart(0), kw.getByteLength(0),
              kw.getBytes(1), kw.getByteStart(1), kw.getByteLength(1),
              kw.getBytes(2), kw.getByteStart(2), kw.getByteLength(2),
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
    VectorHashKeyWrapperThreeString keyWrapper = (VectorHashKeyWrapperThreeString) outputKey;
    int entriesFlushed = 0;
    int rowsEmitted = 0;
    // Visit the separate all-NULL group before the bucket chains.
    flushEntries:
    for (int bucket = -1; bucket < bucketCount(); ++bucket) {
      if (bucket == -1 && nullValue == null) {
        continue;
      }
      int previousSlot = -1;
      int slot = bucket == -1 ? 0 : firstSlot(bucket);
      while (slot != -1) {
        int nextSlot = bucket == -1 ? -1 : nextSlot(slot);
        VectorAggregationBufferRow bufferRow = bucket == -1 ? nullValue : getValue(slot);
        assert bufferRow != null;
        if (!all && avgAccess >= 1 && bufferRow.getAccessCount() > avgAccess) {
          context.resetHashTableAccess(bufferRow);
          previousSlot = slot;
          slot = nextSlot;
          continue;
        }

        keyWrapper.clearIsNull();
        int segment = entrySegment(slot);
        int offset = entryOffset(slot, segment);
        int keyOffset = 3 * offset;
        byte[][] segmentKeys = keys[segment];
        if (bucket == -1 || segmentKeys[keyOffset] == null) {
          keyWrapper.assignNullString(0, 0);
        } else {
          keyWrapper.assignString(0, segmentKeys[keyOffset], 0, segmentKeys[keyOffset].length);
        }
        if (bucket == -1 || segmentKeys[keyOffset + 1] == null) {
          keyWrapper.assignNullString(1, 1);
        } else {
          keyWrapper.assignString(1, segmentKeys[keyOffset + 1], 0, segmentKeys[keyOffset + 1].length);
        }
        if (bucket == -1 || segmentKeys[keyOffset + 2] == null) {
          keyWrapper.assignNullString(2, 2);
        } else {
          keyWrapper.assignString(2, segmentKeys[keyOffset + 2], 0, segmentKeys[keyOffset + 2].length);
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
    keyWrapper.assignNullString(2, 2);
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
  int findSlot(byte[] key0, int start0, int length0,
      byte[] key1, int start1, int length1,
      byte[] key2, int start2, int length2, int hashCode) {
    assert keys.length == values.length &&
        hashCodes.length == values.length && next.length == values.length;
    assert Integer.bitCount(bucketCapacity) == 1;
    assert numEntries >= 0 && numEntries <= numUsed && numUsed <= entryCapacity &&
        numEntries <= resizeThreshold && resizeThreshold < bucketCapacity;
    assert (freeList == 0) == (numEntries == numUsed);
    assert hashCode == spreadHash(VectorHashKeyWrapperThreeString.calculateHashCode(
        (length0 == -1 ? 1 : 0) | (length1 == -1 ? 2 : 0) | (length2 == -1 ? 4 : 0),
        key0, start0, length0, key1, start1, length1, key2, start2, length2, null));
    int bucket = hashCode & (bucketCapacity - 1);
    for (int ref = bucketHead(bucket); ref != 0;) {
      int slot = ref - 1;
      int segment = entrySegment(slot);
      int offset = entryOffset(slot, segment);
      int keyOffset = 3 * offset;
      byte[][] segmentKeys = keys[segment];
      if (hashCodes[segment][offset] == hashCode &&
          ((key0 == null && segmentKeys[keyOffset] == null) ||
          (key0 != null && segmentKeys[keyOffset] != null && StringExpr.equal(
              key0, start0, length0, segmentKeys[keyOffset], 0, segmentKeys[keyOffset].length))) &&
          ((key1 == null && segmentKeys[keyOffset + 1] == null) ||
          (key1 != null && segmentKeys[keyOffset + 1] != null && StringExpr.equal(
              key1, start1, length1, segmentKeys[keyOffset + 1], 0, segmentKeys[keyOffset + 1].length))) &&
          ((key2 == null && segmentKeys[keyOffset + 2] == null) ||
          (key2 != null && segmentKeys[keyOffset + 2] != null && StringExpr.equal(
              key2, start2, length2, segmentKeys[keyOffset + 2], 0, segmentKeys[keyOffset + 2].length)))) {
        return slot;
      }
      ref = next[segment][offset];
    }
    return ~(freeList == 0 ? numUsed : freeList - 1);
  }

  void put(byte[] key0, int start0, int length0,
      byte[] key1, int start1, int length1,
      byte[] key2, int start2, int length2,
      int hashCode, int insertionSlot, VectorAggregationBufferRow value) {
    assert value != null;
    assert insertionSlot == ~findSlot(key0, start0, length0, key1, start1, length1,
        key2, start2, length2, hashCode);
    allocateSlot(insertionSlot);
    int segment = entrySegment(insertionSlot);
    int offset = entryOffset(insertionSlot, segment);
    int keyOffset = 3 * offset;
    byte[][] segmentKeys = keys[segment];
    assert segmentKeys[keyOffset] == null && segmentKeys[keyOffset + 1] == null &&
        segmentKeys[keyOffset + 2] == null;
    hashCodes[segment][offset] = hashCode;
    if (length0 != -1) {
      segmentKeys[keyOffset] = Arrays.copyOfRange(key0, start0, start0 + length0);
      keyMemorySize += JAVA_DATA_MODEL.lengthForByteArrayOfSize(length0);
    }
    if (length1 != -1) {
      segmentKeys[keyOffset + 1] = Arrays.copyOfRange(key1, start1, start1 + length1);
      keyMemorySize += JAVA_DATA_MODEL.lengthForByteArrayOfSize(length1);
    }
    if (length2 != -1) {
      segmentKeys[keyOffset + 2] = Arrays.copyOfRange(key2, start2, start2 + length2);
      keyMemorySize += JAVA_DATA_MODEL.lengthForByteArrayOfSize(length2);
    }
    linkEntryWithSpreadHash(insertionSlot, hashCode, value);
    assert findSlot(key0, start0, length0, key1, start1, length1,
        key2, start2, length2, hashCode) == insertionSlot;
    assert getValue(insertionSlot) == value;
  }

  private int hashCode(int slot) {
    int segment = entrySegment(slot);
    int offset = entryOffset(slot, segment);
    assert values[segment][offset] != null;
    int keyOffset = 3 * offset;
    byte[][] segmentKeys = keys[segment];
    byte[] key0 = segmentKeys[keyOffset];
    byte[] key1 = segmentKeys[keyOffset + 1];
    byte[] key2 = segmentKeys[keyOffset + 2];
    return VectorHashKeyWrapperThreeString.calculateHashCode(
        (key0 == null ? 1 : 0) | (key1 == null ? 2 : 0) | (key2 == null ? 4 : 0),
        key0, 0, key0 == null ? -1 : key0.length,
        key1, 0, key1 == null ? -1 : key1.length,
        key2, 0, key2 == null ? -1 : key2.length, null);
  }

  void remove(int bucket, int previousSlot, int slot) {
    int segment = entrySegment(slot);
    int offset = entryOffset(slot, segment);
    assert values[segment][offset] != null;
    assert bucket == (hashCodes[segment][offset] & (bucketCapacity - 1));
    unlinkEntry(bucket, previousSlot, slot);
    int keyOffset = 3 * offset;
    byte[][] segmentKeys = keys[segment];
    byte[] key0 = segmentKeys[keyOffset];
    byte[] key1 = segmentKeys[keyOffset + 1];
    byte[] key2 = segmentKeys[keyOffset + 2];
    assert findSlot(key0, 0, key0 == null ? -1 : key0.length,
        key1, 0, key1 == null ? -1 : key1.length,
        key2, 0, key2 == null ? -1 : key2.length, hashCodes[segment][offset]) == ~slot;
    if (key0 != null) {
      keyMemorySize -= JAVA_DATA_MODEL.lengthForByteArrayOfSize(key0.length);
    }
    if (key1 != null) {
      keyMemorySize -= JAVA_DATA_MODEL.lengthForByteArrayOfSize(key1.length);
    }
    if (key2 != null) {
      keyMemorySize -= JAVA_DATA_MODEL.lengthForByteArrayOfSize(key2.length);
    }
    segmentKeys[keyOffset] = null;
    segmentKeys[keyOffset + 1] = null;
    segmentKeys[keyOffset + 2] = null;
  }

  @Override
  int size() {
    return numEntries + (nullValue == null ? 0 : 1);
  }

  @Override
  void clear() {
    int remaining = numUsed;
    for (int segment = 0; segment < keys.length && remaining > 0; ++segment) {
      int used = Math.min(remaining, values[segment].length);
      Arrays.fill(keys[segment], 0, 3 * used, null);
      remaining -= used;
    }
    clearEntries();
    nullValue = null;
    keyMemorySize = 0;
    assert checkInvariants();
  }

  @Override
  protected void growKeyStorage(int segment, int capacity) {
    keys = Arrays.copyOf(keys, segment + 1);
    hashCodes = Arrays.copyOf(hashCodes, segment + 1);
    keys[segment] = new byte[Math.multiplyExact(capacity, 3)][];
    hashCodes[segment] = new int[capacity];
  }

  @Override
  protected void resetKeyStorage() {
    keys = new byte[values.length][][];
    hashCodes = new int[values.length][];
    for (int segment = 0; segment < values.length; ++segment) {
      int capacity = values[segment].length;
      keys[segment] = new byte[Math.multiplyExact(capacity, 3)][];
      hashCodes[segment] = new int[capacity];
    }
    nullValue = null;
    keyMemorySize = 0;
  }

  @Override
  protected void rehash() {
    int mask = bucketCapacity - 1;
    int copiedEntries = 0;
    for (int slot = 0; slot < numUsed; ++slot) {
      int segment = entrySegment(slot);
      int offset = entryOffset(slot, segment);
      if (values[segment][offset] != null) {
        int bucket = hashCodes[segment][offset] & mask;
        next[segment][offset] = bucketHead(bucket);
        setBucketHead(bucket, slot + 1);
        ++copiedEntries;
      }
    }
    assert copiedEntries == numEntries;
    assert checkInvariants();
  }

  @Override
  boolean checkInvariants() {
    if (!checkStructure() || keys.length != values.length ||
        hashCodes.length != values.length) {
      return false;
    }
    for (int segment = 0; segment < values.length; ++segment) {
      if (keys[segment].length != 3L * values[segment].length ||
          hashCodes[segment].length != values[segment].length) {
        return false;
      }
    }
    long memorySize = 0;
    for (int slot = 0; slot < entryCapacity; ++slot) {
      int segment = entrySegment(slot);
      int offset = entryOffset(slot, segment);
      int keyOffset = 3 * offset;
      byte[][] segmentKeys = keys[segment];
      byte[] key0 = segmentKeys[keyOffset];
      byte[] key1 = segmentKeys[keyOffset + 1];
      byte[] key2 = segmentKeys[keyOffset + 2];
      if (values[segment][offset] != null) {
        if (key0 == null && key1 == null && key2 == null) {
          return false;
        }
        if (hashCodes[segment][offset] != spreadHash(hashCode(slot)) ||
            findSlot(key0, 0, key0 == null ? -1 : key0.length,
                key1, 0, key1 == null ? -1 : key1.length,
                key2, 0, key2 == null ? -1 : key2.length, hashCodes[segment][offset]) != slot) {
          return false;
        }
        if (key0 != null) {
          memorySize += JAVA_DATA_MODEL.lengthForByteArrayOfSize(key0.length);
        }
        if (key1 != null) {
          memorySize += JAVA_DATA_MODEL.lengthForByteArrayOfSize(key1.length);
        }
        if (key2 != null) {
          memorySize += JAVA_DATA_MODEL.lengthForByteArrayOfSize(key2.length);
        }
      } else if (key0 != null || key1 != null || key2 != null) {
        return false;
      }
    }
    return memorySize == keyMemorySize;
  }

  @Override
  long getEstimatedFixedMemorySize() {
    long memorySize = getEstimatedArrayMemorySize()
        + JAVA_DATA_MODEL.lengthForObjectArrayOfSize(keys.length)
        + JAVA_DATA_MODEL.lengthForObjectArrayOfSize(hashCodes.length);
    for (int segment = 0; segment < values.length; ++segment) {
      memorySize += JAVA_DATA_MODEL.lengthForObjectArrayOfSize(keys[segment].length)
          + JAVA_DATA_MODEL.lengthForIntArrayOfSize(hashCodes[segment].length);
    }
    return memorySize;
  }

  @Override
  long getEstimatedMemorySize() {
    return getEstimatedFixedMemorySize() + keyMemorySize;
  }
}
