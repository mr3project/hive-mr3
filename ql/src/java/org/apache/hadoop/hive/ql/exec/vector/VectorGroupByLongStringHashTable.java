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
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperBatch;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperSingleLongSingleString;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Array-backed, separate-chaining aggregation hash table for one long and one string grouping keys.
 */
final class VectorGroupByLongStringHashTable extends VectorGroupByHashTable {
  private long[] keys0;
  private byte[][] stringKeys;
  private byte[] nullMasks;
  // Cached wrapper hashes used for lookup and bucket selection.
  private int[] hashCodes;
  private final int longKeyIndex0;
  private final int stringKeyIndex;
  private long keyMemorySize;

  VectorGroupByLongStringHashTable(int expectedEntries, VectorHashKeyWrapperBatch keyLayout) {
    super(expectedEntries);
    longKeyIndex0 = keyLayout.longIndices[0];
    stringKeyIndex = keyLayout.stringIndices[0];
    resetKeyStorage();
    assert checkInvariants();
  }

  @Override
  int prepareBatch(VectorGroupByOperator.ProcessingModeHashAggregate context,
      VectorHashKeyWrapperBase[] keyWrappers, int n, VectorAggregationBufferBatch batchInfo)
      throws HiveException {
    int newEntries = 0;
    for (int i = 0; i < n; ++i) {
      VectorHashKeyWrapperSingleLongSingleString kw =
          (VectorHashKeyWrapperSingleLongSingleString) keyWrappers[i];
      int nullMask = (kw.isNull(0) ? 1 : 0) | (kw.isNull(1) ? 2 : 0);
      boolean isNull = nullMask == 3;
      int hashCode = isNull ? 0 : kw.hashCode();
      int slot = isNull ? 0 : findSlot(kw.getLongValue(0), nullMask,
          kw.getBytes(0), kw.getByteStart(0), kw.getByteLength(0), hashCode);
      VectorAggregationBufferRow aggregationBuffer = isNull ? nullValue :
          (slot >= 0 ? values[slot] : null);
      if (aggregationBuffer == null) {
        aggregationBuffer = context.allocateAggregationBuffer();
        if (isNull) {
          assert nullValue == null;
          nullValue = aggregationBuffer;
        } else {
          put(kw.getLongValue(0), nullMask, kw.getBytes(0), kw.getByteStart(0),
              kw.getByteLength(0), hashCode, ~slot, aggregationBuffer);
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
    VectorHashKeyWrapperSingleLongSingleString keyWrapper =
        (VectorHashKeyWrapperSingleLongSingleString) outputKey;
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
        int nullMask = bucket == -1 ? 3 : nullMasks[slot];
        if ((nullMask & (1 << longKeyIndex0)) != 0) {
          keyWrapper.assignNullLong(longKeyIndex0, 0);
        } else {
          keyWrapper.assignLong(longKeyIndex0, 0, keys0[slot]);
        }
        if ((nullMask & (1 << stringKeyIndex)) != 0) {
          keyWrapper.assignNullString(stringKeyIndex, 0);
        } else {
          byte[] bytes = stringKeys[slot];
          keyWrapper.assignString(0, bytes, 0, bytes.length);
        }
        // writeSingleRow copies the key into outputBatch before this wrapper is reused.
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
    assert all || (checkInvariants() && rowsEmitted == entriesFlushed &&
        initialEntries - entriesFlushed == context.getHashTableSize() &&
        size() == context.getHashTableSize());
    assert !all || rowsEmitted == initialEntries;
    keyWrapper.assignNullString(stringKeyIndex, 0);
    return entriesFlushed;
  }

  /**
   * Returns the matching slot, or the bitwise complement of an insertion slot.
   */
  int findSlot(long key0, int nullMask, byte[] bytes, int start, int length,
      int hashCode) {
    assert keys0.length == values.length && stringKeys.length == values.length &&
        nullMasks.length == values.length && hashCodes.length == values.length &&
        next.length == values.length;
    assert Integer.bitCount(buckets.length) == 1;
    assert numEntries >= 0 && numEntries <= numUsed && numUsed <= values.length &&
        numEntries <= resizeThreshold && resizeThreshold < buckets.length;
    assert (freeList == 0) == (numEntries == numUsed);
    assert nullMask >= 0 && nullMask < 3;
    assert ((nullMask & (1 << stringKeyIndex)) != 0) == (length == -1);
    assert hashCode == VectorHashKeyWrapperSingleLongSingleString.calculateHashCode(
        key0, nullMask, bytes, start, length);
    int bucket = hashCode & (buckets.length - 1);
    for (int ref = buckets[bucket]; ref != 0; ref = next[ref - 1]) {
      int slot = ref - 1;
      if (hashCodes[slot] == hashCode && keys0[slot] == key0 && nullMasks[slot] == nullMask &&
          (length == -1 || StringExpr.equal(bytes, start, length,
              stringKeys[slot], 0, stringKeys[slot].length))) {
        return slot;
      }
    }
    return ~(freeList == 0 ? numUsed : freeList - 1);
  }

  void put(long key0, int nullMask, byte[] bytes, int start, int length,
      int hashCode, int insertionSlot, VectorAggregationBufferRow value) {
    assert value != null;
    assert insertionSlot == ~findSlot(key0, nullMask, bytes, start, length, hashCode);
    allocateSlot(insertionSlot);
    hashCodes[insertionSlot] = hashCode;
    assert stringKeys[insertionSlot] == null;
    keys0[insertionSlot] = key0;
    nullMasks[insertionSlot] = (byte) nullMask;
    if (length != -1) {
      stringKeys[insertionSlot] = Arrays.copyOfRange(bytes, start, start + length);
      keyMemorySize += JAVA_DATA_MODEL.lengthForByteArrayOfSize(length);
    }
    linkEntry(insertionSlot, hashCode, value);
    assert findSlot(key0, nullMask, bytes, start, length, hashCode) == insertionSlot;
    assert values[insertionSlot] == value;
  }

  private int hashCode(int slot) {
    byte[] bytes = stringKeys[slot];
    return VectorHashKeyWrapperSingleLongSingleString.calculateHashCode(
        keys0[slot], nullMasks[slot], bytes, 0, bytes == null ? -1 : bytes.length);
  }

  void remove(int bucket, int previousSlot, int slot) {
    assert values[slot] != null;
    assert bucket == (hashCodes[slot] & (buckets.length - 1));
    unlinkEntry(bucket, previousSlot, slot);
    byte[] bytes = stringKeys[slot];
    assert findSlot(keys0[slot], nullMasks[slot], bytes, 0,
        bytes == null ? -1 : bytes.length, hashCodes[slot]) == ~slot;
    if (bytes != null) {
      keyMemorySize -= JAVA_DATA_MODEL.lengthForByteArrayOfSize(bytes.length);
      stringKeys[slot] = null;
    }
  }

  @Override
  void clear() {
    Arrays.fill(stringKeys, 0, numUsed, null);
    clearEntries();
    keyMemorySize = 0;
    assert checkInvariants();
  }

  @Override
  protected void growKeyStorage(int capacity) {
    keys0 = Arrays.copyOf(keys0, capacity);
    stringKeys = Arrays.copyOf(stringKeys, capacity);
    nullMasks = Arrays.copyOf(nullMasks, capacity);
    hashCodes = Arrays.copyOf(hashCodes, capacity);
  }

  @Override
  protected void resetKeyStorage() {
    keys0 = new long[values.length];
    stringKeys = new byte[values.length][];
    nullMasks = new byte[values.length];
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
        + JAVA_DATA_MODEL.lengthForLongArrayOfSize(keys0.length)
        + JAVA_DATA_MODEL.lengthForObjectArrayOfSize(stringKeys.length)
        + JAVA_DATA_MODEL.lengthForByteArrayOfSize(nullMasks.length)
        + JAVA_DATA_MODEL.lengthForIntArrayOfSize(hashCodes.length);
  }

  @Override
  long getTotalMemorySize() {
    return getBucketFixedMemorySize() + keyMemorySize;
  }

  @Override
  boolean checkInvariants() {
    if (!checkStructure() || keys0.length != values.length ||
        stringKeys.length != values.length || nullMasks.length != values.length ||
        hashCodes.length != values.length) {
      return false;
    }
    long memory = 0;
    for (int slot = 0; slot < values.length; ++slot) {
      byte[] bytes = stringKeys[slot];
      if (values[slot] == null) {
        if (bytes != null) {
          return false;
        }
      } else {
        if (nullMasks[slot] < 0 || nullMasks[slot] >= 3 ||
            (((nullMasks[slot] & (1 << stringKeyIndex)) != 0) != (bytes == null))) {
          return false;
        }
        if ((nullMasks[slot] & (1 << longKeyIndex0)) != 0 && keys0[slot] != 0) {
          return false;
        }
        if (hashCodes[slot] != hashCode(slot) ||
            findSlot(keys0[slot], nullMasks[slot], bytes, 0,
                bytes == null ? -1 : bytes.length, hashCodes[slot]) != slot) {
          return false;
        }
        if (bytes != null) {
          memory += JAVA_DATA_MODEL.lengthForByteArrayOfSize(bytes.length);
        }
      }
    }
    return keyMemorySize == memory;
  }
}
