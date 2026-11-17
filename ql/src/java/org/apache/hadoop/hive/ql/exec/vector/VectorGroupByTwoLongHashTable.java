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
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperTwoLong;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Array-backed, separate-chaining aggregation hash table for two long grouping keys.
 */
final class VectorGroupByTwoLongHashTable extends VectorGroupByHashTable {
  // Stores consecutive pairs until the combined payload would exceed the chunk limit.
  private long[] keys;
  // Each chunk stores consecutive pairs, with the same slot order as the flat array.
  private long[][] keysChunks;
  // Zero for flat storage; otherwise the fixed number of key pairs in each chunk.
  private int slotsPerChunk;
  private byte[] nullMasks;
  // Cached spread hashes used for lookup and bucket selection.
  private int[] hashCodes;
  private VectorAggregationBufferRow nullValue;

  VectorGroupByTwoLongHashTable(int expectedEntries) {
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
      VectorHashKeyWrapperTwoLong kw = (VectorHashKeyWrapperTwoLong) keyWrappers[i];
      int nullMask = (kw.isNull(0) ? 1 : 0) | (kw.isNull(1) ? 2 : 0);
      boolean isNull = nullMask == 3;
      int hashCode = isNull ? 0 : spreadHash(kw.hashCode());
      int slot = isNull ? 0 : findSlot(kw.getLongValue(0), kw.getLongValue(1), nullMask, hashCode);
      VectorAggregationBufferRow aggregationBuffer = isNull ? getNullValue() :
          (slot >= 0 ? getValue(slot) : null);
      if (aggregationBuffer == null) {
        aggregationBuffer = context.allocateAggregationBuffer();
        if (isNull) {
          setNullValue(aggregationBuffer);
        } else {
          put(kw.getLongValue(0), kw.getLongValue(1), nullMask, hashCode, ~slot, aggregationBuffer);
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
    VectorHashKeyWrapperTwoLong keyWrapper =
        (VectorHashKeyWrapperTwoLong) outputKey;
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
          keyWrapper.assignNullLong(1, 1);
        } else {
          long[] slotKeys;
          int keyOffset;
          if (keys != null) {
            slotKeys = keys;
            keyOffset = 2 * slot;
          } else {
            int chunk = slot / slotsPerChunk;
            slotKeys = keysChunks[chunk];
            keyOffset = 2 * (slot - chunk * slotsPerChunk);
          }
          if ((nullMasks[slot] & 1) != 0) {
            keyWrapper.assignNullLong(0, 0);
          } else {
            keyWrapper.assignLong(0, 0, slotKeys[keyOffset]);
          }
          if ((nullMasks[slot] & 2) != 0) {
            keyWrapper.assignNullLong(1, 1);
          } else {
            keyWrapper.assignLong(1, 1, slotKeys[keyOffset + 1]);
          }
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
   * The supplied hash code has already been spread for bucket selection.
   */
  int findSlot(long key0, long key1, int nullMask, int hashCode) {
    assert checkKeyStorage() &&
        nullMasks.length == values.length && next.length == values.length &&
        hashCodes.length == values.length;
    assert Integer.bitCount(buckets.length) == 1;
    assert numEntries >= 0 && numEntries <= numUsed && numUsed <= values.length &&
        numEntries <= resizeThreshold && resizeThreshold < buckets.length;
    assert (freeList == 0) == (numEntries == numUsed);
    assert nullMask >= 0 && nullMask < 3;
    assert hashCode == spreadHash(VectorHashKeyWrapperTwoLong.calculateHashCode(key0, key1, nullMask));
    int bucket = hashCode & (buckets.length - 1);
    if (keys != null) {
      for (int ref = buckets[bucket]; ref != 0; ref = next[ref - 1]) {
        int slot = ref - 1;
        int keyOffset = 2 * slot;
        if (hashCodes[slot] == hashCode && nullMasks[slot] == nullMask &&
            keys[keyOffset] == key0 && keys[keyOffset + 1] == key1) {
          return slot;
        }
      }
    } else {
      for (int ref = buckets[bucket]; ref != 0; ref = next[ref - 1]) {
        int slot = ref - 1;
        if (hashCodes[slot] == hashCode && nullMasks[slot] == nullMask) {
          int chunk = slot / slotsPerChunk;
          int offset = 2 * (slot - chunk * slotsPerChunk);
          long[] chunkKeys = keysChunks[chunk];
          if (chunkKeys[offset] == key0 && chunkKeys[offset + 1] == key1) {
            return slot;
          }
        }
      }
    }
    return ~(freeList == 0 ? numUsed : freeList - 1);
  }

  void put(long key0, long key1, int nullMask, int hashCode, int insertionSlot,
      VectorAggregationBufferRow value) {
    assert value != null;
    assert insertionSlot == ~findSlot(key0, key1, nullMask, hashCode);
    allocateSlot(insertionSlot);
    if (keys != null) {
      int keyOffset = 2 * insertionSlot;
      keys[keyOffset] = key0;
      keys[keyOffset + 1] = key1;
    } else {
      int chunk = insertionSlot / slotsPerChunk;
      int offset = 2 * (insertionSlot - chunk * slotsPerChunk);
      long[] chunkKeys = keysChunks[chunk];
      chunkKeys[offset] = key0;
      chunkKeys[offset + 1] = key1;
    }
    nullMasks[insertionSlot] = (byte) nullMask;
    hashCodes[insertionSlot] = hashCode;
    linkEntryWithSpreadHash(insertionSlot, hashCode, value);
    assert findSlot(key0, key1, nullMask, hashCode) == insertionSlot;
    assert getValue(insertionSlot) == value;
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

  private long getKey0(int slot) {
    return keys != null ? keys[2 * slot] :
        keysChunks[slot / slotsPerChunk][2 * (slot % slotsPerChunk)];
  }

  private long getKey1(int slot) {
    return keys != null ? keys[2 * slot + 1] :
        keysChunks[slot / slotsPerChunk][2 * (slot % slotsPerChunk) + 1];
  }

  private int calculateHashCode(int slot) {
    return VectorHashKeyWrapperTwoLong.calculateHashCode(getKey0(slot), getKey1(slot), nullMasks[slot]);
  }

  void remove(int bucket, int previousSlot, int slot) {
    assert values[slot] != null;
    assert bucket == (hashCodes[slot] & (buckets.length - 1));
    unlinkEntry(bucket, previousSlot, slot);
    assert findSlot(getKey0(slot), getKey1(slot), nullMasks[slot], hashCodes[slot]) == ~slot;
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
    assert capacity == values.length && capacity == 2L * nullMasks.length;
    if (2L * capacity * Long.BYTES <= MAX_COMPONENT_CHUNK_BYTES) {
      assert keys != null && keysChunks == null && slotsPerChunk == 0;
      keys = Arrays.copyOf(keys, Math.multiplyExact(capacity, 2));
    } else if (keys != null) {
      assert keys.length == capacity && numUsed == capacity / 2 &&
          numEntries == numUsed && freeList == 0;
      slotsPerChunk = componentChunkSize(capacity, 2);
      assert slotsPerChunk == capacity / 2 && keys.length == 2L * slotsPerChunk;
      // The existing array already contains the first chunk's consecutive pairs.
      keysChunks = new long[][] {keys, new long[keys.length]};
      keys = null;
    } else {
      int oldChunkCount = keysChunks.length;
      assert capacity == 2L * oldChunkCount * slotsPerChunk;
      int chunkCount = capacity / slotsPerChunk;
      keysChunks = Arrays.copyOf(keysChunks, chunkCount);
      for (int chunk = oldChunkCount; chunk < chunkCount; ++chunk) {
        keysChunks[chunk] = new long[2 * slotsPerChunk];
      }
    }
    nullMasks = Arrays.copyOf(nullMasks, capacity);
    hashCodes = Arrays.copyOf(hashCodes, capacity);
    assert checkKeyStorage();
  }

  @Override
  protected void resetKeyStorage() {
    int capacity = values.length;
    if (2L * capacity * Long.BYTES <= MAX_COMPONENT_CHUNK_BYTES) {
      keys = new long[Math.multiplyExact(capacity, 2)];
      keysChunks = null;
      slotsPerChunk = 0;
    } else {
      keys = null;
      slotsPerChunk = componentChunkSize(capacity, 2);
      int chunkCount = capacity / slotsPerChunk;
      keysChunks = new long[chunkCount][];
      for (int chunk = 0; chunk < chunkCount; ++chunk) {
        keysChunks[chunk] = new long[2 * slotsPerChunk];
      }
    }
    nullMasks = new byte[values.length];
    hashCodes = new int[values.length];
    nullValue = null;
    assert checkKeyStorage();
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

  private boolean checkKeyStorage() {
    if (2L * values.length * Long.BYTES <= MAX_COMPONENT_CHUNK_BYTES) {
      return keys != null && keys.length == 2L * values.length &&
          keysChunks == null && slotsPerChunk == 0;
    }
    return keys == null && keysChunks != null && slotsPerChunk > 0 &&
        2L * slotsPerChunk * Long.BYTES <= MAX_COMPONENT_CHUNK_BYTES &&
        (long) keysChunks.length * slotsPerChunk == values.length;
  }

  /**
   * Checks bucket-chain reachability, entry counts, and free-list membership.
   */
  @Override
  boolean checkInvariants() {
    if (!checkStructure() || !checkKeyStorage() ||
        nullMasks.length != values.length || hashCodes.length != values.length) {
      return false;
    }
    if (keys == null) {
      if (slotsPerChunk != componentChunkSize(values.length, 2)) {
        return false;
      }
      for (long[] chunk : keysChunks) {
        if (chunk == null || chunk.length != 2L * slotsPerChunk) {
          return false;
        }
      }
    }
    for (int slot = 0; slot < numUsed; ++slot) {
      if (values[slot] != null &&
          (hashCodes[slot] != spreadHash(calculateHashCode(slot)) ||
              findSlot(getKey0(slot), getKey1(slot), nullMasks[slot], hashCodes[slot]) != slot)) {
        return false;
      }
    }
    return true;
  }

  @Override
  long getEstimatedFixedMemorySize() {
    long memory = getEstimatedArrayMemorySize()
        + JAVA_DATA_MODEL.lengthForByteArrayOfSize(nullMasks.length)
        + JAVA_DATA_MODEL.lengthForIntArrayOfSize(hashCodes.length);
    if (keys != null) {
      return memory + JAVA_DATA_MODEL.lengthForLongArrayOfSize(keys.length);
    }
    memory += JAVA_DATA_MODEL.lengthForObjectArrayOfSize(keysChunks.length);
    for (long[] chunk : keysChunks) {
      memory += JAVA_DATA_MODEL.lengthForLongArrayOfSize(chunk.length);
    }
    return memory;
  }
}
