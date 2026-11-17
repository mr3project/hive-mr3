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
  // Exactly one of keys and keyChunks stores the slot keys.
  private long[] keys;
  private long[][] keyChunks;
  // Zero for flat storage; otherwise the fixed number of slots in each chunk.
  private int chunkSize;
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
    assert next.length == values.length;
    assert keys != null ? keyChunks == null && chunkSize == 0 && keys.length == values.length :
        keyChunks != null && chunkSize > 0 && (long) keyChunks.length * chunkSize == values.length;
    assert Integer.bitCount(buckets.length) == 1;
    assert numEntries >= 0 && numEntries <= numUsed && numUsed <= values.length &&
        numEntries <= resizeThreshold && resizeThreshold < buckets.length;
    assert (freeList == 0) == (numEntries == numUsed);
    assert hashCode == HashCodeUtil.calculateLongHashCode(key);
    int bucket = (hashCode ^ (hashCode >>> 16)) & (buckets.length - 1);
    if (keys != null) {
      for (int ref = buckets[bucket]; ref != 0; ref = next[ref - 1]) {
        int slot = ref - 1;
        if (keys[slot] == key) {
          return slot;
        }
      }
    } else {
      for (int ref = buckets[bucket]; ref != 0; ref = next[ref - 1]) {
        int slot = ref - 1;
        if (keyChunks[slot / chunkSize][slot % chunkSize] == key) {
          return slot;
        }
      }
    }
    return ~(freeList == 0 ? numUsed : freeList - 1);
  }

  void put(long key, int hashCode, int insertionSlot, VectorAggregationBufferRow value) {
    assert value != null;
    assert insertionSlot == ~findSlot(key, hashCode);
    allocateSlot(insertionSlot);
    if (keys != null) {
      keys[insertionSlot] = key;
    } else {
      keyChunks[insertionSlot / chunkSize][insertionSlot % chunkSize] = key;
    }
    linkEntryWithSpreadHash(insertionSlot, spreadHash(hashCode), value);
    assert findSlot(key, hashCode) == insertionSlot;
    assert getValue(insertionSlot) == value;
  }

  long getKey(int slot) {
    assert values[slot] != null;
    return keyAt(slot);
  }

  private long keyAt(int slot) {
    return keys != null ? keys[slot] : keyChunks[slot / chunkSize][slot % chunkSize];
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
    assert bucket == (spreadHash(HashCodeUtil.calculateLongHashCode(keyAt(slot))) &
        (buckets.length - 1));
    unlinkEntry(bucket, previousSlot, slot);
    assert findSlot(keyAt(slot), HashCodeUtil.calculateLongHashCode(keyAt(slot))) == ~slot;
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
    if ((long) capacity * Long.BYTES <= MAX_COMPONENT_CHUNK_BYTES) {
      assert keyChunks == null && chunkSize == 0;
      keys = Arrays.copyOf(keys, capacity);
    } else if (keys != null) {
      chunkSize = keys.length;
      assert chunkSize == componentChunkSize(capacity) && capacity == 2L * chunkSize;
      keyChunks = new long[][] {keys, new long[chunkSize]};
      keys = null;
    } else {
      assert chunkSize == componentChunkSize(capacity) &&
          capacity == 2L * keyChunks.length * chunkSize;
      int oldChunkCount = keyChunks.length;
      keyChunks = Arrays.copyOf(keyChunks, capacity / chunkSize);
      for (int chunk = oldChunkCount; chunk < keyChunks.length; ++chunk) {
        keyChunks[chunk] = new long[chunkSize];
      }
    }
  }

  @Override
  protected void resetKeyStorage() {
    if ((long) values.length * Long.BYTES <= MAX_COMPONENT_CHUNK_BYTES) {
      keys = new long[values.length];
      keyChunks = null;
      chunkSize = 0;
    } else {
      keys = null;
      chunkSize = componentChunkSize(values.length);
      keyChunks = new long[values.length / chunkSize][];
      for (int chunk = 0; chunk < keyChunks.length; ++chunk) {
        keyChunks[chunk] = new long[chunkSize];
      }
    }
    nullValue = null;
  }

  @Override
  protected void rehash() {
    int mask = buckets.length - 1;
    int copiedEntries = 0;
    for (int slot = 0; slot < numUsed; ++slot) {
      if (values[slot] != null) {
        int hashCode = HashCodeUtil.calculateLongHashCode(keyAt(slot));
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
  @Override
  boolean checkInvariants() {
    if (!checkStructure() || !checkKeyStorage()) {
      return false;
    }
    for (int slot = 0; slot < numUsed; ++slot) {
      if (values[slot] != null &&
          findSlot(keyAt(slot), HashCodeUtil.calculateLongHashCode(keyAt(slot))) != slot) {
        return false;
      }
    }
    return true;
  }

  private boolean checkKeyStorage() {
    if (keys != null) {
      return keyChunks == null && chunkSize == 0 && keys.length == values.length &&
          (long) keys.length * Long.BYTES <= MAX_COMPONENT_CHUNK_BYTES;
    }
    if (keyChunks == null || chunkSize != componentChunkSize(values.length) ||
        (long) keyChunks.length * chunkSize != values.length ||
        (long) values.length * Long.BYTES <= MAX_COMPONENT_CHUNK_BYTES) {
      return false;
    }
    for (long[] chunk : keyChunks) {
      if (chunk == null || chunk.length != chunkSize ||
          (long) chunk.length * Long.BYTES > MAX_COMPONENT_CHUNK_BYTES) {
        return false;
      }
    }
    return true;
  }

  @Override
  long getEstimatedFixedMemorySize() {
    long memory = getEstimatedArrayMemorySize();
    if (keys != null) {
      return memory + JAVA_DATA_MODEL.lengthForLongArrayOfSize(keys.length);
    }
    memory += JAVA_DATA_MODEL.lengthForObjectArrayOfSize(keyChunks.length);
    for (long[] chunk : keyChunks) {
      memory += JAVA_DATA_MODEL.lengthForLongArrayOfSize(chunk.length);
    }
    return memory;
  }
}
