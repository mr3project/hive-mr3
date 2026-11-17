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
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperGeneralLongString;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Array-backed, separate-chaining aggregation hash table for general long/string grouping keys.
 */
final class VectorGroupByGeneralLongStringHashTable extends VectorGroupByHashTable {
  private final int longCount;
  private final int stringCount;
  private final int keyCount;
  private final int nullMaskBytes;
  private final int[] longKeyIndices;
  private final int[] stringKeyIndices;
  // Scratch storage for the current input key, reused for each row.
  private final byte[] lookupNullMask;

  // Components belonging to a slot are stored consecutively.
  private long[][] longKeys;
  private byte[][][] stringKeys;
  private byte[][] nullMasks;
  // Cached spread hashes used for lookup and bucket selection.
  private int[][] hashCodes;
  private VectorAggregationBufferRow nullValue;
  private long keyMemorySize;

  VectorGroupByGeneralLongStringHashTable(int expectedEntries,
      VectorHashKeyWrapperBatch keyLayout) {
    super(expectedEntries);
    longKeyIndices = keyLayout.longIndices.clone();
    stringKeyIndices = keyLayout.stringIndices.clone();
    longCount = longKeyIndices.length;
    stringCount = stringKeyIndices.length;
    keyCount = keyLayout.keyCount;
    nullMaskBytes = (keyCount + 7) >>> 3;
    lookupNullMask = new byte[nullMaskBytes];
    assert keyCount == longCount + stringCount;
    resetKeyStorage();
    assert checkInvariants();
  }

  private static boolean isNull(byte[] masks, int offset, int keyIndex) {
    return (masks[offset + (keyIndex >>> 3)] & (1 << (keyIndex & 7))) != 0;
  }

  private boolean buildNullMask(VectorHashKeyWrapperBase key) {
    Arrays.fill(lookupNullMask, (byte) 0);
    boolean allNull = true;
    for (int i = 0; i < keyCount; ++i) {
      if (key.isNull(i)) {
        lookupNullMask[i >>> 3] |= (byte) (1 << (i & 7));
      } else {
        allNull = false;
      }
    }
    return allNull;
  }

  @Override
  int prepareBatch(VectorGroupByOperator.ProcessingModeHashAggregate context,
      VectorHashKeyWrapperBase[] keyWrappers, int n, VectorAggregationBufferBatch batchInfo)
      throws HiveException {
    int newEntries = 0;
    for (int i = 0; i < n; ++i) {
      VectorHashKeyWrapperBase key = keyWrappers[i];
      boolean allNull = buildNullMask(key);
      int hashCode = allNull ? 0 : spreadHash(key.hashCode());
      int slot = allNull ? 0 : findSlot(key, lookupNullMask, hashCode);
      VectorAggregationBufferRow aggregationBuffer = allNull ? nullValue :
          (slot >= 0 ? getValue(slot) : null);
      if (aggregationBuffer == null) {
        aggregationBuffer = context.allocateAggregationBuffer();
        if (allNull) {
          assert nullValue == null;
          nullValue = aggregationBuffer;
        } else {
          put(key, lookupNullMask, hashCode, ~slot, aggregationBuffer);
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
        outputKey.clearIsNull();
        int segment = entrySegment(slot);
        int offset = entryOffset(slot, segment);
        int maskOffset = offset * nullMaskBytes;
        int longOffset = offset * longCount;
        int stringOffset = offset * stringCount;
        for (int i = 0; i < longCount; ++i) {
          int keyIndex = longKeyIndices[i];
          if (bucket == -1 || isNull(nullMasks[segment], maskOffset, keyIndex)) {
            outputKey.assignNullLong(keyIndex, i);
          } else {
            outputKey.assignLong(keyIndex, i, longKeys[segment][longOffset + i]);
          }
        }
        for (int i = 0; i < stringCount; ++i) {
          int keyIndex = stringKeyIndices[i];
          if (bucket == -1 || isNull(nullMasks[segment], maskOffset, keyIndex)) {
            outputKey.assignNullString(keyIndex, i);
          } else {
            byte[] bytes = stringKeys[segment][stringOffset + i];
            outputKey.assignString(i, bytes, 0, bytes.length);
          }
        }
        // writeSingleRow copies the key bytes before owned storage is released.
        context.writeHashTableRow(outputKey, bufferRow);
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
    // Drop references held by the reusable output wrapper.
    for (int i = 0; i < stringCount; ++i) {
      outputKey.assignNullString(stringKeyIndices[i], i);
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
  private int findSlot(VectorHashKeyWrapperBase key, byte[] mask, int hashCode) {
    assert longKeys.length == values.length && stringKeys.length == values.length &&
        nullMasks.length == values.length &&
        hashCodes.length == values.length && next.length == values.length;
    assert Integer.bitCount(bucketCapacity) == 1;
    assert numEntries >= 0 && numEntries <= numUsed && numUsed <= entryCapacity &&
        numEntries <= resizeThreshold && resizeThreshold < bucketCapacity;
    assert (freeList == 0) == (numEntries == numUsed);
    assert hashCode == spreadHash(key.hashCode());
    int bucket = hashCode & (bucketCapacity - 1);
    for (int ref = bucketHead(bucket); ref != 0;) {
      int slot = ref - 1;
      int segment = entrySegment(slot);
      int offset = entryOffset(slot, segment);
      ref = next[segment][offset];
      if (hashCodes[segment][offset] != hashCode) {
        continue;
      }
      int maskOffset = offset * nullMaskBytes;
      boolean equal = true;
      for (int i = 0; i < nullMaskBytes; ++i) {
        if (nullMasks[segment][maskOffset + i] != mask[i]) {
          equal = false;
          break;
        }
      }
      if (!equal) {
        continue;
      }
      int longOffset = offset * longCount;
      for (int i = 0; i < longCount; ++i) {
        if (longKeys[segment][longOffset + i] != key.getLongValue(i)) {
          equal = false;
          break;
        }
      }
      if (!equal) {
        continue;
      }
      int stringOffset = offset * stringCount;
      for (int i = 0; i < stringCount; ++i) {
        if (!isNull(mask, 0, stringKeyIndices[i])) {
          byte[] bytes = stringKeys[segment][stringOffset + i];
          if (!StringExpr.equal(key.getBytes(i), key.getByteStart(i),
              key.getByteLength(i), bytes, 0, bytes.length)) {
            equal = false;
            break;
          }
        }
      }
      if (equal) {
        return slot;
      }
    }
    return ~(freeList == 0 ? numUsed : freeList - 1);
  }

  private void put(VectorHashKeyWrapperBase key, byte[] mask, int hashCode,
      int insertionSlot, VectorAggregationBufferRow value) {
    assert value != null;
    assert insertionSlot == ~findSlot(key, mask, hashCode);
    allocateSlot(insertionSlot);
    int segment = entrySegment(insertionSlot);
    int offset = entryOffset(insertionSlot, segment);
    int maskOffset = offset * nullMaskBytes;
    System.arraycopy(mask, 0, nullMasks[segment], maskOffset, nullMaskBytes);
    int longOffset = offset * longCount;
    for (int i = 0; i < longCount; ++i) {
      longKeys[segment][longOffset + i] = isNull(mask, 0, longKeyIndices[i]) ?
          0 : key.getLongValue(i);
    }
    int stringOffset = offset * stringCount;
    for (int i = 0; i < stringCount; ++i) {
      assert stringKeys[segment][stringOffset + i] == null;
      if (!isNull(mask, 0, stringKeyIndices[i])) {
        int length = key.getByteLength(i);
        stringKeys[segment][stringOffset + i] = Arrays.copyOfRange(
            key.getBytes(i), key.getByteStart(i), key.getByteStart(i) + length);
        keyMemorySize += JAVA_DATA_MODEL.lengthForByteArrayOfSize(length);
      }
    }
    hashCodes[segment][offset] = hashCode;
    linkEntryWithSpreadHash(insertionSlot, hashCode, value);
    assert findSlot(key, mask, hashCode) == insertionSlot;
    assert getValue(insertionSlot) == value;
  }

  private void remove(int bucket, int previousSlot, int slot) {
    int segment = entrySegment(slot);
    int offset = entryOffset(slot, segment);
    assert values[segment][offset] != null;
    assert bucket == (hashCodes[segment][offset] & (bucketCapacity - 1));
    unlinkEntry(bucket, previousSlot, slot);
    int stringOffset = offset * stringCount;
    for (int i = 0; i < stringCount; ++i) {
      byte[] bytes = stringKeys[segment][stringOffset + i];
      if (bytes != null) {
        keyMemorySize -= JAVA_DATA_MODEL.lengthForByteArrayOfSize(bytes.length);
        stringKeys[segment][stringOffset + i] = null;
      }
    }
  }

  @Override
  int size() {
    return numEntries + (nullValue == null ? 0 : 1);
  }

  @Override
  void clear() {
    int remaining = numUsed;
    for (int segment = 0; segment < values.length && remaining > 0; ++segment) {
      int used = Math.min(remaining, values[segment].length);
      Arrays.fill(stringKeys[segment], 0, used * stringCount, null);
      remaining -= used;
    }
    clearEntries();
    nullValue = null;
    keyMemorySize = 0;
    assert checkInvariants();
  }

  @Override
  protected void growKeyStorage(int segment, int capacity) {
    longKeys = Arrays.copyOf(longKeys, segment + 1);
    stringKeys = Arrays.copyOf(stringKeys, segment + 1);
    nullMasks = Arrays.copyOf(nullMasks, segment + 1);
    hashCodes = Arrays.copyOf(hashCodes, segment + 1);
    longKeys[segment] = new long[Math.multiplyExact(capacity, longCount)];
    stringKeys[segment] = new byte[Math.multiplyExact(capacity, stringCount)][];
    nullMasks[segment] = new byte[Math.multiplyExact(capacity, nullMaskBytes)];
    hashCodes[segment] = new int[capacity];
  }

  @Override
  protected void resetKeyStorage() {
    longKeys = new long[values.length][];
    stringKeys = new byte[values.length][][];
    nullMasks = new byte[values.length][];
    hashCodes = new int[values.length][];
    for (int segment = 0; segment < values.length; ++segment) {
      int capacity = values[segment].length;
      longKeys[segment] = new long[Math.multiplyExact(capacity, longCount)];
      stringKeys[segment] = new byte[Math.multiplyExact(capacity, stringCount)][];
      nullMasks[segment] = new byte[Math.multiplyExact(capacity, nullMaskBytes)];
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
    if (!checkStructure() ||
        longKeys.length != values.length || stringKeys.length != values.length ||
        nullMasks.length != values.length ||
        hashCodes.length != values.length) {
      return false;
    }
    for (int segment = 0; segment < values.length; ++segment) {
      int capacity = values[segment].length;
      if (longKeys[segment].length != (long) capacity * longCount ||
          stringKeys[segment].length != (long) capacity * stringCount ||
          nullMasks[segment].length != (long) capacity * nullMaskBytes ||
          hashCodes[segment].length != capacity) {
        return false;
      }
    }
    VectorHashKeyWrapperGeneralLongString key =
        new VectorHashKeyWrapperGeneralLongString(null, longCount, stringCount, keyCount);
    byte[] mask = new byte[nullMaskBytes];
    long memorySize = 0;
    for (int slot = 0; slot < entryCapacity; ++slot) {
      int segment = entrySegment(slot);
      int offset = entryOffset(slot, segment);
      int maskOffset = offset * nullMaskBytes;
      int longOffset = offset * longCount;
      int stringOffset = offset * stringCount;
      if (values[segment][offset] != null) {
        System.arraycopy(nullMasks[segment], maskOffset, mask, 0, nullMaskBytes);
        key.clearIsNull();
        boolean allNull = true;
        for (int i = 0; i < longCount; ++i) {
          int keyIndex = longKeyIndices[i];
          if (isNull(mask, 0, keyIndex)) {
            if (longKeys[segment][longOffset + i] != 0) {
              return false;
            }
            key.assignNullLong(keyIndex, i);
          } else {
            key.assignLong(keyIndex, i, longKeys[segment][longOffset + i]);
            allNull = false;
          }
        }
        for (int i = 0; i < stringCount; ++i) {
          int keyIndex = stringKeyIndices[i];
          byte[] bytes = stringKeys[segment][stringOffset + i];
          if (isNull(mask, 0, keyIndex)) {
            if (bytes != null) {
              return false;
            }
            key.assignNullString(keyIndex, i);
          } else {
            if (bytes == null) {
              return false;
            }
            key.assignString(i, bytes, 0, bytes.length);
            memorySize += JAVA_DATA_MODEL.lengthForByteArrayOfSize(bytes.length);
            allNull = false;
          }
        }
        int remainingBits = keyCount & 7;
        if (allNull || (remainingBits != 0 &&
            (mask[nullMaskBytes - 1] & ~((1 << remainingBits) - 1)) != 0)) {
          return false;
        }
        key.setHashKey();
        if (hashCodes[segment][offset] != spreadHash(key.hashCode()) ||
            findSlot(key, mask, hashCodes[segment][offset]) != slot) {
          return false;
        }
      } else {
        for (int i = 0; i < stringCount; ++i) {
          if (stringKeys[segment][stringOffset + i] != null) {
            return false;
          }
        }
      }
    }
    return memorySize == keyMemorySize;
  }

  @Override
  long getEstimatedFixedMemorySize() {
    long memory = getEstimatedArrayMemorySize()
        + JAVA_DATA_MODEL.lengthForObjectArrayOfSize(longKeys.length)
        + JAVA_DATA_MODEL.lengthForObjectArrayOfSize(stringKeys.length)
        + JAVA_DATA_MODEL.lengthForObjectArrayOfSize(nullMasks.length)
        + JAVA_DATA_MODEL.lengthForObjectArrayOfSize(hashCodes.length);
    for (int segment = 0; segment < values.length; ++segment) {
      memory += JAVA_DATA_MODEL.lengthForLongArrayOfSize(longKeys[segment].length)
          + JAVA_DATA_MODEL.lengthForObjectArrayOfSize(stringKeys[segment].length)
          + JAVA_DATA_MODEL.lengthForByteArrayOfSize(nullMasks[segment].length)
          + JAVA_DATA_MODEL.lengthForIntArrayOfSize(hashCodes[segment].length);
    }
    return memory;
  }

  @Override
  long getEstimatedMemorySize() {
    return getEstimatedFixedMemorySize() + keyMemorySize;
  }
}
