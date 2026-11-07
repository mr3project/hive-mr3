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

package org.apache.hadoop.hive.ql.exec.vector.wrapper;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperBase.HashContext;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/** Compares row-wise and column-wise hashing of a complete 1,024-row batch. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 3, jvmArgsAppend = "-ea")
@Threads(1)
public class VectorHashKeyWrapperLongStringHashBench {

  private static final int BATCH_SIZE = 1024;

  public enum Layout {
    L4_S0(4, 0),
    L8_S0(8, 0),
    L0_S4(0, 4),
    L2_S2(2, 2),
    L4_S5(4, 5);

    private final int longCount;
    private final int stringCount;

    Layout(int longCount, int stringCount) {
      this.longCount = longCount;
      this.stringCount = stringCount;
    }
  }

  public enum NullPattern {
    NO_NULLS,
    RANDOM_10_PERCENT,
    HALF_STRINGS_NULL,
    ALL_STRINGS_NULL
  }

  public enum Reuse {
    HOT,
    ROTATING
  }

  @Param({"L4_S0", "L8_S0", "L0_S4", "L2_S2", "L4_S5"})
  public Layout layout;

  @Param({"NO_NULLS"})
  public NullPattern nullPattern;

  @Param({"32"})
  public int stringBytes;

  @Param({"HOT", "ROTATING"})
  public Reuse reuse;

  private VectorHashKeyWrapperLongStringBatchStorage[] batches;
  private int nextBatchIndex;

  @Setup(Level.Trial)
  public void setup() {
    Random random = new Random(0x67C0FFEE1234ABCDL);
    batches = new VectorHashKeyWrapperLongStringBatchStorage[reuse == Reuse.HOT ? 1 : 16];
    int[] expectedHashes = new int[BATCH_SIZE];
    for (int batchIndex = 0; batchIndex < batches.length; ++batchIndex) {
      VectorHashKeyWrapperLongStringBatchStorage storage =
          new VectorHashKeyWrapperLongStringBatchStorage(
              new HashContext(), BATCH_SIZE, layout.longCount, layout.stringCount);
      batches[batchIndex] = storage;
      populate(storage, random);

      setHashKeysRowWise(storage);
      for (int row = 0; row < BATCH_SIZE; ++row) {
        expectedHashes[row] = storage.rowViews[row].hashCode();
      }
      storage.setHashKeys(BATCH_SIZE);
      for (int row = 0; row < BATCH_SIZE; ++row) {
        assert expectedHashes[row] == storage.rowViews[row].hashCode() :
            "Hash mismatch for batch " + batchIndex + ", row " + row;
      }
    }
    nextBatchIndex = 0;
  }

  private void populate(VectorHashKeyWrapperLongStringBatchStorage storage, Random random) {
    // Q67 orders three numeric keys, five strings, and then the grouping identifier.
    boolean q67Layout = layout == Layout.L4_S5;
    int nullStringCount = nullPattern == NullPattern.ALL_STRINGS_NULL ? layout.stringCount :
        nullPattern == NullPattern.HALF_STRINGS_NULL ? layout.stringCount / 2 : 0;
    long groupingId = ((1L << nullStringCount) - 1) << (layout.stringCount - nullStringCount);
    for (int index = 0; index < layout.longCount; ++index) {
      boolean groupingIdKey = q67Layout && index == layout.longCount - 1;
      int keyIndex = groupingIdKey ? storage.keyCount - 1 : index;
      for (int row = 0; row < BATCH_SIZE; ++row) {
        long value = random.nextLong();
        boolean randomNull = random.nextInt(10) == 0;
        boolean isNull = !groupingIdKey && nullPattern == NullPattern.RANDOM_10_PERCENT && randomNull;
        storage.longValues[index * BATCH_SIZE + row] = groupingIdKey ? groupingId : isNull ? 0 : value;
        storage.isNull[keyIndex * BATCH_SIZE + row] = isNull;
      }
    }
    for (int index = 0; index < layout.stringCount; ++index) {
      int keyIndex = (q67Layout ? layout.longCount - 1 : layout.longCount) + index;
      for (int row = 0; row < BATCH_SIZE; ++row) {
        int length = stringBytes == 0 ? 0 : stringBytes / 2 + random.nextInt(stringBytes + 1);
        int start = 1 + random.nextInt(7);
        byte[] bytes = new byte[start + length + 3];
        for (int i = 0; i < bytes.length; ++i) {
          bytes[i] = (byte) ('a' + random.nextInt(26));
        }
        boolean randomNull = random.nextInt(10) == 0;
        boolean isNull = index < nullStringCount ||
            nullPattern == NullPattern.RANDOM_10_PERCENT && randomNull;
        int offset = index * BATCH_SIZE + row;
        storage.byteValues[offset] = isNull ? null : bytes;
        storage.byteStarts[offset] = isNull ? 0 : start;
        storage.byteLengths[offset] = isNull ? -1 : length;
        storage.isNull[keyIndex * BATCH_SIZE + row] = isNull;
      }
    }
  }

  private VectorHashKeyWrapperLongStringBatchStorage nextBatch() {
    VectorHashKeyWrapperLongStringBatchStorage storage = batches[nextBatchIndex++];
    if (nextBatchIndex == batches.length) {
      nextBatchIndex = 0;
    }
    return storage;
  }

  private static void setHashKeysRowWise(VectorHashKeyWrapperLongStringBatchStorage storage) {
    for (int row = 0; row < BATCH_SIZE; ++row) {
      storage.rowViews[row].hashcode = storage.computeHash(row);
    }
  }

  private static long checksum(VectorHashKeyWrapperLongStringBatchStorage storage) {
    long sum = 0;
    for (int row = 0; row < BATCH_SIZE; ++row) {
      sum += storage.rowViews[row].hashCode();
    }
    return sum;
  }

  @Benchmark
  public long rowWise() {
    VectorHashKeyWrapperLongStringBatchStorage storage = nextBatch();
    setHashKeysRowWise(storage);
    return checksum(storage);
  }

  @Benchmark
  public long columnWise() {
    VectorHashKeyWrapperLongStringBatchStorage storage = nextBatch();
    storage.setHashKeys(BATCH_SIZE);
    return checksum(storage);
  }

  @Benchmark
  public long checksumOnly() {
    return checksum(nextBatch());
  }
}
