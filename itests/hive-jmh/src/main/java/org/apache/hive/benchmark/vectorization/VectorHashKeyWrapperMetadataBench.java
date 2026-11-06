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
package org.apache.hive.benchmark.vectorization;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.ql.exec.KeyWrapper;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IdentityExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperBase;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperBatch;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperGeneralLongString;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Compares metadata layouts by running this unchanged benchmark against each production build.
 * See itests/hive-jmh/README-vector-hash-key-metadata.md for the build and run commands.
 * All fixture allocation, copying, random generation, and validation occur at trial setup.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 8, time = 2)
@Fork(3)
@Threads(1)
public class VectorHashKeyWrapperMetadataBench {
  private static final int ROWS = VectorizedRowBatch.DEFAULT_SIZE;
  // Selection keeps 1,024 sorted, sparse rows from vectors with room for 2,048 rows.
  private static final int INPUT_CAPACITY = ROWS * 2;
  private static final long SEED = 0x1372a9b64c5d8e01L;

  public enum KeyShape {
    L2_S2(2, 2), L1_S4(1, 4), L4_S1(4, 1);

    private final int longCount;
    private final int stringCount;

    KeyShape(int longCount, int stringCount) {
      this.longCount = longCount;
      this.stringCount = stringCount;
    }
  }

  public enum StringSizes {
    SHORT, MIXED
  }

  public enum NullProfile {
    NONE(0, 0), MIXED(10, 10), NULL_HEAVY(70, 10);

    private final int nullPercent;
    private final int emptyPercent;

    NullProfile(int nullPercent, int emptyPercent) {
      this.nullPercent = nullPercent;
      this.emptyPercent = emptyPercent;
    }
  }

  @State(Scope.Thread)
  public static class InputState {
    @Param({"L2_S2"})
    public KeyShape keyShape;

    @Param({"SHORT"})
    public StringSizes stringSizes;

    @Param({"NONE"})
    public NullProfile nullProfile;

    @Param({"95"})
    public int hitPercent;

    @Param({"4096"})
    public int residentGroups;

    @Param({"32"})
    public int batchRingSize;

    @Param({"false"})
    public boolean selected;

    @Param({"false"})
    public boolean groupingSets;

    private VectorHashKeyWrapperBatch wrapperBatch;
    private VectorHashKeyWrapperBase[] wrappers;
    private HashMap<KeyWrapper, Integer> groups;
    private VectorizedRowBatch[] batches;
    private boolean[] groupingNulls;
    private int[] longColumns;
    private int[] stringColumns;
    private int nextBatch;

    @Setup(Level.Trial)
    public void setup() throws HiveException {
      if (hitPercent < 0 || hitPercent > 100 || residentGroups <= 0
          || residentGroups > Integer.MAX_VALUE / 2 || batchRingSize <= 0) {
        throw new IllegalArgumentException("Invalid hitPercent, residentGroups, or batchRingSize");
      }
      initializeLayout();
      // Fail rather than accidentally measuring one of the smaller specialized wrappers.
      if (!(wrappers[0] instanceof VectorHashKeyWrapperGeneralLongString)) {
        throw new IllegalStateException("The key shape must select GeneralLongString");
      }

      groups = HashMap.newHashMap(residentGroups);
      VectorizedRowBatch residentBatch = newBatch();
      int[] ids = new int[INPUT_CAPACITY];
      for (int first = 0; first < residentGroups;) {
        int count = Math.min(ROWS, residentGroups - first);
        for (int row = 0; row < count; ++row) {
          ids[row] = first + row;
        }
        populate(residentBatch, ids, count);
        residentBatch.size = count;
        evaluate(residentBatch);
        for (int row = 0; row < count; ++row) {
          VectorHashKeyWrapperBase copy = (VectorHashKeyWrapperBase) wrappers[row].copyKey();
          validateCopy(wrappers[row], copy);
          if (groups.put(copy, ids[row]) != null) {
            throw new IllegalStateException("Resident keys must be distinct");
          }
        }
        first += count;
      }
      if (groups.size() != residentGroups) {
        throw new IllegalStateException("Incorrect resident group count");
      }

      Random random = new Random(SEED);
      batches = new VectorizedRowBatch[batchRingSize];
      for (int batchIndex = 0; batchIndex < batches.length; ++batchIndex) {
        VectorizedRowBatch batch = newBatch();
        for (int row = 0; row < INPUT_CAPACITY; ++row) {
          // Miss IDs occupy a disjoint range. The first long is never nulled, even for
          // grouping sets, so null-heavy keys cannot collapse into an unintended hit.
          ids[row] = random.nextInt(residentGroups)
              + (random.nextInt(100) < hitPercent ? 0 : residentGroups);
        }
        populate(batch, ids, INPUT_CAPACITY);
        batch.size = ROWS;
        batch.selectedInUse = selected;
        for (int row = 0; row < ROWS; ++row) {
          batch.selected[row] = 2 * row + random.nextInt(2);
        }
        evaluate(batch);
        for (int row = 0; row < ROWS; ++row) {
          int id = ids[selected ? batch.selected[row] : row];
          Integer actual = groups.get(wrappers[row]);
          if (id < residentGroups ? actual == null || actual.intValue() != id : actual != null) {
            throw new IllegalStateException("Incorrect lookup in batch " + batchIndex + ", row " + row);
          }
        }
        batches[batchIndex] = batch;
      }
      nextBatch = 0;
    }

    private void initializeLayout() throws HiveException {
      int keyCount = keyShape.longCount + keyShape.stringCount;
      VectorExpression[] expressions = new VectorExpression[keyCount];
      TypeInfo[] types = new TypeInfo[keyCount];
      longColumns = new int[keyShape.longCount];
      stringColumns = new int[keyShape.stringCount];
      int column = 0;
      // Interleave types to exercise the distinction between key and type-specific indices.
      for (int i = 0; i < Math.max(longColumns.length, stringColumns.length); ++i) {
        if (i < longColumns.length) {
          longColumns[i] = column;
          types[column++] = TypeInfoFactory.longTypeInfo;
        }
        if (i < stringColumns.length) {
          stringColumns[i] = column;
          types[column++] = TypeInfoFactory.stringTypeInfo;
        }
      }
      for (int i = 0; i < keyCount; ++i) {
        expressions[i] = new IdentityExpression(i);
      }
      wrapperBatch = VectorHashKeyWrapperBatch.compileKeyWrapperBatch(expressions, types);
      wrappers = wrapperBatch.getVectorHashKeyWrappers();
      groupingNulls = new boolean[keyCount];
      groupingNulls[stringColumns[0]] = true;
      if (longColumns.length > 1) {
        groupingNulls[longColumns[longColumns.length - 1]] = true;
      }
    }

    private VectorizedRowBatch newBatch() {
      VectorizedRowBatch batch = new VectorizedRowBatch(
          longColumns.length + stringColumns.length, INPUT_CAPACITY);
      for (int column : longColumns) {
        batch.cols[column] = new LongColumnVector(INPUT_CAPACITY);
      }
      for (int column : stringColumns) {
        batch.cols[column] = new BytesColumnVector(INPUT_CAPACITY);
      }
      return batch;
    }

    private void populate(VectorizedRowBatch batch, int[] ids, int count) {
      for (int i = 0; i < longColumns.length; ++i) {
        LongColumnVector column = (LongColumnVector) batch.cols[longColumns[i]];
        column.noNulls = true;
        for (int row = 0; row < count; ++row) {
          boolean isNull = i != 0 && bucket(mix(ids[row], i + 100)) < nullProfile.nullPercent;
          column.isNull[row] = isNull;
          column.noNulls &= !isNull;
          column.vector[row] = i == 0 ? ids[row] : mix(ids[row], i + 200);
        }
      }
      for (int i = 0; i < stringColumns.length; ++i) {
        BytesColumnVector column = (BytesColumnVector) batch.cols[stringColumns[i]];
        column.noNulls = true;
        int offset = 0;
        for (int row = 0; row < count; ++row) {
          long bits = mix(ids[row], i);
          int kind = bucket(bits);
          boolean isNull = kind < nullProfile.nullPercent;
          boolean isEmpty = kind < nullProfile.nullPercent + nullProfile.emptyPercent;
          int length = isEmpty ? 0 : stringLength(mix(ids[row], i + 300));
          column.isNull[row] = isNull;
          column.noNulls &= !isNull;
          // Vary the padding as well as the length. Even the first slice starts after zero.
          offset += 1 + (int) (bits & 7);
          column.start[row] = offset;
          column.length[row] = length;
          offset += length;
        }
        byte[] buffer = new byte[offset];
        for (int row = 0; row < count; ++row) {
          column.vector[row] = buffer;
          long content = mix(ids[row], i + 400);
          for (int p = column.start[row], end = p + column.length[row]; p < end; ++p) {
            content ^= content << 13;
            content ^= content >>> 7;
            content ^= content << 17;
            buffer[p] = (byte) content;
          }
        }
      }
    }

    private int stringLength(long bits) {
      if (stringSizes == StringSizes.SHORT) {
        return 1 + (int) (bits & 15);
      }
      // MIXED: 90% 8..32 bytes, 10% 128..256 bytes, independent of null/empty selection.
      return bucket(bits) < 90
          ? 8 + (int) Math.floorMod(bits >>> 8, 25L)
          : 128 + (int) Math.floorMod(bits >>> 8, 129L);
    }

    private void validateCopy(VectorHashKeyWrapperBase source, VectorHashKeyWrapperBase copy) {
      if (source == copy || source.hashCode() != copy.hashCode()
          || !source.equals(copy) || !copy.equals(source)) {
        throw new IllegalStateException("A copied key must be independently equal to its source");
      }
      for (int i = 0; i < stringColumns.length; ++i) {
        int length = source.getByteLength(i);
        if (copy.getByteStart(i) != 0 || copy.getByteLength(i) != length) {
          throw new IllegalStateException("A copied slice must have start zero and the same length");
        }
        if (length != -1 && (source.getBytes(i) == copy.getBytes(i)
            || !Arrays.equals(source.getBytes(i), source.getByteStart(i),
                source.getByteStart(i) + length, copy.getBytes(i), 0, length))) {
          throw new IllegalStateException("Copied bytes must have independent storage and equal contents");
        }
      }
    }

    private VectorizedRowBatch nextInput() {
      VectorizedRowBatch result = batches[nextBatch];
      nextBatch = nextBatch + 1 == batches.length ? 0 : nextBatch + 1;
      return result;
    }

    private void evaluate(VectorizedRowBatch batch) throws HiveException {
      if (groupingSets) {
        wrapperBatch.evaluateBatchGroupingSets(batch, groupingNulls);
      } else {
        wrapperBatch.evaluateBatch(batch);
      }
    }

    private static int bucket(long bits) {
      return (int) Math.floorMod(bits, 100L);
    }

    private static long mix(int id, int salt) {
      long value = SEED + id + 0x9e3779b97f4a7c15L * (salt + 1L);
      value = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
      value = (value ^ (value >>> 27)) * 0x94d049bb133111ebL;
      return value ^ (value >>> 31);
    }
  }

  @Benchmark
  @OperationsPerInvocation(ROWS)
  public long evaluateAndHash(InputState state) throws HiveException {
    state.evaluate(state.nextInput());
    long checksum = 0;
    for (int row = 0; row < ROWS; ++row) {
      checksum += state.wrappers[row].hashCode();
    }
    return checksum;
  }

  @Benchmark
  @OperationsPerInvocation(ROWS)
  public long evaluateAndProbe(InputState state) throws HiveException {
    state.evaluate(state.nextInput());
    long checksum = 0;
    for (int row = 0; row < ROWS; ++row) {
      Integer groupId = state.groups.get(state.wrappers[row]);
      checksum += groupId == null ? -1L : groupId.longValue();
    }
    return checksum;
  }
}
