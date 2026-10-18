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

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.apache.hive.common.util.Murmur3;
import org.junit.Test;

public class TestVectorHashKeyWrapperStringHash {
  private static final int[] LENGTHS = {-1, 0, 17};
  private static final long[] LONG_VALUES = {0x0123456789abcdefL, -99321L};

  @Test
  public void testStringHashesMatchIncrementalHash() {
    VectorHashKeyWrapperBase.HashContext context = new VectorHashKeyWrapperBase.HashContext();
    checkHashes(new VectorHashKeyWrapperSingleString(context), 0, 0, 1);
    checkHashes(new VectorHashKeyWrapperSingleLongSingleString(context), 1, 0, 1);
    checkHashes(new VectorHashKeyWrapperTwoLongSingleString(context), 2, 0, 1);
    checkHashes(new VectorHashKeyWrapperTwoString(context), 0, 0, 2);
    checkHashes(new VectorHashKeyWrapperSingleLongTwoString(context), 1, 0, 2);
    checkHashes(new VectorHashKeyWrapperThreeString(context), 0, 0, 3);
    checkHashes(new VectorHashKeyWrapperGeneralLongString(context, 2, 4, 6), 2, 0, 4);
    checkHashes(new VectorHashKeyWrapperGeneral(context, 1, 1, 4, 0, 0, 0, 6), 1, 1, 4);
  }

  private void checkHashes(VectorHashKeyWrapperBase wrapper,
      int longCount, int doubleCount, int stringCount) {
    byte[][] values = new byte[stringCount][40];
    int[] starts = new int[stringCount];
    for (int i = 0; i < stringCount; ++i) {
      starts[i] = i + 1;
      for (int j = 0; j < values[i].length; ++j) {
        values[i][j] = (byte) (37 * j + 53 * i);
      }
    }
    long[] longs = Arrays.copyOf(LONG_VALUES, longCount);
    double[] doubles = doubleCount == 0 ? new double[0] : new double[] {42.5};
    int firstStringKey = longCount + doubleCount;
    boolean[] nulls = new boolean[firstStringKey + stringCount];
    int[] lengths = new int[stringCount];
    Murmur3.IncrementalHash32 baseline = new Murmur3.IncrementalHash32();
    int combinations = 1;
    for (int i = 0; i < stringCount; ++i) {
      combinations *= LENGTHS.length;
    }

    // Reuse both the wrapper and its shared hash context across all null/empty/value transitions.
    for (int combination = 0; combination < combinations; ++combination) {
      wrapper.clearIsNull();
      Arrays.fill(nulls, false);
      for (int i = 0; i < longCount; ++i) {
        wrapper.assignLong(i, i, longs[i]);
      }
      for (int i = 0; i < doubleCount; ++i) {
        wrapper.assignDouble(i, doubles[i]);
      }
      int remaining = combination;
      for (int i = 0; i < stringCount; ++i) {
        lengths[i] = LENGTHS[remaining % LENGTHS.length];
        remaining /= LENGTHS.length;
        if (lengths[i] == -1) {
          nulls[firstStringKey + i] = true;
          wrapper.assignNullString(firstStringKey + i, i);
        } else {
          wrapper.assignString(i, values[i], starts[i], lengths[i]);
        }
      }

      int seed = Arrays.hashCode(nulls);
      if (wrapper instanceof VectorHashKeyWrapperGeneral) {
        seed ^= Arrays.hashCode(longs) ^ Arrays.hashCode(doubles);
      } else if (wrapper instanceof VectorHashKeyWrapperGeneralLongString) {
        seed ^= Arrays.hashCode(longs);
      } else if (longCount != 0) {
        seed += 31 * Arrays.hashCode(longs);
      }
      boolean started = false;
      for (int i = 0; i < stringCount; ++i) {
        if (lengths[i] != -1) {
          if (!started) {
            baseline.start(seed);
            started = true;
          }
          baseline.add(values[i], starts[i], lengths[i]);
        }
      }
      int expected = started ? baseline.end() : seed;
      wrapper.setHashKey();
      assertEquals(wrapper.getClass().getSimpleName() + " lengths=" + Arrays.toString(lengths),
          expected, wrapper.hashCode());
    }
  }
}
