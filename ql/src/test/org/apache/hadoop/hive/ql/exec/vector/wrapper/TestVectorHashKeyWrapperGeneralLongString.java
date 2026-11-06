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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.Random;

import org.apache.hive.common.util.Murmur3;
import org.junit.Test;

/** Tests slice metadata, hash compatibility, and independent copied string storage. */
public class TestVectorHashKeyWrapperGeneralLongString {
  @Test
  public void testHashAndCopyOfSlices() {
    int[] lengthsToTest = {-1, 0, 1, 3, 4, 7, 8, 15, 16, 17, 31, 32, 33, 64};
    for (boolean sharedContext : new boolean[] {false, true}) {
      VectorHashKeyWrapperBase.HashContext context =
          sharedContext ? new VectorHashKeyWrapperBase.HashContext() : null;
      VectorHashKeyWrapperGeneralLongString source =
          new VectorHashKeyWrapperGeneralLongString(context, 2, 4, 6);
      // The first copy must resize the target; subsequent copies reuse its arrays and buffers.
      VectorHashKeyWrapperGeneralLongString reused =
          new VectorHashKeyWrapperGeneralLongString(context, 0, 1, 1);
      Random random = new Random(9371);
      for (int iteration = 0; iteration < 200; ++iteration) {
        source.clearIsNull();
        long[] longs = new long[2];
        boolean[] nulls = new boolean[6];
        for (int i = 0; i < longs.length; ++i) {
          nulls[i] = random.nextBoolean();
          if (nulls[i]) {
            source.assignNullLong(i, i);
          } else {
            longs[i] = random.nextLong();
            source.assignLong(i, i, longs[i]);
          }
        }
        byte[][] bytes = new byte[4][];
        int[] starts = new int[4];
        int[] lengths = new int[4];
        for (int i = 0; i < lengths.length; ++i) {
          // Cover all-null, all-empty, and one-nonempty paths before randomized mixtures.
          int length = iteration == 0 ? -1 : iteration == 1 ? 0
              : iteration == 2 ? (i == 0 ? 17 : -1)
              : lengthsToTest[random.nextInt(lengthsToTest.length)];
          lengths[i] = length;
          nulls[i + 2] = length == -1;
          if (length == -1) {
            source.assignNullString(i + 2, i);
          } else {
            starts[i] = 1 + random.nextInt(12);
            bytes[i] = new byte[starts[i] + length + 5];
            random.nextBytes(bytes[i]);
            source.assignString(i, bytes[i], starts[i], length);
          }
          assertEquals(starts[i], source.getByteStart(i));
          assertEquals(length, source.getByteLength(i));
        }

        int seed = Arrays.hashCode(longs) ^ Arrays.hashCode(nulls);
        Murmur3.IncrementalHash32 reference = new Murmur3.IncrementalHash32();
        reference.start(seed);
        boolean hasNonNull = false;
        for (int i = 0; i < lengths.length; ++i) {
          if (lengths[i] != -1) {
            reference.add(bytes[i], starts[i], lengths[i]);
            hasNonNull = true;
          }
        }
        source.setHashKey();
        assertEquals(hasNonNull ? reference.end() : seed, source.hashCode());
        assertEquals("longs " + Arrays.toString(longs) + ", byte lengths " + Arrays.toString(lengths)
            + ", nulls " + Arrays.toString(nulls), source.toString());

        VectorHashKeyWrapperGeneralLongString clone =
            (VectorHashKeyWrapperGeneralLongString) source.copyKey();
        source.copyKey(reused);
        checkCopy(source, clone);
        checkCopy(source, reused);
        // Rehash normalized slices to catch copies that accidentally retained input offsets.
        clone.setHashKey();
        reused.setHashKey();
        assertEquals(source.hashCode(), clone.hashCode());
        assertEquals(source.hashCode(), reused.hashCode());
      }
    }
  }

  private void checkCopy(VectorHashKeyWrapperGeneralLongString source,
      VectorHashKeyWrapperGeneralLongString copy) {
    assertEquals(source, copy);
    assertEquals(copy, source);
    assertEquals(source.toString(), copy.toString());
    assertEquals(source.getVariableSize(), copy.getVariableSize());
    for (int i = 0; i < 6; ++i) {
      assertEquals(source.isNull(i), copy.isNull(i));
    }
    for (int i = 0; i < 4; ++i) {
      int length = source.getByteLength(i);
      assertEquals(0, copy.getByteStart(i));
      assertEquals(length, copy.getByteLength(i));
      if (length == -1) {
        assertNull(copy.getBytes(i));
      } else {
        assertNotSame(source.getBytes(i), copy.getBytes(i));
        assertArrayEquals(Arrays.copyOfRange(source.getBytes(i), source.getByteStart(i),
            source.getByteStart(i) + length), Arrays.copyOfRange(copy.getBytes(i), 0, length));
      }
    }
  }

  @Test
  public void testEqualityChecksLengthsDespiteMatchingHashes() {
    byte[] bytes = {'a', 'b', 'c'};
    VectorHashKeyWrapperGeneralLongString left =
        new VectorHashKeyWrapperGeneralLongString(null, 0, 2, 2);
    VectorHashKeyWrapperGeneralLongString right =
        new VectorHashKeyWrapperGeneralLongString(null, 0, 2, 2);
    left.assignString(0, bytes, 0, 1);
    left.assignString(1, bytes, 1, 2);
    right.assignString(0, bytes, 0, 2);
    right.assignString(1, bytes, 2, 1);
    left.setHashKey();
    right.setHashKey();
    assertEquals(left.hashCode(), right.hashCode());
    assertFalse(left.equals(right));
    assertFalse(right.equals(left));
  }

  @Test
  public void testCopyWithoutStrings() {
    VectorHashKeyWrapperGeneralLongString source =
        new VectorHashKeyWrapperGeneralLongString(null, 4, 0, 4);
    VectorHashKeyWrapperGeneralLongString target =
        new VectorHashKeyWrapperGeneralLongString(null, 0, 2, 2);
    for (int i = 0; i < 4; ++i) {
      source.assignLong(i, i, i + 10);
    }
    source.setHashKey();
    source.copyKey(target);
    target.setHashKey();
    assertEquals(source, target);
    assertEquals(source, source.copyKey());
    assertEquals(0, target.getVariableSize());
    assertEquals(source.toString(), target.toString());
  }
}
