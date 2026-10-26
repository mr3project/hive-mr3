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
package org.apache.hadoop.hive.ql.exec.tez;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchSerializer;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

public class TestVectorShuffleBatchAccumulator {
  @Test
  public void testDispatchReleasesConsumedReferencesAndPreservesRetainedBatches() throws Exception {
    for (boolean retained : new boolean[] {false, true}) {
      Operator<?> reducer = mock(Operator.class);
      when(reducer.batchNeedsClone()).thenReturn(retained);
      List<VectorizedRowBatch> dispatched = new ArrayList<>();
      List<byte[]> inputArrays = new ArrayList<>();
      doAnswer(invocation -> {
        VectorizedRowBatch batch = invocation.getArgument(0);
        dispatched.add(batch);
        inputArrays.add(((BytesColumnVector) batch.cols[0]).vector[0]);
        // Scratch values can keep input buffers alive even after a filter sets size to zero.
        ((BytesColumnVector) batch.cols[1]).setRef(7, inputArrays.get(inputArrays.size() - 1), 0, 1);
        batch.size = 0;
        return null;
      }).when(reducer).process(any(), eq(0));

      VectorShuffleBatchAccumulator accumulator = accumulator(reducer);
      BytesWritable first = frame((byte) 1);
      accumulator.process(new BytesWritable(), first, true);
      accumulator.flushPendingBatch();
      BytesWritable second = frame((byte) 2);
      accumulator.process(new BytesWritable(), second, true);
      accumulator.flushPendingBatch();

      assertEquals(2, dispatched.size());
      assertSame(first.getBytesRaw(), inputArrays.get(0));
      assertSame(second.getBytesRaw(), inputArrays.get(1));
      if (retained) {
        assertNotSame(dispatched.get(0), dispatched.get(1));
        assertSame(first.getBytesRaw(), ((BytesColumnVector) dispatched.get(0).cols[0]).vector[0]);
        assertSame(first.getBytesRaw(), ((BytesColumnVector) dispatched.get(0).cols[1]).vector[7]);
      } else {
        assertSame(dispatched.get(0), dispatched.get(1));
        for (int col = 0; col < 2; col++) {
          for (byte[] value : ((BytesColumnVector) dispatched.get(0).cols[col]).vector) {
            assertNull(value);
          }
        }
      }
      accumulator.discardPendingBatch();
      if (retained) {
        assertSame(second.getBytesRaw(), ((BytesColumnVector) dispatched.get(1).cols[0]).vector[0]);
      }
    }
  }

  @Test
  public void testFailedDispatchDoesNotResetPublishedBatch() throws Exception {
    Operator<?> reducer = mock(Operator.class);
    when(reducer.batchNeedsClone()).thenReturn(true);
    List<VectorizedRowBatch> dispatched = new ArrayList<>();
    doAnswer(invocation -> {
      dispatched.add(invocation.getArgument(0));
      throw new HiveException("dispatch failed after retaining input");
    }).when(reducer).process(any(), eq(0));
    VectorShuffleBatchAccumulator accumulator = accumulator(reducer);
    BytesWritable input = frame((byte) 3);
    accumulator.process(new BytesWritable(), input, true);
    assertThrows(HiveException.class, accumulator::flushPendingBatch);

    accumulator.discardPendingBatch();

    assertEquals(1, dispatched.get(0).size);
    assertSame(input.getBytesRaw(), ((BytesColumnVector) dispatched.get(0).cols[0]).vector[0]);
  }

  private static VectorShuffleBatchAccumulator accumulator(Operator<?> reducer) {
    VectorizedRowBatchCtx context = mock(VectorizedRowBatchCtx.class);
    when(context.createVectorizedRowBatch()).thenAnswer(invocation -> batch());
    return new VectorShuffleBatchAccumulator(context, reducer, batch(), 1, (byte) 0, null, null);
  }

  private static VectorizedRowBatch batch() {
    VectorizedRowBatch batch = new VectorizedRowBatch(2, 8);
    batch.cols[0] = new BytesColumnVector(8);
    batch.cols[1] = new BytesColumnVector(8);
    batch.projectionSize = 1;
    batch.reset();
    return batch;
  }

  private static BytesWritable frame(byte value) {
    VectorizedRowBatch source = batch();
    ((BytesColumnVector) source.cols[0]).setRef(0, new byte[] {value}, 0, 1);
    source.size = 1;
    byte[] buffer = new byte[128];
    int length = new VectorShuffleBatchSerializer().serialize(source, new int[] {0}, buffer, 5);
    BytesWritable serialized = new BytesWritable();
    serialized.setDirect(buffer, 5, length);
    return serialized;
  }
}
