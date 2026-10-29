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
package org.apache.hadoop.hive.ql.exec.vector.reducesink;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.tez.TezProcessor.TezKVOutputCollector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorSerializeRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchSerializer;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinarySerializeWrite;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.tez.runtime.library.api.KeyValueWriterEdge.WriteValueBytes;
import org.junit.Test;

public class TestVectorReduceSinkPartitionCapacity {
  @Test
  public void testUnpreparedPrefixRetains128ByteGate() throws Exception {
    TezKVOutputCollector out = mock(TezKVOutputCollector.class);
    TestOperator operator = new TestOperator(1, out);
    byte[] buffer = new byte[512];
    writerRegions(out, buffer, 128, 129);
    assertTrue(operator.collectBatch(source(1)));
    verify(out, times(4)).writeWithPartition(any(), any(), eq(0));
    verify(out, never()).completeWriteValueBytes(any(), anyInt(), eq(0));
    verify(out).completeWriteValueBytes(any(), eq(27), eq(1));
    assertEquals(6, operator.collectedRows());
  }

  @Test
  public void testPreparedPrefixUsesBoundBelow128Bytes() throws Exception {
    TezKVOutputCollector out = mock(TezKVOutputCollector.class);
    TestOperator operator = new TestOperator(1, out);
    byte[] buffer = new byte[512];
    writerRegions(out, buffer, 129, 27);
    assertTrue(operator.collectBatch(source(1)));
    verify(out).completeWriteValueBytes(any(), eq(43), eq(0));
    verify(out).completeWriteValueBytes(any(), eq(27), eq(1));
    verify(out, never()).writeWithPartition(any(), any(), anyInt());
    assertEquals(6, operator.collectedRows());
  }

  @Test
  public void testFirstPartitionRejectionFallsBackAndNextPartitionUsesCachedPrefix()
      throws Exception {
    TezKVOutputCollector out = mock(TezKVOutputCollector.class);
    TestOperator operator = new TestOperator(10, out);
    VectorizedRowBatch source = source(10);
    byte[] buffer = new byte[4096];
    writerRegions(out, buffer, 200, 181);
    doAnswer(invocation -> {
      Arrays.fill(buffer, (byte) 0x5a); // Row writes consume/reuse the writer's exposed region.
      return null;
    }).when(out).writeWithPartition(any(), any(), eq(0));
    assertTrue(operator.collectBatch(source));
    verify(out, times(4)).writeWithPartition(any(), any(), eq(0));
    verify(out, never()).completeWriteValueBytes(any(), anyInt(), eq(0));
    verify(out).completeWriteValueBytes(any(), eq(181), eq(1));
    assertEquals(6, operator.collectedRows());
    int[] mapping = new int[10];
    for (int column = 0; column < mapping.length; column++) {
      mapping[column] = column;
    }
    VectorShuffleBatchSerializer direct = new VectorShuffleBatchSerializer();
    direct.initialize(source.cols, mapping);
    byte[] expected = new byte[4096];
    int length = direct.serialize(source, mapping, new int[] {4, 5}, 0, 2, expected, 7);
    assertArrayEquals(Arrays.copyOfRange(expected, 7, 7 + length),
        Arrays.copyOfRange(buffer, 7, 7 + length));
  }

  private static void writerRegions(TezKVOutputCollector out, byte[] buffer,
      int firstCapacity, int secondCapacity) throws Exception {
    when(out.getNumUnorderedPartitions()).thenReturn(2);
    when(out.getPartitionerType()).thenReturn(0);
    when(out.requestWriteValueBytes(any(), eq(0)))
        .thenReturn(new WriteValueBytes(buffer, 7, firstCapacity));
    when(out.requestWriteValueBytes(any(), eq(1)))
        .thenReturn(new WriteValueBytes(buffer, 7, secondCapacity));
  }

  private static VectorizedRowBatch source(int columnCount) {
    VectorizedRowBatch batch = new VectorizedRowBatch(columnCount);
    batch.size = 6;
    for (int column = 0; column < columnCount; column++) {
      LongColumnVector values = new LongColumnVector();
      for (int row = 0; row < batch.size; row++) {
        values.vector[row] = column * 100 + row;
      }
      batch.cols[column] = values;
    }
    return batch;
  }

  private static class TestOperator extends VectorReduceSinkEmptyKeyOperator {
    TestOperator(int columnCount, TezKVOutputCollector out) throws Exception {
      isEmptyKey = true;
      reduceSkipTag = true;
      reduceSinkValueColumnMap = new int[columnCount];
      reduceSinkValueTypeInfos = new TypeInfo[columnCount];
      for (int column = 0; column < columnCount; column++) {
        reduceSinkValueColumnMap[column] = column;
        reduceSinkValueTypeInfos[column] = TypeInfoFactory.longTypeInfo;
      }
      valueLazyBinarySerializeWrite = new LazyBinarySerializeWrite(columnCount);
      valueVectorSerializeRow = new VectorSerializeRow<>(valueLazyBinarySerializeWrite);
      valueVectorSerializeRow.init(reduceSinkValueTypeInfos, reduceSinkValueColumnMap);
      valueOutput = new Output();
      valueVectorSerializeRow.setOutput(valueOutput);
      keyWritable = new HiveKey();
      valueBytesWritable = new BytesWritable();
      setOutputCollector(out);
      Field allowed = VectorReduceSinkCommonOperator.class
          .getDeclaredField("vectorShuffleBatchAllowed");
      allowed.setAccessible(true);
      allowed.setBoolean(this, true);
      Field partitions = VectorReduceSinkCommonOperator.class
          .getDeclaredField("vectorShuffleNumUnorderedPartitions");
      partitions.setAccessible(true);
      partitions.setInt(this, -1);
    }

    @Override
    protected void computeKeyHashCodes(VectorizedRowBatch batch, int[] hashes) {
      for (int row = 0; row < batch.size; row++) {
        hashes[row] = row < 4 ? 0 : 1;
      }
    }

    boolean collectBatch(VectorizedRowBatch batch) throws Exception {
      return tryCollectVectorShuffleBatch(batch, -1);
    }

    long collectedRows() {
      return numRows;
    }
  }
}
