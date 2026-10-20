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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.ReduceRecordSource;
import org.apache.hadoop.hive.ql.exec.tez.TezProcessor.TezKVOutputCollector;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchSerializer;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.VectorReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.VectorReduceSinkInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

public class TestVectorReduceSinkSinglePartitionBatch {
  @Test
  public void testBatchAndRowWritesUseSeparateStorage() throws Exception {
    try (Fixture fixture = new Fixture()) {
      VectorizedRowBatch first = batch(bytes("first"), bytes("second"));
      fixture.operator.process(first, -1);
      WrittenRecord batchRecord = fixture.records.get(0);
      assertBatchEquals(first, batchRecord);
      assertNotSame(fixture.operator.valueBytesWritable, batchRecord.wrapper);

      fixture.operator.process(batch(bytes("ordinary row")), -1);
      WrittenRecord rowRecord = fixture.records.get(1);
      assertSame(fixture.operator.valueBytesWritable, rowRecord.wrapper);
      assertNotSame(batchRecord.wrapper, rowRecord.wrapper);
      assertNotSame(batchRecord.backing, rowRecord.backing);
      assertArrayEquals(new byte[0], rowRecord.key);
      assertArrayEquals(batchRecord.payload, Arrays.copyOfRange(batchRecord.backing,
          batchRecord.offset, batchRecord.offset + batchRecord.payload.length));

      VectorizedRowBatch selected = batch(bytes("selected"), bytes("unused"), null);
      selected.selectedInUse = true;
      selected.selected[0] = 2;
      selected.selected[1] = 0;
      selected.size = 2;
      fixture.operator.process(selected, -1);
      WrittenRecord selectedRecord = fixture.records.get(2);
      assertBatchEquals(selected, selectedRecord);
      assertSame(batchRecord.wrapper, selectedRecord.wrapper);
      assertSame(batchRecord.backing, selectedRecord.backing);
      assertBatchEquals(first, batchRecord);
      assertEquals(3, fixture.records.size());
    }
  }

  @Test
  public void testBatchBufferGrowthAndReuse() throws Exception {
    try (Fixture fixture = new Fixture()) {
      fixture.operator.process(batch(bytes("small"), bytes("batch")), -1);
      WrittenRecord first = fixture.records.get(0);

      byte[] large = new byte[70 * 1024];
      Arrays.fill(large, (byte) 'x');
      VectorizedRowBatch grown = batch(large, large);
      fixture.operator.process(grown, -1);
      WrittenRecord grownRecord = fixture.records.get(1);
      assertBatchEquals(grown, grownRecord);
      assertSame(first.wrapper, grownRecord.wrapper);
      assertNotSame(first.backing, grownRecord.backing);

      fixture.operator.process(batch(bytes("row after growth")), -1);
      assertArrayEquals(grownRecord.payload, Arrays.copyOfRange(grownRecord.backing,
          grownRecord.offset, grownRecord.offset + grownRecord.payload.length));

      VectorizedRowBatch repeating = batch(bytes("repeated"), bytes("unused"));
      repeating.cols[0].isRepeating = true;
      fixture.operator.process(repeating, -1);
      WrittenRecord reused = fixture.records.get(3);
      assertBatchEquals(repeating, reused);
      assertSame(grownRecord.wrapper, reused.wrapper);
      assertSame(grownRecord.backing, reused.backing);
      // The collector consumed the earlier bytes before the producer reused its scratch array.
      assertBatchEquals(grown, grownRecord);
      assertEquals(4, fixture.records.size());
    }
  }

  private static VectorizedRowBatch batch(byte[]... values) {
    VectorizedRowBatch batch = new VectorizedRowBatch(1);
    BytesColumnVector column = new BytesColumnVector();
    column.initBuffer();
    for (int row = 0; row < values.length; row++) {
      if (values[row] == null) {
        column.noNulls = false;
        column.isNull[row] = true;
      } else {
        column.setVal(row, values[row]);
      }
    }
    batch.cols[0] = column;
    batch.size = values.length;
    return batch;
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static void assertBatchEquals(VectorizedRowBatch batch, WrittenRecord record) {
    assertArrayEquals(ReduceRecordSource.VECTOR_BATCH_KEY_BYTES, record.key);
    byte[] expected = new byte[512 * 1024];
    int length = new VectorShuffleBatchSerializer().serialize(batch, new int[] {0}, expected, 0);
    assertArrayEquals(Arrays.copyOf(expected, length), record.payload);
  }

  private static final class WrittenRecord {
    private final byte[] key;
    private final BytesWritable wrapper;
    private final byte[] backing;
    private final int offset;
    private final byte[] payload;

    private WrittenRecord(BytesWritable key, BytesWritable value) {
      this.key = key.copyBytes();
      wrapper = value;
      backing = value.getBytesRaw();
      offset = value.getOffset();
      payload = value.copyBytes();
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final Configuration configuration = new Configuration();
    private final List<WrittenRecord> records = new ArrayList<>();
    private final VectorReduceSinkEmptyKeyOperator operator;

    private Fixture() throws Exception {
      // Initialization reads the cached work name when debug logging is enabled.
      HiveConf.setVar(configuration, HiveConf.ConfVars.PLAN,
          "file:/vector-reduce-sink-test-" + UUID.randomUUID());
      Utilities.setMapWork(configuration, new MapWork("single-partition-batch"));

      ReduceSinkDesc descriptor = new ReduceSinkDesc();
      descriptor.setSkipTag(true);
      VectorReduceSinkInfo info = new VectorReduceSinkInfo();
      info.setReduceSinkValueColumnMap(new int[] {0});
      info.setReduceSinkValueTypeInfos(new TypeInfo[] {TypeInfoFactory.stringTypeInfo});
      VectorReduceSinkDesc vectorDescriptor = new VectorReduceSinkDesc();
      vectorDescriptor.setIsEmptyKey(true);
      vectorDescriptor.setIsEmptyValue(false);
      vectorDescriptor.setVectorReduceSinkInfo(info);
      operator = new VectorReduceSinkEmptyKeyOperator(new CompilationOpContext(), descriptor,
          null, vectorDescriptor);
      operator.initializeOp(configuration);

      TezKVOutputCollector collector = mock(TezKVOutputCollector.class);
      when(collector.getNumUnorderedPartitions()).thenReturn(1);
      doAnswer(invocation -> {
        // Consume the key/value before writeWithPartition returns, as the Tez writer does.
        records.add(new WrittenRecord(invocation.getArgument(0), invocation.getArgument(1)));
        return null;
      }).when(collector).writeWithPartition(any(BytesWritable.class), any(BytesWritable.class), eq(0));
      operator.setOutputCollector(collector);
    }

    @Override
    public void close() {
      Utilities.clearWorkMapForConf(configuration);
    }
  }
}
