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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.ql.exec.TopNHash;
import org.apache.hadoop.hive.ql.exec.tez.TezProcessor.TezKVOutputCollector;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorSerializeRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.StringAppender;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinarySerializeWrite;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.logging.log4j.Level;
import org.apache.tez.runtime.library.api.KeyValueWriterEdge.WriteValueBytes;
import org.junit.Test;

public class TestVectorReduceSinkShuffleStatistics {
  private static final String LOGGER = VectorReduceSinkCommonOperator.class.getName();

  @Test
  public void testPartitionOutcomesRemainDistinct() throws Exception {
    TezKVOutputCollector out = output(5);
    TestOperator operator = new TestOperator(out, TypeInfoFactory.stringTypeInfo);
    operator.partitions = new int[] {0, 0, 1, 2, 2, 3, 3, 4, 4};
    BytesColumnVector values = new BytesColumnVector();
    for (int row = 0; row < 9; row++) {
      byte[] bytes = new byte[row >= 7 ? 256 : 0];
      values.setRef(row, bytes, 0, bytes.length);
    }
    when(out.requestWriteValueBytes(any(), eq(0)))
        .thenReturn(region(128));
    // Partition 2 retains the mock's null response.
    when(out.requestWriteValueBytes(any(), eq(3)))
        .thenReturn(region(512));
    when(out.requestWriteValueBytes(any(), eq(4)))
        .thenReturn(region(129));
    try (SummaryCapture log = new SummaryCapture()) {
      assertTrue(operator.collectBatch(batch(9, values)));
      operator.finish(false);
      assertEquals(Arrays.asList("xxxxx 5 1 9 1 1 1 1 1 0"), log.lines());
    }
  }

  @Test
  public void testLowerBoundRejectionCountsAsCapacityFallback() throws Exception {
    TezKVOutputCollector out = output(2);
    TypeInfo[] types = new TypeInfo[10];
    Arrays.fill(types, TypeInfoFactory.longTypeInfo);
    TestOperator operator = new TestOperator(out, types);
    operator.partitions = new int[] {0, 0, 0, 0, 1, 1};
    ColumnVector[] columns = new ColumnVector[10];
    Arrays.setAll(columns, column -> new LongColumnVector());
    when(out.requestWriteValueBytes(any(), eq(0)))
        .thenReturn(region(200));
    when(out.requestWriteValueBytes(any(), eq(1)))
        .thenReturn(region(181));
    try (SummaryCapture log = new SummaryCapture()) {
      assertTrue(operator.collectBatch(batch(6, columns)));
      operator.finish(false);
      assertEquals(Arrays.asList("xxxxx 2 1 6 1 0 0 1 0 0"), log.lines());
    }
  }

  @Test
  public void testPrefixPreparationOverflowCountsAsOverflowFallback() throws Exception {
    TezKVOutputCollector out = output(2);
    TestOperator operator = new TestOperator(out, TypeInfoFactory.stringTypeInfo);
    operator.partitions = new int[] {0, 0};
    BytesColumnVector values = new BytesColumnVector();
    values.isRepeating = true;
    values.setRef(0, new byte[256], 0, 256);
    when(out.requestWriteValueBytes(any(), eq(0)))
        .thenReturn(region(129));
    try (SummaryCapture log = new SummaryCapture()) {
      assertTrue(operator.collectBatch(batch(2, values)));
      operator.finish(false);
      assertEquals(Arrays.asList("xxxxx 2 1 2 0 0 0 0 1 0"), log.lines());
    }
  }

  @Test
  public void testSinglePartitionRetriesCountAttemptsWithoutExtraFrames() throws Exception {
    TezKVOutputCollector out = output(1);
    TestOperator operator = new TestOperator(out, TypeInfoFactory.stringTypeInfo);
    BytesColumnVector values = new BytesColumnVector();
    byte[] bytes = new byte[110000];
    values.setRef(0, bytes, 0, bytes.length);
    values.setRef(1, bytes, 0, bytes.length);
    try (SummaryCapture log = new SummaryCapture()) {
      assertTrue(operator.collectBatch(batch(2, values)));
      assertTrue(operator.collectBatch(batch(1, values)));
      operator.finish(false);
      // 220019 bytes overflow the 100 KiB and 200 KiB scratch buffers, then fit at 300 KiB.
      assertEquals(Arrays.asList("xxxxx 1 2 3 1 1 0 0 0 2"), log.lines());
    }
  }

  @Test
  public void testDisabledBatchingCountsEvaluatedBatchesAndFlushRowsBeforeOneSummary()
      throws Exception {
    TezKVOutputCollector out = output(3);
    TestOperator operator = new TestOperator(out, TypeInfoFactory.longTypeInfo);
    setPrivate(operator, "vectorShuffleBatchAllowed", false);
    operator.process(batch(0, new LongColumnVector()), -1);
    operator.process(batch(3, new LongColumnVector()), -1);
    VectorExpression filter = mock(VectorExpression.class);
    doAnswer(call -> {
      ((VectorizedRowBatch) call.getArgument(0)).size = 0;
      return null;
    }).when(filter).evaluate(any());
    operator.valueExpressions(filter);
    operator.process(batch(2, new LongColumnVector()), -1);
    assertEquals(1, counter(operator, "shuffleNonemptyBatches"));
    TopNHash topN = mock(TopNHash.class);
    doAnswer(call -> {
      operator.collect(new byte[0], new byte[0], 0);
      operator.collect(new byte[0], new byte[0], 0);
      return null;
    }).when(topN).flush();
    operator.topN(topN);
    try (SummaryCapture log = new SummaryCapture()) {
      operator.finish(false);
      // Reattach the collector to exercise the explicit summary guard on repeated close.
      operator.setOutputCollector(out);
      operator.finish(false);
      assertEquals(Arrays.asList("xxxxx 3 1 5 0 0 0 0 0 0"), log.lines());
    }
    verify(topN).flush();
    verify(out, times(5)).collect(any(), any());
  }

  @Test
  public void testAbortAndOrderedOutputsDoNotEmitSummary() throws Exception {
    try (SummaryCapture log = new SummaryCapture()) {
      for (int partitions : new int[] {-1, 0, 2}) {
        TestOperator operator = new TestOperator(output(partitions), TypeInfoFactory.longTypeInfo);
        if (partitions < 1) {
          assertFalse(operator.collectBatch(batch(2, new LongColumnVector())));
        }
        operator.finish(partitions > 0);
      }
      assertTrue(log.lines().isEmpty());
    }
  }

  private static TezKVOutputCollector output(int partitions) {
    TezKVOutputCollector out = mock(TezKVOutputCollector.class);
    when(out.getNumUnorderedPartitions()).thenReturn(partitions);
    when(out.getPartitionerType()).thenReturn(0);
    return out;
  }

  private static WriteValueBytes region(int capacity) {
    return new WriteValueBytes(new byte[capacity + 7], 7, capacity);
  }

  private static VectorizedRowBatch batch(int rows, ColumnVector... columns) {
    VectorizedRowBatch batch = new VectorizedRowBatch(columns.length);
    System.arraycopy(columns, 0, batch.cols, 0, columns.length);
    batch.size = rows;
    return batch;
  }

  private static void setPrivate(TestOperator operator, String name, Object value)
      throws Exception {
    Field field = VectorReduceSinkCommonOperator.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(operator, value);
  }

  private static long counter(TestOperator operator, String name) throws Exception {
    Field field = VectorReduceSinkCommonOperator.class.getDeclaredField(name);
    field.setAccessible(true);
    return field.getLong(operator);
  }

  private static class TestOperator extends VectorReduceSinkEmptyKeyOperator {
    private int[] partitions;

    TestOperator(TezKVOutputCollector out, TypeInfo... types) throws Exception {
      isEmptyKey = true;
      reduceSkipTag = true;
      reduceSinkValueColumnMap = new int[types.length];
      Arrays.setAll(reduceSinkValueColumnMap, column -> column);
      reduceSinkValueTypeInfos = types;
      valueLazyBinarySerializeWrite = new LazyBinarySerializeWrite(types.length);
      valueVectorSerializeRow = new VectorSerializeRow<>(valueLazyBinarySerializeWrite);
      valueVectorSerializeRow.init(types, reduceSinkValueColumnMap);
      valueOutput = new Output();
      valueVectorSerializeRow.setOutput(valueOutput);
      keyWritable = new HiveKey();
      valueBytesWritable = new BytesWritable();
      setOutputCollector(out);
      setPrivate(this, "vectorShuffleBatchAllowed", true);
      setPrivate(this, "vectorShuffleNumUnorderedPartitions", -1);
    }

    @Override
    protected void computeKeyHashCodes(VectorizedRowBatch batch, int[] hashes) {
      for (int row = 0; row < batch.size; row++) {
        int physical = batch.selectedInUse ? batch.selected[row] : row;
        hashes[physical] = partitions[physical];
      }
    }

    boolean collectBatch(VectorizedRowBatch batch) throws Exception {
      return tryCollectVectorShuffleBatch(batch, -1);
    }

    void valueExpressions(VectorExpression expression) {
      reduceSinkValueExpressions = new VectorExpression[] {expression};
    }

    void topN(TopNHash topN) {
      reducerHash = topN;
    }

    void finish(boolean abort) throws Exception {
      closeOp(abort);
    }
  }

  private static class SummaryCapture implements AutoCloseable {
    private final StringAppender appender = StringAppender.createStringAppender("%m%n");

    SummaryCapture() {
      appender.addToLogger(LOGGER, Level.ERROR);
      appender.start();
    }

    List<String> lines() {
      return Arrays.stream(appender.getOutput().split("\\R"))
          .filter(line -> line.startsWith("xxxxx ")).collect(Collectors.toList());
    }

    @Override
    public void close() {
      appender.removeFromLogger(LOGGER);
      appender.stop();
    }
  }
}
