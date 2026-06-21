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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorSerializeRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.VectorDesc;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hive.common.util.Murmur3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * This class is uniform hash (common) operator class for native vectorized reduce sink.
 * There are variation operators for Long, String, and MultiKey.  And, a special case operator
 * for no key (VectorReduceSinkEmptyKeyOperator).
 */
public abstract class VectorReduceSinkUniformHashOperator extends VectorReduceSinkCommonOperator {

  private static final long serialVersionUID = 1L;
  private static final String CLASS_NAME = VectorReduceSinkUniformHashOperator.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  // The above members are initialized by the constructor and must not be
  // transient.
  //---------------------------------------------------------------------------

  private transient VectorUniformKeyHashCodeComputer keyHashCodeComputer;
  private transient Output keyOutput;
  private transient VectorSerializeRow<BinarySortableSerializeWrite> keyVectorSerializeRow;

  /** Kryo ctor. */
  protected VectorReduceSinkUniformHashOperator() {
    super();
  }

  public VectorReduceSinkUniformHashOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  public VectorReduceSinkUniformHashOperator(CompilationOpContext ctx, OperatorDesc conf,
      VectorizationContext vContext, VectorDesc vectorDesc) throws HiveException {
    super(ctx, conf, vContext, vectorDesc);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);

    Preconditions.checkState(!isEmptyKey);
    try {
      keyOutput = new Output();
      keyVectorSerializeRow = new VectorSerializeRow<>(keyBinarySortableSerializeWrite);
      keyVectorSerializeRow.init(reduceSinkKeyTypeInfos, reduceSinkKeyColumnMap);

    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  protected void setKeyHashCodeComputer(VectorUniformKeyHashCodeComputer keyHashCodeComputer) {
    this.keyHashCodeComputer = keyHashCodeComputer;
  }

  @Override
  protected void computeKeyHashCodes(VectorizedRowBatch batch, int[] hashCodes) throws IOException {
    keyHashCodeComputer.computeHashCodes(batch, hashCodes);
  }

  @Override
  public void process(Object row, int tag) throws HiveException {

    try {
      VectorizedRowBatch batch = (VectorizedRowBatch) row;

      batchCounter++;

      if (batch.size == 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(CLASS_NAME + " batch #" + batchCounter + " empty");
        }
        return;
      }

      // Perform any key expressions.  Results will go into scratch columns.
      if (reduceSinkKeyExpressions != null) {
        for (VectorExpression ve : reduceSinkKeyExpressions) {
          ve.evaluate(batch);
        }
      }

      // Perform any value expressions.  Results will go into scratch columns.
      if (reduceSinkValueExpressions != null) {
        for (VectorExpression ve : reduceSinkValueExpressions) {
          ve.evaluate(batch);
        }
      }

      if (tryCollectVectorShuffleBatch(batch, tag)) {
        return;
      }

      boolean selectedInUse = batch.selectedInUse;
      int[] selected = batch.selected;
      final int size = batch.size;
      for (int logical = 0; logical < size; logical++) {
        final int batchIndex = selectedInUse ? selected[logical] : logical;

        keyVectorSerializeRow.setOutput(keyOutput);
        keyVectorSerializeRow.serializeWrite(batch, batchIndex);
        final int keyLength = keyOutput.getLength();
        setVectorShuffleRowKey(keyOutput.getData(), 0, keyLength,
            Murmur3.hash32(keyOutput.getData(), 0, keyLength, 0), tag);

        if (!isEmptyValue) {
          valueLazyBinarySerializeWrite.reset();
          valueVectorSerializeRow.serializeWrite(batch, batchIndex);
          valueBytesWritable.set(valueOutput.getData(), 0, valueOutput.getLength());
          collect(keyWritable, valueBytesWritable);
        } else {
          collect(keyWritable, valueBytesWritable);
        }
      }

    } catch (Exception e) {
      throw new HiveException(e);
    }
  }
}
