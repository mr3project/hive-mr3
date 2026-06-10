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
package org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast;

import java.util.Arrays;
import java.util.Properties;

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Assert;
import org.junit.Test;

public class TestVectorMapJoinFastHashTableLoader {

  @Test
  public void testVectorBatchReaderSerdeResetsRowSerializers() throws Exception {
    MapJoinDesc desc = new MapJoinDesc();
    desc.setNoOuterJoin(true);
    desc.setKeyTblDesc(tableDesc("bigint"));
    desc.setValueTblDescs(Arrays.asList(null, tableDesc("bigint")));

    VectorMapJoinFastHashTableLoader.VectorBatchReaderSerde serde =
        new VectorMapJoinFastHashTableLoader.VectorBatchReaderSerde(desc, 1);
    VectorizedRowBatch batch = serde.getBatch();
    ((LongColumnVector) batch.cols[0]).vector[0] = 11;
    ((LongColumnVector) batch.cols[0]).vector[1] = 22;
    ((LongColumnVector) batch.cols[1]).vector[0] = 33;
    ((LongColumnVector) batch.cols[1]).vector[1] = 44;
    batch.size = 2;

    byte[] firstKey = copy(serde.serializeKey(0));
    byte[] secondKey = copy(serde.serializeKey(1));
    byte[] firstValue = copy(serde.serializeValue(0));
    byte[] secondValue = copy(serde.serializeValue(1));

    Assert.assertFalse(Arrays.equals(firstKey, secondKey));
    Assert.assertFalse(Arrays.equals(firstValue, secondValue));
  }

  private static TableDesc tableDesc(String types) {
    Properties properties = new Properties();
    properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, types);
    TableDesc tableDesc = new TableDesc();
    tableDesc.setProperties(properties);
    return tableDesc;
  }

  private static byte[] copy(BytesWritable writable) {
    return Arrays.copyOfRange(writable.getBytesRaw(), writable.getOffset(),
        writable.getOffset() + writable.getLength());
  }
}
