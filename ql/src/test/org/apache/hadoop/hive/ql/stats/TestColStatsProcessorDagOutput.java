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
package org.apache.hadoop.hive.ql.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.google.protobuf.ByteString;
import java.util.Collections;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.plan.ColumnStatsDesc;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.junit.Test;

public class TestColStatsProcessorDagOutput {

  @Test
  public void testReadsRowsDirectlyFromDagOutput() throws Exception {
    HiveConf conf = new HiveConf();
    TableDesc tableDesc = PlanUtils.getDefaultQueryOutputTableDesc(
        "value", "string", "SequenceFile", LazySimpleSerDe.class);
    FetchWork fetchWork = new FetchWork(new Path("file:/unused"), tableDesc, -1);
    ColumnStatsDesc columnStatsDesc = new ColumnStatsDesc(
        "default.test", Collections.emptyList(), Collections.emptyList(), true, 0, fetchWork);
    Context.InternalDagOutput dagOutput = new Context.InternalDagOutput(
        Collections.singletonList(ByteString.copyFromUtf8("hello\n")), '\n');
    ColStatsProcessor processor = new ColStatsProcessor(columnStatsDesc, conf, dagOutput);

    processor.initialize(new CompilationOpContext());
    InspectableObject row = processor.getNextPackedRow();
    assertNotNull(row);
    assertEquals(ObjectInspector.Category.STRUCT, row.oi.getCategory());
    assertNull(processor.getNextPackedRow());
  }
}
