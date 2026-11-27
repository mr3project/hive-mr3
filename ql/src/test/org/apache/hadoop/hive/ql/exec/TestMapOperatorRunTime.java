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

package org.apache.hadoop.hive.ql.exec;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestMapOperatorRunTime {
  private final CompilationOpContext compilationContext = new CompilationOpContext();

  @Test
  public void requiresJoinAndGroupByOnTheSamePath() {
    TestOperator join = operator(OperatorType.MAPJOIN);
    TestOperator groupBy = operator(OperatorType.GROUPBY);
    TestOperator scan = operator(OperatorType.TABLESCAN, join, groupBy);

    assertNull(MapOperatorRunTime.create(List.of(map(scan))));
  }

  @Test
  public void chargesReaderAndNestedProcessingOnceAndStopsBeforeClose() throws HiveException {
    TestOperator groupBy = operator(OperatorType.GROUPBY);
    TestOperator join = operator(OperatorType.MAPJOIN, groupBy);
    TestOperator scan = operator(OperatorType.TABLESCAN, join);
    MapOperator map = map(scan);
    MapOperatorRunTime timing = MapOperatorRunTime.create(List.of(map));
    MapredContext context = MapredContext.init(true, new JobConf(false));
    try {
      context.setMapOperatorRunTime(timing);
      long simulatedRead = TimeUnit.MILLISECONDS.toNanos(20);
      timing.finishRead(map, System.nanoTime() - simulatedRead, timing.nestedNanos());
      scan.processWithRunTime(NullWritable.get(), 0);
      long inclusive = timing.nestedNanos();

      timing.finishRun();
      long scanNanos = MapOperatorRunTime.completedNanos(scan);
      long joinNanos = MapOperatorRunTime.completedNanos(join);
      long groupByNanos = MapOperatorRunTime.completedNanos(groupBy);
      assertTrue(scanNanos >= simulatedRead);
      assertTrue(groupByNanos > 0);
      assertEquals(inclusive, scanNanos + joinNanos + groupByNanos);

      scan.processWithRunTime(NullWritable.get(), 0);
      assertEquals(Long.valueOf(scanNanos), MapOperatorRunTime.completedNanos(scan));
      assertEquals(Long.valueOf(joinNanos), MapOperatorRunTime.completedNanos(join));
      assertEquals(Long.valueOf(groupByNanos), MapOperatorRunTime.completedNanos(groupBy));
    } finally {
      MapredContext.close();
    }
  }

  @Test
  public void chargesSharedReaderToTheMapOperator() {
    TestOperator join = operator(OperatorType.MAPJOIN);
    TestOperator groupBy = operator(OperatorType.GROUPBY, join);
    TestOperator scan = operator(OperatorType.TABLESCAN, groupBy);
    TestOperator otherScan = operator(OperatorType.TABLESCAN);
    MapOperator map = map(scan, otherScan);
    MapOperatorRunTime timing = MapOperatorRunTime.create(List.of(map));
    MapredContext context = MapredContext.init(true, new JobConf(false));
    try {
      context.setMapOperatorRunTime(timing);
      timing.finishRead(map, System.nanoTime(), timing.nestedNanos());
      timing.finishRun();

      assertEquals(Long.valueOf(timing.nestedNanos()), MapOperatorRunTime.completedNanos(map));
      assertEquals(Long.valueOf(0), MapOperatorRunTime.completedNanos(scan));
      assertNull(MapOperatorRunTime.completedNanos(otherScan));
    } finally {
      MapredContext.close();
    }
  }

  private MapOperator map(TestOperator... children) {
    MapOperator map = new MapOperator(compilationContext);
    map.setChildOperators(List.of(children));
    return map;
  }

  private TestOperator operator(OperatorType type, TestOperator... children) {
    TestOperator operator = new TestOperator(compilationContext, type);
    operator.setChildOperators(List.of(children));
    return operator;
  }

  private static final class TestOperator extends Operator<OperatorDesc> {
    private final OperatorType type;

    private TestOperator(CompilationOpContext context, OperatorType type) {
      super(context);
      this.type = type;
    }

    @Override
    public void process(Object row, int tag) throws HiveException {
      for (Operator<?> child : getChildOperators()) {
        child.processWithRunTime(row, tag);
      }
    }

    @Override
    public String getName() {
      return "TEST";
    }

    @Override
    public OperatorType getType() {
      return type;
    }
  }
}
