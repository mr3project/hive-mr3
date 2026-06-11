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
package org.apache.hadoop.hive.ql.plan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.junit.Test;

public class TestMergeJoinWork {
  @Test
  public void testMergedReduceWorkDisablesVectorBatchWrites() {
    MapWork mainWork = new MapWork();
    mainWork.setName("main");
    ReduceWork mergedWork = new ReduceWork();
    ReduceSinkDesc reduceSinkDesc = new ReduceSinkDesc();
    ReduceSinkOperator reduceSink = mock(ReduceSinkOperator.class);
    when(reduceSink.getConf()).thenReturn(reduceSinkDesc);
    Map<Operator<?>, BaseWork> leafOperatorToFollowingWork = new IdentityHashMap<>();
    leafOperatorToFollowingWork.put(reduceSink, mergedWork);

    MergeJoinWork mergeJoinWork = new MergeJoinWork();
    mergeJoinWork.addMergedWork(mainWork, null, Collections.emptyMap());
    mergeJoinWork.addMergedWork(null, mergedWork, leafOperatorToFollowingWork);

    assertVectorBatchWritesDisabled(reduceSinkDesc, mainWork.getName());
  }

  @Test
  public void testMainReduceWorkDisablesVectorBatchWrites() {
    ReduceWork mainWork = new ReduceWork();
    mainWork.setName("main");
    ReduceSinkDesc reduceSinkDesc = new ReduceSinkDesc();
    ReduceSinkOperator reduceSink = mock(ReduceSinkOperator.class);
    when(reduceSink.getConf()).thenReturn(reduceSinkDesc);
    Map<Operator<?>, BaseWork> leafOperatorToFollowingWork = new IdentityHashMap<>();
    leafOperatorToFollowingWork.put(reduceSink, mainWork);

    MergeJoinWork mergeJoinWork = new MergeJoinWork();
    mergeJoinWork.addMergedWork(mainWork, null, leafOperatorToFollowingWork);

    assertVectorBatchWritesDisabled(reduceSinkDesc, mainWork.getName());
  }

  private void assertVectorBatchWritesDisabled(ReduceSinkDesc reduceSinkDesc, String outputName) {
    assertFalse(reduceSinkDesc.isVectorBatchWriteEnabled());
    assertEquals(outputName, reduceSinkDesc.getOutputName());
  }
}
