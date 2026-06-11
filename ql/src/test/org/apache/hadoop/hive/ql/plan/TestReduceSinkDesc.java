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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Test;

public class TestReduceSinkDesc {
  @Test
  public void testVectorBatchWriteEnabledIsCloned() {
    ReduceSinkDesc desc = new ReduceSinkDesc(new ArrayList<>(), 0, new ArrayList<>(),
        new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), -1, new ArrayList<>(), 1,
        new TableDesc(), new TableDesc(), null);
    desc.setColumnExprMap(new HashMap<>());
    assertTrue(desc.isVectorBatchWriteEnabled());

    desc.setVectorBatchWriteEnabled(false);
    ReduceSinkDesc clone = (ReduceSinkDesc) desc.clone();

    assertFalse(clone.isVectorBatchWriteEnabled());
  }
}
