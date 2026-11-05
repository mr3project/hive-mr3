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

package org.apache.hadoop.hive.ql.optimizer.physical;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ForwardOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationDesc;
import org.apache.hadoop.hive.ql.exec.vector.VectorAppMasterEventOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorFileSinkOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorFilterOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorGroupByOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorLimitOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorSelectOperator;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ConstantVectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFCount;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFCountMerge;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFCountStar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFSumDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFSumDecimal64;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFSumDecimal64ToDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFSumTimestamp;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFBloomFilter;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFBloomFilterMerge;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMinLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMinDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMaxLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMaxDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMaxDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMinDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMaxDecimal64;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMinDecimal64;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMinString;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMaxString;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMaxTimestamp;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMinTimestamp;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMaxIntervalDayTime;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFMinIntervalDayTime;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFSumLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFSumDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFAvgLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFAvgLongComplete;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFAvgDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFAvgDoubleComplete;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFAvgDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFAvgDecimalComplete;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFAvgDecimal64ToDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFAvgDecimal64ToDecimalComplete;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFAvgTimestamp;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFAvgTimestampComplete;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFAvgPartial2;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFAvgFinal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFAvgDecimalPartial2;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFAvgDecimalFinal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFVarLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFVarLongComplete;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFVarDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFVarDoubleComplete;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFVarDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFVarDecimalComplete;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFVarTimestamp;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFVarTimestampComplete;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFVarPartial2;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFVarFinal;
import org.apache.hadoop.hive.ql.exec.vector.reducesink.VectorReduceSinkEmptyKeyOperator;
import org.apache.hadoop.hive.ql.exec.vector.reducesink.VectorReduceSinkLongOperator;
import org.apache.hadoop.hive.ql.exec.vector.reducesink.VectorReduceSinkStringOperator;
import org.apache.hadoop.hive.ql.exec.vector.reducesink.VectorReduceSinkMultiKeyOperator;
import org.apache.hadoop.hive.ql.exec.vector.reducesink.VectorReduceSinkObjectHashOperator;
import org.apache.hadoop.hive.ql.plan.VectorGroupByDesc;

/** Compile-time lifetime policy for byte references borrowed from accumulated shuffle input. */
final class VectorShuffleInputByteReferences {

  // These operators finish reading the input or copy retained values before process returns.
  private static final Set<Class<?>> SYNCHRONOUS_OPERATORS = ImmutableSet.of(
      ForwardOperator.class,
      VectorSelectOperator.class,
      VectorFilterOperator.class,
      VectorLimitOperator.class,
      VectorFileSinkOperator.class,
      VectorReduceSinkOperator.class,
      VectorAppMasterEventOperator.class,
      VectorReduceSinkEmptyKeyOperator.class,
      VectorReduceSinkLongOperator.class,
      VectorReduceSinkStringOperator.class,
      VectorReduceSinkMultiKeyOperator.class,
      VectorReduceSinkObjectHashOperator.class);

  // String min/max and group keys copy values into aggregation-owned storage.
  private static final Set<Class<?>> SYNCHRONOUS_AGGREGATES = ImmutableSet.of(
      VectorUDAFCount.class,
      VectorUDAFCountMerge.class,
      VectorUDAFCountStar.class,
      VectorUDAFSumDecimal.class,
      VectorUDAFSumDecimal64.class,
      VectorUDAFSumDecimal64ToDecimal.class,
      VectorUDAFSumTimestamp.class,
      VectorUDAFBloomFilter.class,
      VectorUDAFMinLong.class,
      VectorUDAFMinDouble.class,
      VectorUDAFMaxLong.class,
      VectorUDAFMaxDouble.class,
      VectorUDAFMaxDecimal.class,
      VectorUDAFMinDecimal.class,
      VectorUDAFMaxDecimal64.class,
      VectorUDAFMinDecimal64.class,
      VectorUDAFMinString.class,
      VectorUDAFMaxString.class,
      VectorUDAFMaxTimestamp.class,
      VectorUDAFMinTimestamp.class,
      VectorUDAFMaxIntervalDayTime.class,
      VectorUDAFMinIntervalDayTime.class,
      VectorUDAFSumLong.class,
      VectorUDAFSumDouble.class,
      VectorUDAFAvgLong.class,
      VectorUDAFAvgLongComplete.class,
      VectorUDAFAvgDouble.class,
      VectorUDAFAvgDoubleComplete.class,
      VectorUDAFAvgDecimal.class,
      VectorUDAFAvgDecimalComplete.class,
      VectorUDAFAvgDecimal64ToDecimal.class,
      VectorUDAFAvgDecimal64ToDecimalComplete.class,
      VectorUDAFAvgTimestamp.class,
      VectorUDAFAvgTimestampComplete.class,
      VectorUDAFAvgPartial2.class,
      VectorUDAFAvgFinal.class,
      VectorUDAFAvgDecimalPartial2.class,
      VectorUDAFAvgDecimalFinal.class,
      VectorUDAFVarLong.class,
      VectorUDAFVarLongComplete.class,
      VectorUDAFVarDouble.class,
      VectorUDAFVarDoubleComplete.class,
      VectorUDAFVarDecimal.class,
      VectorUDAFVarDecimalComplete.class,
      VectorUDAFVarTimestamp.class,
      VectorUDAFVarTimestampComplete.class,
      VectorUDAFVarPartial2.class,
      VectorUDAFVarFinal.class);

  private VectorShuffleInputByteReferences() {
  }

  static boolean isAllowed(Operator<?> reducer) {
    Set<Operator<?>> visited = Collections.newSetFromMap(new IdentityHashMap<Operator<?>, Boolean>());
    Deque<Operator<?>> pending = new ArrayDeque<>();
    pending.add(reducer);
    while (!pending.isEmpty()) {
      Operator<?> operator = pending.removeFirst();
      if (!visited.add(operator)) {
        continue;
      }
      if (operator.getClass() == VectorGroupByOperator.class) {
        VectorGroupByDesc vectorDesc = (VectorGroupByDesc)
            ((VectorGroupByOperator) operator).getVectorDesc();
        for (VectorAggregationDesc aggregation : vectorDesc.getVecAggrDescs()) {
          if (!isSynchronous(aggregation)) {
            return false;
          }
        }
      } else if (!SYNCHRONOUS_OPERATORS.contains(operator.getClass())) {
        return false;
      }
      pending.addAll(operator.getChildOperators());
    }
    for (Operator<?> operator : visited) {
      if (operator.getClass() == VectorGroupByOperator.class) {
        VectorGroupByDesc vectorDesc = (VectorGroupByDesc)
            ((VectorGroupByOperator) operator).getVectorDesc();
        vectorDesc.setInputByteReferencesAllowed(true);
      }
    }
    return true;
  }

  private static boolean isSynchronous(VectorAggregationDesc aggregation) {
    if (aggregation.getVecAggrClass() == VectorUDAFBloomFilterMerge.class) {
      // Match both bloom-filter merge constructors, including the default thread count.
      List<ConstantVectorExpression> constants = aggregation.getConstants();
      int numThreads = constants == null || constants.isEmpty()
          ? HiveConf.ConfVars.TEZ_BLOOM_FILTER_MERGE_THREADS.defaultIntVal
          : (int) constants.get(0).getLongValue();
      return numThreads == 0;
    }
    return SYNCHRONOUS_AGGREGATES.contains(aggregation.getVecAggrClass());
  }
}
