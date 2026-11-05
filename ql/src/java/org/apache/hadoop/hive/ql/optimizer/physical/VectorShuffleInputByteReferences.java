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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ForwardOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MergeJoinWork;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty.EdgeType;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationDesc;
import org.apache.hadoop.hive.ql.exec.vector.VectorAppMasterEventOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorFileSinkOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorFilterOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorGroupByOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorLimitOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinCommonOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinAntiJoinLongOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinAntiJoinMultiKeyOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinFullOuterMultiKeyOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinInnerBigOnlyLongOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinInnerBigOnlyMultiKeyOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinInnerLongOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinInnerMultiKeyOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinLeftSemiLongOperator;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.VectorMapJoinOuterMultiKeyOperator;
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
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Compile-time lifetime policy for byte references borrowed from accumulated shuffle input. */
public final class VectorShuffleInputByteReferences implements PhysicalPlanResolver {

  private static final Logger LOG = LoggerFactory.getLogger(VectorShuffleInputByteReferences.class);

  // These operators finish reading the input or copy retained values before process returns.
  private static final Set<Class<?>> OPERATORS_ALLOWING_CLEANUP = ImmutableSet.of(
      ForwardOperator.class,
      VectorSelectOperator.class,
      VectorFilterOperator.class,
      VectorLimitOperator.class,
      VectorMapJoinAntiJoinLongOperator.class,
      VectorMapJoinAntiJoinMultiKeyOperator.class,
      VectorMapJoinFullOuterMultiKeyOperator.class,
      VectorMapJoinInnerBigOnlyLongOperator.class,
      VectorMapJoinInnerBigOnlyMultiKeyOperator.class,
      VectorMapJoinInnerLongOperator.class,
      VectorMapJoinInnerMultiKeyOperator.class,
      VectorMapJoinLeftSemiLongOperator.class,
      VectorMapJoinOperator.class,
      VectorMapJoinOuterMultiKeyOperator.class,
      VectorFileSinkOperator.class,
      VectorReduceSinkOperator.class,
      VectorAppMasterEventOperator.class,
      VectorReduceSinkEmptyKeyOperator.class,
      VectorReduceSinkLongOperator.class,
      VectorReduceSinkStringOperator.class,
      VectorReduceSinkMultiKeyOperator.class,
      VectorReduceSinkObjectHashOperator.class);

  // String min/max and group keys copy values into aggregation-owned storage.
  private static final Set<Class<?>> AGGREGATES_ALLOWING_CLEANUP = ImmutableSet.of(
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

  @Override
  public PhysicalContext resolve(PhysicalContext context) throws SemanticException {
    SemanticDispatcher dispatcher = (node, stack, outputs) -> {
      Task<?> task = (Task<?>) node;
      if (task instanceof TezTask) {
        TezWork tezWork = ((TezTask) task).getWork();
        for (BaseWork work : tezWork.getAllWork()) {
          if (work instanceof ReduceWork) {
            classify(context.getConf(), tezWork, (ReduceWork) work);
            report(tezWork, (ReduceWork) work);
          } else if (work instanceof MergeJoinWork) {
            MergeJoinWork mergeWork = (MergeJoinWork) work;
            String reason = "merge-join vertex is not vectorized";
            if (mergeWork.getMainWork() instanceof ReduceWork) {
              ((ReduceWork) mergeWork.getMainWork()).setShuffleInputByteDecision(3, reason);
              LOG.error("xxxxxx 3 dag={} vertex={} {}", tezWork.getDagId(), work.getName(), reason);
            }
          }
        }
      }
      return null;
    };
    List<Node> roots = new ArrayList<>(context.getRootTasks());
    new TaskGraphWalker(dispatcher).startWalking(roots, null);
    return context;
  }

  private static void report(TezWork tezWork, ReduceWork work) {
    LOG.error("xxxxxx {} dag={} vertex={} {}", work.getShuffleInputByteDecision(),
        tezWork.getDagId(), work.getName(), work.getShuffleInputByteDecisionReason());
  }

  private static void classify(HiveConf conf, TezWork tezWork, ReduceWork work) {
    if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_VECTORIZATION_REDUCE_SHUFFLE_INPUT_ACCUMULATE)) {
      work.setShuffleInputByteDecision(3, "shuffle input accumulation is disabled");
      return;
    }
    if (!work.getVectorMode()) {
      work.setShuffleInputByteDecision(3, "reduce input is not vectorized");
      return;
    }
    int inputTag = work.getTagToValueDesc().size() > 1 ? work.getTag() : 0;
    String inputName = work.getTagToInput().get(inputTag);
    boolean unordered = false;
    for (BaseWork parent : tezWork.getParents(work)) {
      if (parent.getName().equals(inputName)) {
        EdgeType edge = tezWork.getEdgeType(parent, work);
        unordered = edge == EdgeType.BROADCAST_EDGE || edge == EdgeType.CUSTOM_EDGE
            || edge == EdgeType.CUSTOM_SIMPLE_EDGE || edge == EdgeType.ONE_TO_ONE_EDGE
            || edge == EdgeType.XPROD_EDGE;
      }
    }
    if (!unordered) {
      work.setShuffleInputByteDecision(3, "main input does not use unordered shuffle");
      return;
    }
    if (work.getUsesVectorUDFAdaptor()) {
      work.setShuffleInputByteDecision(2, "unverified vector UDF adaptor");
      return;
    }

    Set<Operator<?>> visited = Collections.newSetFromMap(new IdentityHashMap<Operator<?>, Boolean>());
    Deque<Operator<?>> pending = new ArrayDeque<>();
    pending.add(work.getReducer());
    while (!pending.isEmpty()) {
      Operator<?> operator = pending.removeFirst();
      if (!visited.add(operator)) {
        continue;
      }
      if (operator.getClass() == VectorGroupByOperator.class) {
        VectorGroupByDesc vectorDesc = (VectorGroupByDesc)
            ((VectorGroupByOperator) operator).getVectorDesc();
        for (VectorAggregationDesc aggregation : vectorDesc.getVecAggrDescs()) {
          String reason = aggregationRejection(aggregation);
          if (reason != null) {
            work.setShuffleInputByteDecision(2, operator.getOperatorId() + " " + reason);
            return;
          }
        }
      } else if (!OPERATORS_ALLOWING_CLEANUP.contains(operator.getClass())) {
        work.setShuffleInputByteDecision(2, "unsupported operator " + operator.getOperatorId()
            + " (" + operator.getClass().getSimpleName() + ")");
        return;
      }
      pending.addAll(operator.getChildOperators());
    }
    for (Operator<?> operator : visited) {
      if (operator.getClass() == VectorGroupByOperator.class) {
        VectorGroupByDesc vectorDesc = (VectorGroupByDesc)
            ((VectorGroupByOperator) operator).getVectorDesc();
        vectorDesc.setInputByteReferencesAllowed(true);
      } else if (operator instanceof VectorMapJoinCommonOperator
          || operator.getClass() == VectorMapJoinOperator.class) {
        VectorMapJoinDesc vectorDesc = (VectorMapJoinDesc)
            ((VectorizationOperator) operator).getVectorDesc();
        vectorDesc.setInputByteReferencesAllowed(true);
      }
    }
    work.setShuffleInputByteDecision(1, "all downstream operators and aggregators allow cleanup");
  }

  private static String aggregationRejection(VectorAggregationDesc aggregation) {
    if (aggregation.getVecAggrClass() == VectorUDAFBloomFilterMerge.class) {
      // Match both bloom-filter merge constructors, including the default thread count.
      List<ConstantVectorExpression> constants = aggregation.getConstants();
      int numThreads = constants == null || constants.isEmpty()
          ? HiveConf.ConfVars.TEZ_BLOOM_FILTER_MERGE_THREADS.defaultIntVal
          : (int) constants.get(0).getLongValue();
      return numThreads == 0 ? null : "bloom-filter merge threads=" + numThreads;
    }
    return AGGREGATES_ALLOWING_CLEANUP.contains(aggregation.getVecAggrClass()) ? null
        : "unsupported aggregator " + aggregation.getVecAggrClass().getSimpleName();
  }
}
