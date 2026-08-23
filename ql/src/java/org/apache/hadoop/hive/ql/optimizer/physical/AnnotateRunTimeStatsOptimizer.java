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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration.AnalyzeState;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TezWork;

public class AnnotateRunTimeStatsOptimizer implements PhysicalPlanResolver {
  private static final Logger LOG = LoggerFactory.getLogger(AnnotateRunTimeStatsOptimizer.class);

  private class AnnotateRunTimeStatsDispatcher implements SemanticDispatcher {

    private final PhysicalContext physicalContext;

    public AnnotateRunTimeStatsDispatcher(PhysicalContext context, Map<SemanticRule, SemanticNodeProcessor> rules) {
      super();
      physicalContext = context;
    }

    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs)
        throws SemanticException {
      Task<?> currTask = (Task<?>) nd;
      // Assume HIVE_EXECUTION_ENGINE is set to tez, so only TezTask carries executable operator work.
      if (!(currTask instanceof TezTask)) {
        return null;
      }

      Set<Operator<? extends OperatorDesc>> ops = new HashSet<>();
      TezWork work = ((TezTask) currTask).getWork();
      for (BaseWork w : work.getAllWork()) {
        ops.addAll(w.getAllOperators());
      }
      setOrAnnotateStats(ops, physicalContext.getParseContext());
      return null;
    }

  }

  public static void setOrAnnotateStats(Set<Operator<? extends OperatorDesc>> ops, ParseContext pctx)
      throws SemanticException {
    AnalyzeState analyzeState = pctx.getContext().getExplainAnalyze();
    if (analyzeState == AnalyzeState.RUNNING) {
      // MR3 operators already publish their row counts as DAG counters; no plan setup is required.
      return;
    }
    if (analyzeState != AnalyzeState.ANALYZING) {
      throw new SemanticException("Unexpected stats in AnnotateWithRunTimeStatistics.");
    }
    for (Operator<? extends OperatorDesc> op : ops) {
      annotateRuntimeStats(op, pctx);
    }
  }

  private static void annotateRuntimeStats(Operator<? extends OperatorDesc> op, ParseContext pctx) {
    Long runTimeNumRows = pctx.getContext().getExplainConfig().getOpIdToRuntimeNumRows()
        .get(op.getOperatorId());
    if (op.getConf() != null && op.getConf().getStatistics() != null && runTimeNumRows != null) {
      LOG.info("annotateRuntimeStats for " + op.getOperatorId());
      op.getConf().getStatistics().setRunTimeNumRows(runTimeNumRows);
    } else {
      LOG.debug("skip annotateRuntimeStats for " + op.getOperatorId());
    }
  }

  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
    SemanticDispatcher disp = new AnnotateRunTimeStatsDispatcher(pctx, opRules);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getRootTasks());
    ogw.startWalking(topNodes, null);
    return pctx;
  }

  public void resolve(Set<Operator<?>> opSet, ParseContext pctx) throws SemanticException {
    Set<Operator<?>> ops = OperatorUtils.getAllOperatorsForSimpleFetch(opSet);
    setOrAnnotateStats(ops, pctx);
  }


}
