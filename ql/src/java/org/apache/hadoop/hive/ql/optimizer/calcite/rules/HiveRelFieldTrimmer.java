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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.CorrelationReferenceFinder;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveMultiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.parse.ColumnAccessInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class HiveRelFieldTrimmer extends RelFieldTrimmer {

  protected static final Logger LOG = LoggerFactory.getLogger(HiveRelFieldTrimmer.class);

  private ColumnAccessInfo columnAccessInfo;
  private Map<HiveProject, Table> viewProjectToTableSchema;
  private final RelBuilder relBuilder;
  private final boolean fetchStats;

  public HiveRelFieldTrimmer(SqlValidator validator, RelBuilder relBuilder) {
    this(validator, relBuilder, false);
  }

  public HiveRelFieldTrimmer(SqlValidator validator, RelBuilder relBuilder,
      ColumnAccessInfo columnAccessInfo, Map<HiveProject, Table> viewToTableSchema) {
    this(validator, relBuilder, false);
    this.columnAccessInfo = columnAccessInfo;
    this.viewProjectToTableSchema = viewToTableSchema;
  }

  public HiveRelFieldTrimmer(SqlValidator validator, RelBuilder relBuilder, boolean fetchStats) {
    super(validator, relBuilder);
    this.relBuilder = relBuilder;
    this.fetchStats = fetchStats;
  }

  /**
   * Trims the fields of an input relational expression.
   *
   * @param rel        Relational expression
   * @param input      Input relational expression, whose fields to trim
   * @param fieldsUsed Bitmap of fields needed by the consumer
   * @return New relational expression and its field mapping
   */
  protected TrimResult trimChild(
      RelNode rel,
      RelNode input,
      final ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final ImmutableBitSet.Builder fieldsUsedBuilder = fieldsUsed.rebuild();

    // Correlating variables are a means for other relational expressions to use
    // fields.
    for (final CorrelationId correlation : rel.getVariablesSet()) {
      rel.accept(
          new CorrelationReferenceFinder() {
            protected RexNode handle(RexFieldAccess fieldAccess) {
              final RexCorrelVariable v =
                  (RexCorrelVariable) fieldAccess.getReferenceExpr();
              if (v.id.equals(correlation)) {
                fieldsUsedBuilder.set(fieldAccess.getField().getIndex());
              }
              return fieldAccess;
            }
          });
    }

    return dispatchTrimFields(input, fieldsUsedBuilder.build(), extraFields);
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveMultiJoin}.
   */
  public TrimResult trimFields(
      HiveMultiJoin join,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final int fieldCount = join.getRowType().getFieldCount();
    final RexNode conditionExpr = join.getCondition();
    final List<RexNode> joinFilters = join.getJoinFilters();

    // Add in fields used in the condition.
    final Set<RelDataTypeField> combinedInputExtraFields =
        new LinkedHashSet<RelDataTypeField>(extraFields);
    RelOptUtil.InputFinder inputFinder =
        new RelOptUtil.InputFinder(combinedInputExtraFields);
    inputFinder.inputBitSet.addAll(fieldsUsed);
    conditionExpr.accept(inputFinder);
    final ImmutableBitSet fieldsUsedPlus = inputFinder.inputBitSet.build();

    int inputStartPos = 0;
    int changeCount = 0;
    int newFieldCount = 0;
    List<RelNode> newInputs = new ArrayList<RelNode>();
    List<Mapping> inputMappings = new ArrayList<Mapping>();
    for (RelNode input : join.getInputs()) {
      final RelDataType inputRowType = input.getRowType();
      final int inputFieldCount = inputRowType.getFieldCount();

      // Compute required mapping.
      ImmutableBitSet.Builder inputFieldsUsed = ImmutableBitSet.builder();
      for (int bit : fieldsUsedPlus) {
        if (bit >= inputStartPos && bit < inputStartPos + inputFieldCount) {
          inputFieldsUsed.set(bit - inputStartPos);
        }
      }

      Set<RelDataTypeField> inputExtraFields =
              Collections.<RelDataTypeField>emptySet();
      TrimResult trimResult =
          trimChild(join, input, inputFieldsUsed.build(), inputExtraFields);
      newInputs.add(trimResult.left);
      if (trimResult.left != input) {
        ++changeCount;
      }

      final Mapping inputMapping = trimResult.right;
      inputMappings.add(inputMapping);

      // Move offset to point to start of next input.
      inputStartPos += inputFieldCount;
      newFieldCount += inputMapping.getTargetCount();
    }

    Mapping mapping =
        Mappings.create(
            MappingType.INVERSE_SURJECTION,
            fieldCount,
            newFieldCount);
    int offset = 0;
    int newOffset = 0;
    for (int i = 0; i < inputMappings.size(); i++) {
      Mapping inputMapping = inputMappings.get(i);
      for (IntPair pair : inputMapping) {
        mapping.set(pair.source + offset, pair.target + newOffset);
      }
      offset += inputMapping.getSourceCount();
      newOffset += inputMapping.getTargetCount();
    }

    if (changeCount == 0
        && mapping.isIdentity()) {
      return new TrimResult(join, Mappings.createIdentity(fieldCount));
    }

    // Build new join.
    final RexVisitor<RexNode> shuttle = new RexPermuteInputsShuttle(
            mapping, newInputs.toArray(new RelNode[newInputs.size()]));
    RexNode newConditionExpr = conditionExpr.accept(shuttle);

    List<RexNode> newJoinFilters = Lists.newArrayList();

    for (RexNode joinFilter : joinFilters) {
      newJoinFilters.add(joinFilter.accept(shuttle));
    }

    final RelDataType newRowType = RelOptUtil.permute(join.getCluster().getTypeFactory(),
            join.getRowType(), mapping);
    final RelNode newJoin = new HiveMultiJoin(join.getCluster(),
            newInputs,
            newConditionExpr,
            newRowType,
            join.getJoinInputs(),
            join.getJoinTypes(),
            newJoinFilters);

    return new TrimResult(newJoin, mapping);
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.calcite.rel.logical.LogicalProject}.
   */
  public TrimResult trimFields(Project project, ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    // set columnAccessInfo for ViewColumnAuthorization
    for (Ord<RexNode> ord : Ord.zip(project.getProjects())) {
      if (fieldsUsed.get(ord.i)) {
        if (this.columnAccessInfo != null && this.viewProjectToTableSchema != null
            && this.viewProjectToTableSchema.containsKey(project)) {
          Table tab = this.viewProjectToTableSchema.get(project);
          this.columnAccessInfo.add(tab.getCompleteName(), tab.getAllCols().get(ord.i).getName());
        }
      }
    }
    return super.trimFields(project, fieldsUsed, extraFields);
  }

  @Override
  public TrimResult trimFields(TableScan tableAccessRel, ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final TrimResult result = super.trimFields(tableAccessRel, fieldsUsed, extraFields);
    if (fetchStats) {
      fetchColStats(result.getKey(), tableAccessRel, fieldsUsed, extraFields);
    }
    return result;
  }

  private void fetchColStats(RelNode key, TableScan tableAccessRel, ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final List<Integer> iRefSet = Lists.newArrayList();
    if (key instanceof Project) {
      final Project project = (Project) key;
      for (RexNode rx : project.getChildExps()) {
        iRefSet.addAll(HiveCalciteUtil.getInputRefs(rx));
      }
    } else {
      final int fieldCount = tableAccessRel.getRowType().getFieldCount();
      if (fieldsUsed.equals(ImmutableBitSet.range(fieldCount)) && extraFields.isEmpty()) {
        // get all cols
        iRefSet.addAll(ImmutableBitSet.range(fieldCount).asList());
      }
    }

    //Remove any virtual cols
    if (tableAccessRel instanceof HiveTableScan) {
      iRefSet.removeAll(((HiveTableScan)tableAccessRel).getVirtualCols());
    }

    if (!iRefSet.isEmpty()) {
      final RelOptTable table = tableAccessRel.getTable();
      if (table instanceof RelOptHiveTable) {
        ((RelOptHiveTable) table).getColStat(iRefSet, true);
        LOG.debug("Got col stats for {} in {}", iRefSet,
            tableAccessRel.getTable().getQualifiedName());
      }
    }
  }

  protected TrimResult result(RelNode r, final Mapping mapping) {
    return new TrimResult(r, mapping);
  }
}
