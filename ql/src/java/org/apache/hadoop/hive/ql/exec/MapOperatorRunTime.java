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

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.io.Writable;

/** Exclusive wall time on the map task thread, from the first read through the end of the run loop. */
public final class MapOperatorRunTime {
  private static final int MAP_JOIN = 1;
  private static final int GROUP_BY = 2;
  private static final int REQUIRED_TYPES = MAP_JOIN | GROUP_BY;

  private static final class Total {
    private long nanos;
  }

  private final Map<Operator<?>, Total> totals = new IdentityHashMap<>();
  private final Map<AbstractMapOperator, Operator<?>> readerOwners = new IdentityHashMap<>();
  private boolean running = true;
  private long nestedNanos;

  private MapOperatorRunTime(List<AbstractMapOperator> mapRoots) {
    Map<Operator<?>, Boolean[]> matches = new IdentityHashMap<>();
    for (AbstractMapOperator root : mapRoots) {
      List<Operator<? extends OperatorDesc>> children = root.getChildOperators();
      if (children == null) {
        continue;
      }
      for (Operator<?> child : children) {
        if (child.getType() == OperatorType.TABLESCAN && selectPath(child, 0, matches)) {
          totals.computeIfAbsent(root, ignored -> new Total());
        }
      }
      if (totals.containsKey(root)) {
        // A row-mode reader can feed multiple aliases. Charge that shared work once to the map root.
        readerOwners.put(root, children.size() == 1 ? children.get(0) : root);
      }
    }
  }

  public static MapOperatorRunTime create(List<AbstractMapOperator> mapRoots) {
    MapOperatorRunTime timing = new MapOperatorRunTime(mapRoots);
    return timing.totals.isEmpty() ? null : timing;
  }

  private boolean selectPath(Operator<?> operator, int types, Map<Operator<?>, Boolean[]> matches) {
    OperatorType type = operator.getType();
    if (type == OperatorType.MAPJOIN) {
      types |= MAP_JOIN;
    } else if (type == OperatorType.GROUPBY) {
      types |= GROUP_BY;
    }

    Boolean[] matchesByType = matches.computeIfAbsent(operator, ignored -> new Boolean[REQUIRED_TYPES + 1]);
    if (matchesByType[types] != null) {
      return matchesByType[types];
    }
    List<Operator<? extends OperatorDesc>> children = operator.getChildOperators();
    boolean matchesPath = false;
    if (children == null || children.isEmpty()) {
      matchesPath = types == REQUIRED_TYPES;
    } else {
      for (Operator<?> child : children) {
        if (selectPath(child, types, matches)) {
          matchesPath = true;
        }
      }
    }
    matchesByType[types] = matchesPath;
    if (matchesPath) {
      totals.computeIfAbsent(operator, ignored -> new Total());
    }
    return matchesPath;
  }

  private static MapOperatorRunTime fromContext() {
    MapredContext context = MapredContext.get();
    return context == null ? null : context.getMapOperatorRunTime();
  }

  public static MapOperatorRunTime current() {
    MapOperatorRunTime timing = fromContext();
    return timing != null && timing.running ? timing : null;
  }

  static Long completedNanos(Operator<?> operator) {
    MapOperatorRunTime timing = fromContext();
    Total total = timing == null || timing.running ? null : timing.totals.get(operator);
    return total == null ? null : total.nanos;
  }

  public void finishRun() {
    running = false;
  }

  public long nestedNanos() {
    return nestedNanos;
  }

  private void finishScope(Operator<?> operator, long startNanos, long nestedAtStart) {
    long elapsed = System.nanoTime() - startNanos;
    long children = nestedNanos - nestedAtStart;
    assert elapsed >= children && children >= 0;
    Total total = totals.get(operator);
    if (total != null) {
      total.nanos += elapsed - children;
    }
    // Collapse the completed scope to a single child of its caller, even if it is unreported.
    nestedNanos = nestedAtStart + elapsed;
  }

  public void finishRead(AbstractMapOperator root, long startNanos, long nestedAtStart) {
    finishScope(readerOwners.get(root), startNanos, nestedAtStart);
  }

  public void process(AbstractMapOperator operator, Writable row) throws HiveException {
    long nestedAtStart = nestedNanos;
    long start = System.nanoTime();
    try {
      operator.process(row);
    } finally {
      finishScope(operator, start, nestedAtStart);
    }
  }

  void process(Operator<?> operator, Object row, int tag) throws HiveException {
    long nestedAtStart = nestedNanos;
    long start = System.nanoTime();
    try {
      operator.process(row, tag);
    } finally {
      finishScope(operator, start, nestedAtStart);
    }
  }

  @FunctionalInterface
  interface OperatorAction {
    void run() throws HiveException;
  }

  void timeAction(Operator<?> operator, OperatorAction action) throws HiveException {
    long nestedAtStart = nestedNanos;
    long start = System.nanoTime();
    try {
      action.run();
    } finally {
      finishScope(operator, start, nestedAtStart);
    }
  }
}
