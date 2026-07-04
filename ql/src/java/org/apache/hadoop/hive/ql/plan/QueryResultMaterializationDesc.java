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

import java.io.Serializable;

import org.apache.hadoop.fs.Path;

/**
 * Metadata for materializing QueryResultOperator output to a local path so it can be read back by FetchWork.
 */
public class QueryResultMaterializationDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String resultId;
  private final Path localMaterializationPath;
  private final TableDesc tableDesc;
  private final String columns;
  private final String columnTypes;

  public QueryResultMaterializationDesc(String resultId, Path localMaterializationPath, TableDesc tableDesc,
      String columns, String columnTypes) {
    this.resultId = resultId;
    this.localMaterializationPath = localMaterializationPath;
    this.tableDesc = tableDesc;
    this.columns = columns;
    this.columnTypes = columnTypes;
  }

  public String getResultId() {
    return resultId;
  }

  public Path getLocalMaterializationPath() {
    return localMaterializationPath;
  }

  public TableDesc getTableDesc() {
    return tableDesc;
  }

  public String getColumns() {
    return columns;
  }

  public String getColumnTypes() {
    return columnTypes;
  }
}
