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
package org.apache.hadoop.hive.ql.io.orc;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.SchemaEvolution;
import org.junit.Test;

public class TestOrcInputFormatIncludeMasks {

  @Test
  public void testIncludeAcidMetadataColumnsForRowId() {
    TypeDescription acidSchema = acidSchema();
    boolean[] include = payloadOnlyInclude(acidSchema);

    boolean payloadColumnA = include[columnId(acidSchema, OrcRecordUpdater.ROW_FIELD_NAME, "a")];
    boolean payloadColumnB = include[columnId(acidSchema, OrcRecordUpdater.ROW_FIELD_NAME, "b")];
    boolean operation = include[columnId(acidSchema, OrcRecordUpdater.OPERATION_FIELD_NAME)];
    boolean currentWriteId = include[columnId(acidSchema, OrcRecordUpdater.CURRENT_WRITEID_FIELD_NAME)];

    boolean[] result = OrcInputFormat.includeAcidMetadataColumns(acidSchema, include, false);

    assertTrue(result[columnId(acidSchema, OrcRecordUpdater.ORIGINAL_WRITEID_FIELD_NAME)]);
    assertTrue(result[columnId(acidSchema, OrcRecordUpdater.BUCKET_FIELD_NAME)]);
    assertTrue(result[columnId(acidSchema, OrcRecordUpdater.ROW_ID_FIELD_NAME)]);
    assertFalse(result[columnId(acidSchema, OrcRecordUpdater.CURRENT_WRITEID_FIELD_NAME)]);
    assertFalse(result[columnId(acidSchema, OrcRecordUpdater.OPERATION_FIELD_NAME)]);
    assertTrue(result[columnId(acidSchema, OrcRecordUpdater.ROW_FIELD_NAME, "a")]);
    assertTrue(result[columnId(acidSchema, OrcRecordUpdater.ROW_FIELD_NAME, "b")]);
    assertTrue(payloadColumnA == result[columnId(acidSchema, OrcRecordUpdater.ROW_FIELD_NAME, "a")]);
    assertTrue(payloadColumnB == result[columnId(acidSchema, OrcRecordUpdater.ROW_FIELD_NAME, "b")]);
    assertTrue(operation == result[columnId(acidSchema, OrcRecordUpdater.OPERATION_FIELD_NAME)]);
    assertTrue(currentWriteId == result[columnId(acidSchema, OrcRecordUpdater.CURRENT_WRITEID_FIELD_NAME)]);
  }

  @Test
  public void testIncludeCurrentWriteIdWhenFetchingDeletedRows() {
    TypeDescription acidSchema = acidSchema();
    boolean[] result = OrcInputFormat.includeAcidMetadataColumns(acidSchema,
        payloadOnlyInclude(acidSchema), true);

    assertTrue(result[columnId(acidSchema, OrcRecordUpdater.ORIGINAL_WRITEID_FIELD_NAME)]);
    assertTrue(result[columnId(acidSchema, OrcRecordUpdater.BUCKET_FIELD_NAME)]);
    assertTrue(result[columnId(acidSchema, OrcRecordUpdater.ROW_ID_FIELD_NAME)]);
    assertTrue(result[columnId(acidSchema, OrcRecordUpdater.CURRENT_WRITEID_FIELD_NAME)]);
  }

  @Test
  public void testCreateEventOptionsUsesEventSchemaLengthIncludeForAcidFiles() {
    TypeDescription rowSchema = TypeDescription.fromString("struct<a:int,b:string>");
    boolean[] rowInclude = OrcInputFormat.genIncludedColumns(rowSchema, java.util.Arrays.asList(0));
    Reader.Options options = new Reader.Options(new Configuration()).schema(rowSchema).include(rowInclude);

    Reader.Options eventOptions = OrcRawRecordMerger.createEventOptions(options, rowSchema, false, false);
    TypeDescription eventSchema = eventOptions.getSchema();
    boolean[] eventInclude = eventOptions.getInclude();

    assertEquals(eventSchema.getMaximumId() + 1, eventInclude.length);
    assertTrue(eventInclude[columnId(eventSchema, OrcRecordUpdater.OPERATION_FIELD_NAME)]);
    assertTrue(eventInclude[columnId(eventSchema, OrcRecordUpdater.ORIGINAL_WRITEID_FIELD_NAME)]);
    assertTrue(eventInclude[columnId(eventSchema, OrcRecordUpdater.BUCKET_FIELD_NAME)]);
    assertTrue(eventInclude[columnId(eventSchema, OrcRecordUpdater.ROW_ID_FIELD_NAME)]);
    assertTrue(eventInclude[columnId(eventSchema, OrcRecordUpdater.CURRENT_WRITEID_FIELD_NAME)]);
    assertTrue(eventInclude[columnId(eventSchema, OrcRecordUpdater.ROW_FIELD_NAME, "a")]);
    assertFalse(eventInclude[columnId(eventSchema, OrcRecordUpdater.ROW_FIELD_NAME, "b")]);
  }

  private static TypeDescription acidSchema() {
    return SchemaEvolution.createEventSchema(TypeDescription.fromString("struct<a:int,b:string>"));
  }

  private static boolean[] payloadOnlyInclude(TypeDescription acidSchema) {
    boolean[] include = new boolean[acidSchema.getMaximumId() + 1];
    include[0] = true;
    TypeDescription row = acidSchema.findSubtype(OrcRecordUpdater.ROW_FIELD_NAME);
    include[row.getId()] = true;
    for (TypeDescription child : row.getChildren()) {
      include[child.getId()] = true;
    }
    return include;
  }

  private static int columnId(TypeDescription schema, String columnName) {
    return schema.findSubtype(columnName).getId();
  }

  private static int columnId(TypeDescription schema, String structName, String columnName) {
    return schema.findSubtype(structName + "." + columnName).getId();
  }
}
