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

package org.apache.hadoop.hive.ql.exec.mr3.timeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.datamonad.mr3.history.EntityKey;
import com.datamonad.mr3.history.EntityType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.junit.Test;

public class TestTimelineEntityDiagnostics {

  @Test
  public void testDescribeDagReportsDuplicateIdentifiers() {
    TimelineEntity entity = new TimelineEntity();
    entity.setEntityId("dag_1");
    entity.setEntityType(EntityType.MR3_DAG());
    Map<String, Object> dagProto = new HashMap<>();
    dagProto.put("vertices", items("vertexName", "Map 1", "Map 2", "Map 1"));
    dagProto.put("edges", items("edgeId", "Map 1-Map 2", "Map 1-Map 2"));
    entity.addOtherInfo(EntityKey.dagProto(), dagProto);

    String description = TimelineEntityDiagnostics.describeDag(entity);

    assertTrue(description.contains("vertices={count=3 distinct=2 duplicates={Map 1=2}}"));
    assertTrue(description.contains("edges={count=2 distinct=1 duplicates={Map 1-Map 2=2}}"));
  }

  @Test
  public void testDescribeDagHandlesAbsentCollections() {
    TimelineEntity entity = new TimelineEntity();
    entity.setEntityId("dag_2");
    entity.setEntityType(EntityType.MR3_DAG());

    assertEquals("entityId=dag_2 vertices={count=0 distinct=0 duplicates={} dagProtoType=null} "
            + "edges={count=0 distinct=0 duplicates={} dagProtoType=null}",
        TimelineEntityDiagnostics.describeDag(entity));
  }

  @Test
  public void testDescribeDagHandlesJsonDagProto() {
    TimelineEntity entity = new TimelineEntity();
    entity.setEntityId("dag_3");
    entity.setEntityType(EntityType.MR3_DAG());
    entity.addOtherInfo(EntityKey.dagProto(), "{\"vertices\":[{\"vertexName\":\"Map 29\"},"
        + "{\"vertexName\":\"Map 29\"}],\"edges\":[{\"edgeId\":\"Map 12-Map 29\"}]}");

    String description = TimelineEntityDiagnostics.describeDag(entity);

    assertTrue(description.contains("vertices={count=2 distinct=1 duplicates={Map 29=2}}"));
    assertTrue(description.contains("edges={count=1 distinct=1 duplicates={}}"));
  }

  private static List<Map<String, Object>> items(String key, String... identifiers) {
    List<Map<String, Object>> result = new ArrayList<>();
    for (String identifier : identifiers) {
      Map<String, Object> item = new HashMap<>();
      item.put(key, identifier);
      result.add(item);
    }
    return result;
  }
}
