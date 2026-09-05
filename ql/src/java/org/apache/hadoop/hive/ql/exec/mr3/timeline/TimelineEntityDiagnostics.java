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

import com.datamonad.mr3.history.EntityKey;
import com.datamonad.mr3.history.EntityType;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;

/** Produces compact diagnostics for duplicate DAG vertices and edges. */
final class TimelineEntityDiagnostics {

  private TimelineEntityDiagnostics() {
  }

  static boolean isDag(TimelineEntity entity) {
    return entity != null && EntityType.MR3_DAG().equals(entity.getEntityType());
  }

  static String describeDag(TimelineEntity entity) {
    if (entity == null) {
      return "entity=null";
    }
    return "entityId=" + entity.getEntityId()
        + " vertices={" + describeField(entity, "vertices", "vertexName") + "}"
        + " edges={" + describeField(entity, "edges", "edgeId") + "}";
  }

  private static String describeField(TimelineEntity entity, String fieldName, String identifierName) {
    Map<String, Object> otherInfo = entity.getOtherInfo();
    Object dagProto = otherInfo == null ? null : otherInfo.get(EntityKey.dagProto());
    Map<?, ?> dagProtoMap = asMap(dagProto);
    Object value = dagProtoMap == null ? null : dagProtoMap.get(fieldName);
    if (!(value instanceof Collection)) {
      return value == null ? "count=0 distinct=0 duplicates={} dagProtoType=" + typeName(dagProto)
          : "unexpectedType=" + value.getClass().getName();
    }

    Collection<?> values = (Collection<?>) value;
    Map<String, Integer> counts = new LinkedHashMap<>();
    for (Object item : values) {
      String identifier = getIdentifier(item, identifierName);
      counts.put(identifier, counts.getOrDefault(identifier, 0) + 1);
    }

    Map<String, Integer> duplicates = new LinkedHashMap<>();
    for (Map.Entry<String, Integer> count : counts.entrySet()) {
      if (count.getValue() > 1) {
        duplicates.put(count.getKey(), count.getValue());
      }
    }
    return "count=" + values.size() + " distinct=" + counts.size() + " duplicates=" + duplicates;
  }

  private static String typeName(Object value) {
    return value == null ? "null" : value.getClass().getName();
  }

  private static Map<?, ?> asMap(Object value) {
    if (value instanceof Map) {
      return (Map<?, ?>) value;
    }
    try {
      Object converted = value instanceof String
          ? GenericObjectMapper.OBJECT_READER.readValue((String) value)
          : GenericObjectMapper.read(GenericObjectMapper.write(value));
      return converted instanceof Map ? (Map<?, ?>) converted : null;
    } catch (IOException | RuntimeException e) {
      return null;
    }
  }

  private static String getIdentifier(Object item, String identifierName) {
    if (item instanceof Map) {
      Object identifier = ((Map<?, ?>) item).get(identifierName);
      if (identifier != null) {
        return String.valueOf(identifier);
      }
    }
    // Retain useful duplicate detection if an MR3 version uses a different identifier field.
    return "<missing " + identifierName + "> " + String.valueOf(item);
  }
}
