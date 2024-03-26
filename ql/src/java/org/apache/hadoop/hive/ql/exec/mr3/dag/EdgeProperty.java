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

package org.apache.hadoop.hive.ql.exec.mr3.dag;

import com.datamonad.mr3.DAGAPI;
import com.datamonad.mr3.api.common.MR3Conf;

import javax.annotation.Nullable;

public class EdgeProperty {

  public enum DataMovementType {
    ONE_TO_ONE,
    BROADCAST,
    SCATTER_GATHER,
    CUSTOM
  }

  private final DataMovementType dataMovementType;

  private final EntityDescriptor srcLogicalOutputDescriptor;
  private final EntityDescriptor destLogicalInputDescriptor;
  private final EntityDescriptor edgeManagerPluginDescriptor;

  private boolean isFixed;  // isFixed == true iff auto parallelism should not be used (for MR3), false by default

  public EdgeProperty(
      DataMovementType dataMovementType,
      EntityDescriptor srcLogicalOutputDescriptor,
      EntityDescriptor destLogicalInputDescriptor,
      @Nullable EntityDescriptor edgeManagerPluginDescriptor) {
    this.dataMovementType = dataMovementType;
    this.srcLogicalOutputDescriptor = srcLogicalOutputDescriptor;
    this.destLogicalInputDescriptor = destLogicalInputDescriptor;
    this.edgeManagerPluginDescriptor = edgeManagerPluginDescriptor;
  }

  public DataMovementType getDataMovementType() {
    return dataMovementType;
  }

  public EntityDescriptor getSrcLogicalOutputDescriptor() {
    return srcLogicalOutputDescriptor;
  }

  public EntityDescriptor getDestLogicalInputDescriptor() {
    return destLogicalInputDescriptor;
  }

  public EntityDescriptor getEdgeManagerPluginDescriptor() {
    return edgeManagerPluginDescriptor;
  }

  public void setFixed() {
    this.isFixed = true;
  }

  public boolean isFixed() {
    return this.isFixed;
  }

  // DAGProto Conversion utilities
  public DAGAPI.EdgeDataMovementTypeProto createEdgeDataMovementTypeProto() {
    switch(dataMovementType){
      case ONE_TO_ONE : return DAGAPI.EdgeDataMovementTypeProto.ONE_TO_ONE;
      case BROADCAST : return DAGAPI.EdgeDataMovementTypeProto.BROADCAST;
      case SCATTER_GATHER : return DAGAPI.EdgeDataMovementTypeProto.SCATTER_GATHER;
      case CUSTOM: return DAGAPI.EdgeDataMovementTypeProto.CUSTOM;
      default :
        throw new RuntimeException("unknown 'dataMovementType': " + dataMovementType);
    }
  }
}