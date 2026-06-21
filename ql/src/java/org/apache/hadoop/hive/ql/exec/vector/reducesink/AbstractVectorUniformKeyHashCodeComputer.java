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

package org.apache.hadoop.hive.ql.exec.vector.reducesink;

import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hive.common.util.Murmur3;

/**
 * Base class for computing uniform reduce-sink key hash codes by serializing one key at a time
 * into a small scratch buffer and hashing the resulting BinarySortable bytes.
 */
abstract class AbstractVectorUniformKeyHashCodeComputer implements VectorUniformKeyHashCodeComputer {

  protected final BinarySortableSerializeWrite serializeWrite;
  protected final Output output;

  AbstractVectorUniformKeyHashCodeComputer(BinarySortableSerializeWrite serializeWrite) {
    this.serializeWrite = serializeWrite;
    output = new Output();
  }

  protected void reset() {
    serializeWrite.set(output);
  }

  protected int hash() {
    return Murmur3.hash32(output.getData(), 0, output.getLength(), 0);
  }
}
