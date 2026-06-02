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
package org.apache.hadoop.hive.serde2.lazybinary;

import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.io.IntWritable;

/**
 * LazyBinaryObject for integer which is serialized as a fixed-width int.
 */
public class LazyBinaryInteger extends
    LazyBinaryPrimitive<WritableIntObjectInspector, IntWritable> {

  LazyBinaryInteger(WritableIntObjectInspector oi) {
    super(oi);
    data = new IntWritable();
  }

  LazyBinaryInteger(LazyBinaryInteger copy) {
    super(copy);
    data = new IntWritable(copy.data.get());
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    assert (length == Integer.BYTES);
    data.set(LazyBinaryUtils.byteArrayToInt(bytes.getData(), start));
  }
}
