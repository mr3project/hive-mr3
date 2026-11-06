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
package org.apache.hive.benchmark.hash;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.hive.common.util.Murmur3;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Compares hash32 byte slices and allocation-free primitive paths. Inputs vary on
 * every invocation to prevent constant folding. Incremental XXHash32 and Murmur3
 * hash64 are controls. Run the same benchmark against both storage-api versions.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(3)
public class Hash32Bench {
  @State(Scope.Thread)
  public static class Bytes {
    @Param({"0", "1", "4", "8", "15", "16", "32", "64", "256", "1024", "4096"})
    public int length;
    public byte[][] data;
    public int cursor;
    public int seed = Murmur3.DEFAULT_SEED;
    public Murmur3.IncrementalHash32 hasher = new Murmur3.IncrementalHash32();

    @Setup
    public void setup() {
      Random random = new Random(123);
      data = new byte[256][length + 7];
      for (byte[] value : data) {
        random.nextBytes(value);
      }
    }
  }

  @State(Scope.Thread)
  public static class Values {
    public long[] data = new long[256];
    public int cursor;
    public int seed = Murmur3.DEFAULT_SEED;

    @Setup
    public void setup() {
      Random random = new Random(123);
      for (int i = 0; i < data.length; i++) {
        data[i] = random.nextLong();
      }
    }
  }

  @Benchmark
  public int bytes(Bytes state) {
    return Murmur3.hash32(state.data[state.cursor++ & 255], 7, state.length, state.seed);
  }

  @Benchmark
  public int incrementalControl(Bytes state) {
    byte[] data = state.data[state.cursor++ & 255];
    state.hasher.start(state.seed);
    state.hasher.add(data, 7, state.length);
    return state.hasher.end();
  }

  @Benchmark
  public int singleLong(Values state) {
    return Murmur3.hash32(state.data[state.cursor++ & 255], state.seed);
  }

  @Benchmark
  public int twoLongs(Values state) {
    int index = state.cursor++ & 255;
    return Murmur3.hash32(state.data[index], state.data[(index + 1) & 255], state.seed);
  }

  @Benchmark
  public long hash64Control(Values state) {
    return Murmur3.hash64(state.data[state.cursor++ & 255]);
  }
}
