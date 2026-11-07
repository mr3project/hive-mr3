# Long/string batch hash benchmark

`org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperLongStringHashBench`
compares two ways of hashing a complete batch of **1,024 active rows**:

| Method | Work per operation |
| --- | --- |
| `rowWise` | Previous row loop: call `computeHash(row)` and write each cached hash. |
| `columnWise` | Current `setHashKeys(1024)`, including both scratch-array resets. |
| `checksumOnly` | Read all cached hashes, without recomputing them. |

Both hash methods use the same production storage class and string-hashing
implementation. This isolates the long/null seed traversal change; it does not
compare different Git revisions. Both include the same batch selection and
checksum consumption. JMH consumes the returned checksum so every row's hash
contributes to the observable result.

All storage, scratch arrays, long values, and byte buffers are allocated during
trial setup. Input generation, assignments, and retained-key copying are outside
the measured methods. Setup compares the two implementations' hashes for every
row of every prepared batch. Forks enable assertions with `-ea` for these checks.
No benchmark has been run as part of adding this benchmark.

## Build

Run from the repository root using the project's JDK 21 and Maven environment:

```sh
mvn -B -ntp -Pitests,perf -pl itests/hive-jmh -am package -DskipTests
```

This builds the reactor dependencies and the runnable
`itests/hive-jmh/target/benchmarks.jar`. Use `-DskipTests`, rather than
`-Dmaven.test.skip=true`, because the existing JMH module depends on Hive test
JARs. The build does not run JMH.

In the provided managed environment, first source
`/workspace/.onboarding/env.sh` and append
`-Drat.excludesFile=/workspace/.onboarding/rat-excludes.txt -Ddevelocity.scan=false`
to the Maven command. Its Maven wrapper prepares the required MR3/Tez artifacts.

## Initial comparison

```sh
java -jar itests/hive-jmh/target/benchmarks.jar \
  'org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperLongStringHashBench\.(rowWise|columnWise|checksumOnly)$' \
  -prof gc -rf json -rff long-string-hash-baseline.json
```

Defaults are one thread, three forks, five one-second warmup iterations, and
five one-second measurement iterations. Each result is average **nanoseconds per
batch operation**, including the checksum. No operations-per-invocation divisor
is applied; divide by 1,024 to express the result per row.

The initial command runs these layouts with no nulls and approximately 32-byte
strings, in both reuse modes:

| `layout` | Long slots | String slots |
| --- | ---: | ---: |
| `L4_S0` | 4 | 0 |
| `L8_S0` | 8 | 0 |
| `L0_S4` | 0 | 4 |
| `L2_S2` | 2 | 2 |
| `L4_S5` | 4 | 5 |

All five layouts select `GENERAL_LONG_STRING` in production.

## Four-long/five-string variations

Keep this second matrix restricted to the Q67-shaped layout:

```sh
java -jar itests/hive-jmh/target/benchmarks.jar \
  'org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperLongStringHashBench\.(rowWise|columnWise|checksumOnly)$' \
  -p layout=L4_S5 \
  -p stringBytes=8,128 \
  -p nullPattern=NO_NULLS,RANDOM_10_PERCENT,HALF_STRINGS_NULL,ALL_STRINGS_NULL \
  -prof gc -rf json -rff long-string-hash-q67.json
```

For the non-null empty-string finalization path, run a separate case:

```sh
java -jar itests/hive-jmh/target/benchmarks.jar \
  'org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperLongStringHashBench\.(rowWise|columnWise|checksumOnly)$' \
  -p layout=L4_S5 -p stringBytes=0 -p nullPattern=NO_NULLS \
  -prof gc -rf json -rff long-string-hash-empty.json
```

## Parameters and data representation

| Parameter | Meaning |
| --- | --- |
| `reuse=HOT` | Repeatedly hash one preallocated batch. |
| `reuse=ROTATING` | Cycle through 16 independently populated, preallocated batches. This broadens the working set; it does not guarantee cold caches on every CPU. |
| `stringBytes` | Average slice length. Positive values generate lengths from half the value through one-and-a-half times the value; zero produces non-null empty slices. Every non-null slice has a nonzero start. |
| `nullPattern=NO_NULLS` | Every logical key is non-null. |
| `nullPattern=RANDOM_10_PERCENT` | Each data key independently has a 10% probability of being null. The Q67 grouping identifier stays non-null. |
| `nullPattern=HALF_STRINGS_NULL` | The first half of the string columns, rounded down, are null for every row. Numeric data columns remain non-null. |
| `nullPattern=ALL_STRINGS_NULL` | Every string column is null for every row. Numeric data columns remain non-null. |

The state owns one or 16 storage objects, a rotation index, and the four public
parameters. Each storage owns its normal column-major values, fixed row views,
two reusable scratch arrays, and its own hash context. Input generation uses a
fixed random seed, producing identical inputs for matching parameters in both
implementations. Long data keys contain varied signed 64-bit values; strings
contain varied ASCII bytes.

For `L4_S5`, the logical key order is three longs, five strings, and the fourth
long as a repeating, non-null grouping identifier. Uniform string-null patterns
use the corresponding grouping mask as that identifier. These are synthetic
grouping-set-like cases, not execution of all nine Q67 grouping sets.

Null longs contain zero. Null string slices contain a null byte reference,
start zero, and length `-1`. Inactive rows are not part of these benchmarks.

## Interpret the results

Compare matching parameter tuples and report:

```text
layout, nullPattern, stringBytes, reuse,
rowWise ns/batch, columnWise ns/batch, checksumOnly ns/batch,
difference ns/batch, speedup
```

`difference = rowWise - columnWise`; a positive value favors column-wise hashing.
`speedup = rowWise / columnWise`; a value above one favors column-wise hashing.
Consider JMH's reported errors and fork-to-fork variation. Increase warmup or
measurement duration if results are unstable, for example
`-wi 10 -w 2s -i 10 -r 2s`.

Report the raw checksum-inclusive scores and the checksum control together.
The control shows the shared consumption cost; subtracting separate noisy
measurements can obscure small differences. The GC profiler should show
approximately zero allocation per operation after setup.

Use the same CPU, JDK, and JVM options for each variant, and record them with the
results. Additional JVM options can be supplied with `-jvmArgsAppend`; retain
`-ea` so setup validates hash equivalence. For example,
`-jvmArgsAppend '-ea -Xms2g -Xmx2g'`. This benchmark measures hashing, including
string bytes, but does not establish total-query CPU savings.

---

Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with this
work for additional information regarding copyright ownership. The ASF
licenses this file to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
