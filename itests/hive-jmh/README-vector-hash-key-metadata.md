<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Vector hash-key string metadata benchmark

`VectorHashKeyWrapperMetadataBench` measures the production
`VectorHashKeyWrapperGeneralLongString` through `VectorHashKeyWrapperBatch`.
Use the same benchmark source in two builds:

1. Baseline: separate `byteStarts[]` and `byteLengths[]` arrays.
2. Candidate: one interleaved `int[]` holding `[start, length, ...]`.

This change adds the benchmark only. It does not implement the candidate layout.
Apply that production change separately; keep hashing, assertions, null behavior,
and byte comparison unchanged when comparing the layouts. There is deliberately
no runtime layout switch or benchmark-only wrapper implementation.

## What is timed

| Benchmark | Timed work |
| --- | --- |
| `evaluateAndHash` | Assign a batch into reusable wrappers, calculate hashes, and sum the hashes. |
| `evaluateAndProbe` | Assign and hash a batch, then look up each wrapper in a populated `HashMap` and sum the results. |

Every invocation processes 1,024 active rows. JMH's
`@OperationsPerInvocation(1024)` makes the average-time score **nanoseconds per
input row**, not per batch. Both methods return a checksum to keep the result
observable. `evaluateAndProbe` is the primary comparison; `evaluateAndHash`
helps explain whether a change affects assignment/hashing or table lookup.

The map is fixed during measurement. Misses are not inserted, so its size, hit
distribution, and allocation behavior do not change as the benchmark runs.
This covers the profiled assignment/hash/equality paths, not full aggregation,
insertion, eviction, or output serialization.

## Fixture

- All fixture generation, allocation, key copying, and validation happen in
  `@Setup(Level.Trial)`. The timed paths do not generate random data or strings.
- A deterministic ring of input batches is reused. Wrappers are reused between
  batches, just as in production; the ring rotates on each invocation.
- Each input string column uses one shared byte buffer, with variable padding,
  lengths, and nonzero slice starts. Map keys are deep copies with starts zero,
  so successful lookups perform content equality against independent arrays.
- The first long key uniquely identifies the logical group and is never null.
  Other long keys and the strings have deterministic, key-specific null states.
  Miss IDs are drawn from a disjoint range, preventing unintended hits even
  when all string keys are null or a grouping set overrides them.
- Types are interleaved in the key layout to exercise key-index versus
  type-specific-index mappings. Setup checks that the factory actually selects
  `VectorHashKeyWrapperGeneralLongString`.
- Selected input uses 2,048-row column capacity and chooses one row from each
  adjacent pair, in increasing order. It still has exactly 1,024 active rows.
  Unselected input uses the first 1,024 rows of the same capacity.
- Grouping-set mode overrides the first string key and, when present, the last
  long key other than the identifying first long. The same override is used for
  resident keys and probes. It exercises `evaluateBatchGroupingSets()` for one
  fixed grouping set; it does not measure processing multiple sets in sequence.
- Setup checks copy equality, hash agreement, normalized starts, independent
  string storage, resident-key uniqueness, and every expected probe result.
  These checks run when the benchmark is eventually started, not at compilation.

## Parameters

Defaults select one combination, avoiding an accidental large Cartesian product.
Use JMH `-p name=value` arguments to expand it deliberately.

| Parameter | Default | Supported values |
| --- | --- | --- |
| `keyShape` | `L2_S2` | `L2_S2`, `L1_S4`, `L4_S1` (long/string counts) |
| `stringSizes` | `SHORT` | `SHORT`: uniform 1–16 bytes; `MIXED`: 90% 8–32, 10% 128–256 bytes |
| `nullProfile` | `NONE` | `NONE`: no nulls/empties; `MIXED`: 10% null, 10% empty strings; `NULL_HEAVY`: 70% null, 10% empty strings |
| `hitPercent` | `95` | Any integer 0–100; try `100`, `95`, `0` |
| `residentGroups` | `4096` | Positive integer; try `4096`, `262144` |
| `batchRingSize` | `32` | Positive integer; try `32`, `128` |
| `selected` | `false` | `false`, `true` |
| `groupingSets` | `false` | `false`, `true` |

Percentages describe deterministic sampled distributions, not exact counts in
each batch. Null profiles also apply their null percentage to non-identifying
long keys. String-size percentages apply to non-null, nonempty strings.

A 32-batch ring has 32,768 active probes, even with a larger resident table. Use
a larger ring to touch more resident keys when investigating cache behavior.
All fixtures and maps are per thread; the default is one thread.

## Build with Maven (does not run JMH)

Use Java 21 and the repository's normal Hive-MR3 build prerequisites. In
particular, the configured Tez-MR3/MR3 dependencies must already be resolvable
from your Maven repositories; this is not a standalone Maven Central project.

From the `hive-mr3` repository root:

```sh
mvn -Pitests,perf -pl itests/hive-jmh -am -DskipTests install
```

The root `itests` profile includes the JMH module in the reactor. `perf` creates
`itests/hive-jmh/target/benchmarks.jar`, including JMH and runtime dependencies.
`-DskipTests` skips test execution but permits test compilation and test-JAR
creation required by this module. Do not substitute `-Dmaven.test.skip=true`.

If the matching Hive artifacts and their test JARs are already installed, the
shorter module-only build is:

```sh
mvn -f itests/hive-jmh/pom.xml -Pperf -DskipTests package
```

After changing the production wrapper in `ql`, use the reactor build again so
the benchmark does not accidentally use an older installed `hive-exec` JAR.

### DNS provider error when starting a fork

If Java 21 reports:

```text
java.net.spi.InetAddressResolverProvider: Provider org.xbill.DNS.spi.DnsjavaInetAddressResolverProvider not found
```

rebuild the shaded JAR with the updated `perf` profile. Hadoop 3.4.1 depends on
dnsjava 3.6.1, whose DNS provider class is stored under `META-INF/versions/18/`.
The shaded JAR must declare `Multi-Release: true` in its manifest for Java to
load that class. The `ManifestResourceTransformer` now adds this entry.

From the repository root, with matching Hive artifacts already installed:

```sh
mvn -f itests/hive-jmh/pom.xml -Pperf -DskipTests clean package
```

Replace any previously saved `baseline.jar` and `candidate.jar` with JARs rebuilt
from their respective production layouts using this packaging fix. Apply the
same POM change to both builds. An old saved JAR will still have the error even
after rebuilding `target/benchmarks.jar`. Keep the multi-release classes and
their service registration together; removing only the class would leave a
broken provider registration.

## Save the two builds

After building the baseline, from the repository root:

```sh
mkdir -p work/vector-key-metadata
cp itests/hive-jmh/target/benchmarks.jar work/vector-key-metadata/baseline.jar
```

Apply the candidate metadata layout, run the same reactor build, and save it:

```sh
mvn -Pitests,perf -pl itests/hive-jmh -am -DskipTests install
cp itests/hive-jmh/target/benchmarks.jar work/vector-key-metadata/candidate.jar
```

Record the production revision/patch for each JAR. Both must contain identical
benchmark source. Saving shaded JARs avoids switching a mutable installed
dependency between measurements.

## Run through Maven

The following commands **execute the benchmark**. They are instructions for a
later run; building the module does not execute them automatically.

From `itests/hive-jmh`, run the default baseline case:

```sh
cd itests/hive-jmh
mvn org.codehaus.mojo:exec-maven-plugin:3.1.0:exec \
  -Dexec.executable=java \
  -Dexec.args='-jar ../../work/vector-key-metadata/baseline.jar VectorHashKeyWrapperMetadataBench -prof gc -jvmArgsAppend "-Xms2g -Xmx2g -da" -rf json -rff ../../work/vector-key-metadata/baseline.json'
```

Repeat with `candidate.jar` and `candidate.json`. The external Java process and
shaded JAR let JMH forks find the full runtime classpath. The benchmark defaults
are three forks, five 2-second warmup iterations, and eight 2-second measurement
iterations per method. JMH scores and errors are reported per row.

For a focused successful-lookup comparison, use the same command with:

```text
VectorHashKeyWrapperMetadataBench.evaluateAndProbe -p hitPercent=100 -p nullProfile=NONE,MIXED,NULL_HEAVY
```

For the large-table, sparse-selection, grouping-set case, use:

```text
VectorHashKeyWrapperMetadataBench.evaluateAndProbe -p residentGroups=262144 -p batchRingSize=128 -p stringSizes=MIXED -p nullProfile=MIXED -p selected=true -p groupingSets=true
```

These fragments replace the benchmark name in `-Dexec.args`; keep the JAR path,
profiling/JVM options, and distinct result paths. To inspect registration without
executing setup or measurement, pass `-l` after the benchmark name.

Use identical JVM/GC settings and hardware for both builds, alternate baseline
and candidate runs, and compare the same parameter tuples and benchmark methods.
The default `-da` disables assertions for timing; keep that setting identical.
Setup validation remains active because it uses explicit checks.

`-prof gc` measures allocation during timed work. It does not measure the arrays
allocated and retained during setup. Use a separate heap/JOL inspection for the
retained-size difference, and inspect assembly or hardware counters separately
if a representative performance difference needs explanation.
