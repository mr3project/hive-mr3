# Apache Hive adapted to use MR3 as the execution engine

# Coding Guidelines

## Assertions, Preconditions, and Invariants

An **invariant** is a property guaranteed by the system's design and preceding execution at a particular point in the program. Use Java `assert` to make useful internal invariants explicit and to detect violations during testing.

### Internal invariants: `assert`

Use `assert` for conditions that must hold if the implementation is correct:

```java
assert currentTask != null;
assert state == State.RUNNING || state == State.TERMINATING;
assert allTasks.keySet().containsAll(runningTasks.keySet());
```

Look for invariants involving state transitions, relationships between data structures, operations on queues or tasks, loop boundaries, and concurrency requirements such as lock ownership.

Prefer a strong, precise assertion to a weak sanity check or a comment when the stronger property follows from the design. For example:

```java
assert numQueuedTasks == taskQueues.values().stream()
    .mapToInt(Queue::size)
    .sum();
```

This captures the relationship between the task count and the queues, not just the validity of either value in isolation. Assert only what the design actually guarantees, not what is merely common or expected.

Place assertions where the invariant matters. Preserve existing assertions unless the design changes. If one fails, investigate whether the code violates the design or the asserted property is incorrect; do not simply remove or weaken it. When debugging reveals a new invariant, record it in the code.

### Preconditions and external input: `Preconditions`

Use Guava's `Preconditions`. Use `checkArgument()` for caller argument requirements and `checkState()` for runtime state requirements:

```java
import com.google.common.base.Preconditions;

Preconditions.checkArgument(retries >= 0, "retries must be non-negative");
Preconditions.checkState(!closed, "scheduler is closed");
```

These checks are always active. Do not use `assert` to validate caller arguments, configuration, user input, network data, file contents, or other values that can legitimately be invalid at runtime. Handle invalid external data appropriately at the boundary.

Distinguish a caller obligation (`checkArgument`), a runtime state requirement (`checkState`), and an internal property guaranteed by the design (`assert`). Do not replace assertions with `checkState` merely to make them always active, or silently substitute a fallback for a state the design says is impossible.

### Java assertion semantics and safety

Java assertions are **disabled by default** and are evaluated only when enabled, typically with the JVM option `-ea` (`-enableassertions`). Enable them during testing and debugging. If a condition must be enforced in production for correctness or safety, use `Preconditions.checkArgument()` or `Preconditions.checkState()` as appropriate, or an explicit error-handling path, rather than relying on `assert` alone.

Assertions must not change program behavior. In particular, **never put side effects inside an assertion**:

```java
// Wrong: removal does not happen when assertions are disabled.
assert map.remove(key) != null;
```

For concurrent code, evaluate invariants only while the required synchronization or ownership guarantees hold; an assertion over independently changing shared data may produce misleading failures.

Expensive invariants, such as those traversing collections, can still be valuable. Keep them in `assert` statements where appropriate so they are evaluated only when assertions are enabled, or check them in dedicated tests.

