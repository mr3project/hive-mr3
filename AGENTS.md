# Apache Hive adapted to use MR3 as the execution engine

# Coding Guidelines

## Assertions, Preconditions, and Invariants

Use assertions actively to express invariants in the design and implementation.

An invariant is a condition that, according to the design of the system, must be true at a particular point in execution.

Assertions are not merely defensive checks. They are executable documentation of assumptions and properties that are essential to understanding why the code is correct.

### `assert`: express invariants

Use `assert` when the condition is guaranteed by the design or by the preceding execution of the program.

For example, in Scala:

```scala
assert(currentTask != null)
assert(queue.nonEmpty)
assert(state == Running || state == Terminating)
```

An assertion means:

> According to the design and implementation of this program, this condition must hold here.

If an assertion fails, assume that there is a bug in the implementation or in the design. Do not treat assertion failure as an ordinary runtime condition.

Look actively for useful invariants when writing or modifying code. In particular, consider assertions for:

* relationships between fields or collections;
* object states and state transitions;
* consistency between separate modules or data structures;
* conditions before and after non-trivial operations;
* loop and recursive-function invariants;
* concurrency assumptions, such as thread ownership or required locks;
* conditions that follow logically from earlier checks or operations.

Prefer a precise executable invariant over a comment that merely describes the same property.

For example, prefer:

```scala
assert(numQueuedTasks == taskQueues.values.map(_.size).sum)
```

to:

```scala
// numQueuedTasks should equal the total number of queued tasks.
```

The assertion both documents the property and checks it during testing.

Try to make invariants as strong as reasonably possible. A stronger invariant documents more of the design and is more likely to expose bugs elsewhere in the system.

When an assertion fails, do not simply remove or weaken it to make the failure disappear. Determine whether:

1. the implementation violates the intended design, in which case fix the implementation; or
2. the asserted invariant does not match the actual design, in which case revise the invariant and, if necessary, the design.

When understanding a bug reveals a new invariant, add that invariant to the code.

### `require`: express caller obligations

Use `require` in Scala when a method accepts only arguments satisfying a particular precondition.

For example:

```scala
def divide(x: Int, y: Int): Int = {
  require(y != 0)
  x / y
}
```

This means:

> The caller is responsible for supplying a non-zero value for `y`.

A failed `require` indicates that the caller violated the contract of the method. It does not express an internal invariant established by the implementation.

The corresponding Java code should use an explicit argument check, such as:

```java
if (y == 0) {
  throw new IllegalArgumentException("y must be non-zero");
}
```

or the project's standard precondition utility.

Do not use Java `assert` for validating method arguments, configuration, external input, user input, network data, file contents, or other conditions that can legitimately be invalid at runtime.

Such validation is part of the runtime contract and must remain active independently of whether assertions are enabled.

### Distinguish preconditions from invariants

The key question is who is responsible for making the condition true.

If the caller must establish it, use a precondition:

```scala
require(index >= 0)
```

If the program's design and implementation guarantee it at this point, use an assertion:

```scala
assert(index < entries.size)
```

Do not replace an invariant with `require` merely because `require` performs a runtime check. Doing so changes the meaning of the code: it turns an internal correctness claim into a caller obligation.

Similarly, do not add defensive fallback behavior for states that the design says are impossible merely to avoid an assertion failure. If an impossible state occurs, exposing the bug is usually preferable to silently continuing with an arbitrary value.

A useful rule is:

* external uncertainty -> validation and error handling
* internal logical certainty -> assertion

### Assertions as executable documentation

Treat important assertions as part of the documentation of the implementation.

An assertion can often communicate information more precisely than prose because it states the property in terms of the actual program state.

This is particularly valuable for complex code involving state machines, multiple related data structures, concurrency, scheduling, ownership, or interactions between modules.

For example:

```scala
assert(localResources.keySet == localResourcePayloads.keySet)
```

does more than detect an error. It documents a structural relationship between the two data structures.

Place assertions close to the points where their invariants are meaningful.

When modifying code, preserve existing assertions unless the corresponding design has genuinely changed.

Whenever a non-obvious design fact can be stated clearly as an executable invariant, prefer recording it as an assertion rather than leaving that knowledge only in comments or in the developer's reasoning.

Assertions also help prevent design erosion. An invariant written today may expose an incorrect change years later when another part of the implementation is modified.

### Strong invariants

Prefer strong, meaningful invariants over weak sanity checks when the stronger property follows from the design.

For example:

```scala
assert(runningTasks.keySet.subsetOf(allTasks.keySet))
```

documents considerably more than:

```scala
assert(runningTasks != null)
```

An assertion should express something that can be justified from the design, not merely something that is usually or probably true.

Before adding an assertion, be able to answer:

> Why must this condition be true?

Do not use assertions for statistical expectations, common cases, performance assumptions, or environmental conditions that may legitimately fail.

### Assertions and program semantics

Assertions must not perform operations required for correct execution.

In particular, never put side effects inside assertions.

Bad:

```java
assert map.remove(key) != null;
```

The behavior of the program must not depend on whether assertions are enabled.

Assertions should check program state, not create or modify that state.

### Expensive invariants

Some valuable invariants may be too expensive to check on every production execution, especially invariants that traverse large collections or compare multiple data structures.

Do not discard such invariants solely because they are expensive.

Where appropriate, arrange for them to be checked only in test, debug, or assertion-enabled configurations.

The invariant itself remains useful documentation even when its runtime checking is conditional.

### General principle

When writing or reviewing code, repeatedly ask:

> According to the design, what must be true at this point?

If the answer is non-trivial and can be expressed clearly in code, consider adding an assertion.

Assertions should make the design visible in the implementation and make violations of that design fail early during development and testing.

