# Apache Hive adapted to use MR3 as the execution engine

## Assertion vs defensive checks

Do not add unnecessary defensive checks.

If a condition is guaranteed to hold true at a specific point according to
the current code and design, express the condition in an assertion,
not in a defensive check.

In this way, we express invariants of the current code and design in 
assertions.

If any assertion fails at runtime, that is due to errors in the implementation
and/or design.

On the other hand, if the arguments of a method are required to meet a condition,
express the condition in a defensive check, not in an assertion.

Such a defensive check expresses the contract between two components, as in the
rely-and-guarantee convention.



