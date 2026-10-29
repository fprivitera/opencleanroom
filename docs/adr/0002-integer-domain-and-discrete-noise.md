---
status: accepted
---
# Aggregated columns are integers; noise is discrete Laplace

Amounts are stored in cents and distances in tenths of a mile, declared as integers with clamping bounds in the schema. This enables OpenDP's discrete mechanism (`vector<i64>`, L1 metric) and closes the IEEE 754 side channel (Mironov) without snapping mechanisms. The cost: the admin must choose the unit of a column before exposing it.

The domain is `i64`, not OpenDP's default `int`, which maps to `i32` and rejects sums of 2^31 or more.
