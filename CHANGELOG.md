# CHANGELOG

## 0.4.0

- Rework `transactions` mode:
  - Update transactions mode to store `block_timestamp` and `block_height` for every ExecutionOutcome.
  - Removes empty `proof` field from the ExecutionOutcome.
  - Rework receipt headers to avoid having to keep them in memory.
  - Introduce `blocks` table to store block headers.
