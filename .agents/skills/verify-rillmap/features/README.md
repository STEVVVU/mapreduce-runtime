# Feature map

| Path | Contract | Driver |
| --- | --- | --- |
| [Processing](processing.md) | Generic partitioned map, HTTP shuffle and reduce | `node tools/verify.mjs` |
| [Recovery](recovery.md) | Worker retry, restart, attempt fencing and duplicate publication | same driver |
| [Benchmark](benchmark.md) | Frozen scaling, skew, offered-load and worker-loss measurements with independent durable-result oracles | `node tools/benchmark.mjs smoke` or `benchmark` on an isolated Linux runner |
