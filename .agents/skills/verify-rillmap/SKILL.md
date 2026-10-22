---
name: verify-rillmap
description: Launch, diagnose and drive the MapReduce runtime's real HTTP coordinator and separate worker processes, preserving crash recovery evidence.
---

# Launch
Run `node tools/verify.mjs` from the repository root with Node 25+. The maintained driver launches an isolated coordinator on an OS-selected loopback port and three worker child processes with distinct storage directories. No dependencies need installation. This skill was proved on Windows with Node 25.8.1; retained evidence is named in the implementation report.

# Doctor
The driver requests authenticated `GET /health`, asserting the coordinator PID equals its owned child PID, and records Node version and port. On unexpected errors inspect `.runs/<run>/coordinator.log` and worker logs. Never attach to another user's coordinator.

# Drive
The driver submits `POST /jobs`, polls `GET /jobs/:id`, and compares actual results against sequential independent word count, maximum temperature and inverted index oracles. It kills an assigned worker and restarts an in-flight coordinator; it also submits obsolete and duplicate task completions through the normal worker API.

# Evidence
`.runs/<timestamp>/evidence.json` records actions, job states, assertions and artifact manifests. `.runs` is ignored. Read the durable coordinator state, accepted attempt files and worker-local files alongside API responses. Require oracle equality and one accepted publication per task. See `features/README.md`.

For frozen load measurement use [the benchmark feature](features/benchmark.md). The reusable command is `node tools/benchmark.mjs smoke`; measurement and separate profiling use `benchmark` and `profile` on isolated Linux runners. Never treat a correctness smoke as throughput evidence.

# Cleanup
The driver kills and waits for only its tracked child handles in `finally`. All data, logs and evidence survive cleanup. A nonzero exit is failure, even if some evidence was written. Confirm `evidence.json` exists after process cleanup. Failed runs retain failure details.
