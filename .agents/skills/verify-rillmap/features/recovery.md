# Recovery
## Sub-features
Lease expiry, reassignment, restart fencing, idempotent completion bounded failure and exclusive concurrent restart.
## How to get to it (user POV)
Restart the coordinator with its original data directory or replace a dead worker.
## Driving it with the Node HTTP harness
`node tools/verify.mjs` kills an assigned worker, restarts a coordinator with an active lease, posts stale completions checks persisted accepted attempts, and launches four recovery contenders against the same data directory. Exactly one may acquire the OS-owned IPC guard.

The same real driver imports an old running lease from preserved `state.json`, requires pending reset and obsolete-token rejection, completes it and proves authoritative `jobs/<UUID>.json` survives another restart over the unchanged legacy lease. A corrupt snapshot must refuse startup. `node --test tools/metadata.test.mjs` separately covers partial import with authoritative overlay, orphan temporary files, invalid IDs and malformed snapshots.
## Gotchas
One coordinator owns a data directory. Local disk must survive; coordinator disk loss and power-loss guarantees are outside this prototype.
