# Vertical elastic worker replacement

[中文说明](vertical_elastic_lifecycle_zh.md)

The master and training agents must be upgraded together. The agent acknowledges
its selected checkpoint using the current `plan_id`, checkpoint directory and
step; the master resolves and waits for that specific UCP task. An unrelated
historical successful task no longer authorizes replacement.

## Exit sequence

1. The master snapshots the old worker IDs before requesting checkpoint saving.
2. Agents send the graceful-exit signal to the lowest local rank and keep their
   checkpoint savers alive. Rank 0 submits UCP and acknowledges its checkpoint.
3. The master waits for the acknowledged UCP task to succeed and selects its
   output for recovery. In explicitly configured non-UCP mode, existing cold
   start behavior remains supported.
4. The master enters `REPLACING_WORKERS`. Agents observe this phase, wait for
   natural process exit, and force-stop remaining training processes only after
   the grace period. They report `VERTICAL_ELASTIC_EXITED` and return a successful
   agent result containing every local worker's global rank. This is not a
   training-success report.
5. The master waits briefly for these acknowledgements, then removes the fixed
   old generation, including Failed/Succeeded and critical workers. Unresponsive
   old Pods are deleted after the drain deadline because recovery is ready.
6. New worker ranks start at zero. Agents acknowledge configuration readiness even
   when `nproc_per_node` already equals the requested allocation.

Old-generation terminal events do not trigger failure retries, failure counting,
or job-success callbacks. New workers retain ordinary failure handling.
Checkpoint/UCP failure or timeout fails the replacement and stops the job rather
than silently restarting from scratch. Automatic rollback is not implemented.

## Timeouts

| Environment variable | Process | Default | Meaning |
| --- | --- | --- | --- |
| `DLROVER_VERTICAL_EXIT_TIMEOUT` | Training agent | 1800 seconds | Maximum wait after checkpoint submission for master replacement authorization |
| `DLROVER_VERTICAL_EXIT_GRACE_SECONDS` | Training agent | 30 seconds | Graceful process-exit wait after authorization |
| `DLROVER_VERTICAL_DRAIN_TIMEOUT` | Master | 60 seconds | Wait for old agents to acknowledge exit before deleting Pods |

Keep the master drain timeout longer than the agent grace timeout plus RPC
latency. Align the agent exit timeout with the master's existing checkpoint/UCP
wait limit. UCP storage must remain accessible independently of the old Pod.

## Validation boundaries

The focused tests exercise the real PyTorch agent event-recording path, planned
terminal states, graceful/forced exits, bounded waits, stale UCP results, and
cleanup of terminal/critical workers. Ascend hardware, external training
callbacks and Kubernetes scheduling still require deployment integration tests.
The existing phase named `COMPLETED` continues to mean replacement configuration
is ready; it does not certify that the first resumed training step has completed.
