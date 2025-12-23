# Dragonboat Raft Configuration Demo

A demonstration tool for testing Raft consensus configurations, with focus on election timing and cluster behavior analysis.

## Installation

For dragonboat fork submodule 

```bash
git submodule update --init --recursive
```

```bash
cd raft-demo
go mod init raft-demo
go mod tidy
go build -o raft-demo .
```

## Parameters

| Flag | Description | Default |
|------|-------------|---------|
| `--nodes` | Number of nodes in cluster | 3 |
| `--rtt` | Base RTT time unit (ms) | 100 |
| `--heartbeat` | Heartbeat interval (in RTT units) | 1 |
| `--election` | Election timeout (in RTT units) | 10 |
| `--snapshot` | Entries between snapshots | 100 |
| `--check-quorum` | Enable CheckQuorum | true |
| `--prevote` | Enable PreVote | true |
| `--quiesce` | Enable Quiesce mode | false |
| `--proposal-interval` | Proposal interval (ms) | 500 |
| `--auto-workload` | Automatic workload | false |
| `--duration` | Test duration (sec, 0=interactive) | 0 |
| `--startup-spread` | **Max random startup delay per node (ms)** | 0 |
| `--transport-max-delay-ms` | Uniform per-message one-way delay injector (ms). Simple jitter model. | 0 |
| `--latency-preset` | Transport RTT distribution preset: `global-pessimistic` \| `global-measured` \| `latency-aware` | - |
| `--latency-values-ms` | Custom RTT samples in ms (comma-separated). Used when no preset. | - |
| `--latency-weights` | Optional weights (comma-separated, same length as values). | - |
| `--latency-jitter-ms` | Symmetric RTT jitter (ms) applied to sampled RTT. | 0 |
| `--latency-spike-ms` | RTT spike value (ms) applied with probability. | 0 |
| `--latency-spike-prob` | Probability of RTT spike (0..1). | 0 |
| `--forced-leader-replica-id` | Force a specific replica ID to be the initial leader (dragonboat fork PR#2). All nodes must agree on this value. `0` disables. | 0 |
| `--trials` | Run N automated trials and print p50/p90/p99 for initial election and stop-leader re-election | 0 |
| `--trials-verbose` | Print per-trial results when using `--trials` | false |
| `--trials-timeout-ms` | Per-phase timeout for trials | 10000 |
| `--consensus-trials` | Run N automated **consensus** trials and print p50/p90/p99 for **proposal commit latency** (steady-state and post-failover) | 0 |
| `--consensus-proposals` | Proposals per phase in consensus trials (steady-state + post-failover) | 100 |
| `--consensus-proposal-timeout-ms` | Timeout per proposal in consensus trials (ms) | 5000 |
| `--config` | Load config from JSON | - |
| `--save-config` | Save config to JSON | - |

## Startup Spread Parameter

The `--startup-spread` parameter simulates real-world block propagation delays:

```bash
./raft-demo --nodes=11 --rtt=100 --election=10 --startup-spread=500 
```

When `--startup-spread=500`:
- Each node starts with a random delay between 0-500ms
- Simulates nodes receiving Ethereum block at different times
- First node to start typically becomes leader quickly
- Eliminates split vote chaos

Output example:
```
Node startup delays (simulating block propagation):
  Node 1: +234ms
  Node 2: +12ms    ← Starts first, likely becomes leader
  Node 3: +456ms
  ...

>>> INITIAL LEADER: Node 2 elected in 312ms
```

## Time Calculation Formulas

```
Actual election timeout = ElectionRTT × RTTMillisecond
Actual heartbeat interval = HeartbeatRTT × RTTMillisecond
```

Note: `--rtt` influences **Raft timers**. Network latency is modeled separately via `--transport-max-delay-ms` or the `--latency-*` distribution flags.

## Transport Latency Modeling (RTT distributions)

For globally distributed nodes, a single RTT number is misleading. Use a distribution:

- `--latency-preset=global-pessimistic`: RTT ~200ms baseline, jitter ±30ms, occasional spikes to 350ms.
- `--latency-preset=global-measured`: sample RTT from `{90,105,150,220,280,350}` with a heavier tail.
- `--latency-preset=latency-aware`: RTT clustered around 50–90ms.

All `--latency-*` values are interpreted as **RTT in ms**, and the demo applies **one-way delay = RTT/2** on each message send.

## Interactive Commands

| Command | Description |
|---------|-------------|
| `stats` | Show cluster statistics |
| `leader` | Show current leader |
| `history` | Leader election history |
| `elections` | Election timing stats (avg/min/max) |
| `stop <n>` | Stop node n (simulate failure) |
| `start <n>` | Restart node n |
| `propose <v>` | Send value to cluster |
| `workload` | Start continuous workload |
| `quit` | Exit |

## Measuring Election Time

Real-time output during leader changes:
```
>>> LEADER LOST at 15:04:05.123 (was Node 1)
>>> NEW LEADER ELECTED: Node 3 in 287ms
```

Use `elections` command for statistics:
```
> elections

=== Election Timing Statistics ===
Total elections: 3
Average: 195 ms
Min: 142 ms
Max: 287 ms

Individual election times:
  initial: 142ms      ← Fast with staggered startup!
  re-election #1: 156ms
  re-election #2: 287ms
```

## Metrics to Monitor

1. **election_avg_ms** — average time to elect leader
2. **election_max_ms** — worst-case election time (important for SLA)
3. **leader_changes** — number of leader changes (fewer = more stable)
4. **proposals_failed** — failed proposals during elections
5. **Individual elections** — compare initial vs subsequent

## Key Insights

| Scenario | Initial Election | Subsequent Elections |
|----------|-----------------|---------------------|
| Simultaneous startup | 1-5+ seconds (split votes) | 50-300ms |
| Staggered startup | 100-400ms | 50-300ms |
| After leader failure | N/A | 50-300ms |

## Recommendations

- `ElectionRTT` should be ≥ 10 × `HeartbeatRTT`
- Use `--startup-spread` matching your expected block propagation time
- `CheckQuorum=true` prevents split-brain scenarios
- `PreVote=true` reduces unnecessary re-elections
- For Ethereum-style consensus, set spread to expected block propagation window

## Measuring Consensus (proposal commit latency)

`--consensus-trials` measures end-to-end `SyncPropose` latency (client-observed commit latency) under your transport delay model:

- **steady-state**: after a leader is elected
- **post-failover**: after force-stopping the leader and waiting for a new leader

Example (10 trials, global-measured transport RTT distribution):

```bash
./raft-demo --nodes=11 --rtt=100 --heartbeat=1 --election=5 --startup-spread=500 \
  --latency-preset=global-measured \
  --consensus-trials=10 --consensus-proposals=100 --consensus-proposal-timeout-ms=5000
```
