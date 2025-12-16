# Dragonboat Raft Configuration Demo

A demonstration tool for testing Raft consensus configurations, with focus on election timing and cluster behavior analysis.

## Installation

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
