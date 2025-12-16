// Dragonboat Raft Configuration Demo
// This demo allows testing various Raft configuration parameters
// to find optimal settings for your environment.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	transport "github.com/lni/dragonboat/v4/plugin/chan"
	"github.com/lni/dragonboat/v4/raftio"
	"github.com/lni/dragonboat/v4/raftpb"
	sm "github.com/lni/dragonboat/v4/statemachine"
	"golang.org/x/sync/errgroup"
)

// DemoConfig holds all configurable parameters for the demo
type DemoConfig struct {
	// Cluster settings
	NodeCount int    `json:"node_count"`
	ShardID   uint64 `json:"shard_id"`
	BasePort  int    `json:"base_port"`
	DataDir   string `json:"data_dir"`

	// Timing parameters (in RTT units)
	RTTMillisecond uint64 `json:"rtt_millisecond"`
	HeartbeatRTT   uint64 `json:"heartbeat_rtt"`
	ElectionRTT    uint64 `json:"election_rtt"`

	// Snapshot settings
	SnapshotEntries    uint64 `json:"snapshot_entries"`
	CompactionOverhead uint64 `json:"compaction_overhead"`

	// Advanced settings
	CheckQuorum bool `json:"check_quorum"`
	PreVote     bool `json:"pre_vote"`
	Quiesce     bool `json:"quiesce"`

	// Test parameters
	ProposalIntervalMs int `json:"proposal_interval_ms"`
	TestDurationSec    int `json:"test_duration_sec"`

	// Startup spread - simulates nodes receiving block at different times
	// Each node starts with random delay in range [0, StartupSpreadMs)
	StartupSpreadMs     int    `json:"startup_spread_ms"`
	TransportMaxDelayMs uint64 `json:"transport_max_delay_ms"`

	// Transport latency model (optional). When set, this overrides TransportMaxDelayMs.
	//
	// All values below are interpreted as RTT in milliseconds and converted to
	// one-way delays internally (by dividing by 2) since we delay each send.
	TransportLatencyPreset    string    `json:"transport_latency_preset"`
	TransportLatencyValuesMs  []uint64  `json:"transport_latency_values_ms"`
	TransportLatencyWeights   []float64 `json:"transport_latency_weights"`
	TransportLatencyJitterMs  uint64    `json:"transport_latency_jitter_ms"`
	TransportLatencySpikeMs   uint64    `json:"transport_latency_spike_ms"`
	TransportLatencySpikeProb float64   `json:"transport_latency_spike_prob"`
}

// DefaultConfig returns sensible default configuration
func DefaultConfig() DemoConfig {
	return DemoConfig{
		NodeCount:          3,
		ShardID:            1,
		BasePort:           63000,
		DataDir:            "/tmp/raft-demo",
		RTTMillisecond:     100,
		HeartbeatRTT:       1,
		ElectionRTT:        10,
		SnapshotEntries:    100,
		CompactionOverhead: 50,
		CheckQuorum:        true,
		PreVote:            true,
		Quiesce:            false,
		ProposalIntervalMs: 500,
		TestDurationSec:    60,
		StartupSpreadMs:    0, // 0 = all nodes start simultaneously
	}
}

type TransportLatencyModel struct {
	// If preset is empty, this model is disabled.
	preset string

	// Discrete RTT distribution (ms). Sample values with weights.
	valuesRTTms []uint64
	weights     []float64
	weightCDF   []float64

	// Optional jitter applied to sampled RTT (ms). Applied symmetrically.
	jitterRTTms uint64

	// Optional spike applied to sampled RTT with probability (ms).
	spikeRTTms uint64
	spikeProb  float64
}

func (m *TransportLatencyModel) Enabled() bool {
	return m != nil && (m.preset != "" || len(m.valuesRTTms) > 0)
}

func (m *TransportLatencyModel) SampleOneWayDelay() time.Duration {
	// Returns one-way delay, as we apply delay to each send direction.
	if m == nil {
		return 0
	}
	rtt := m.sampleRTTms()
	return time.Duration(rtt/2) * time.Millisecond
}

func (m *TransportLatencyModel) sampleRTTms() uint64 {
	var base uint64

	if len(m.valuesRTTms) > 0 {
		base = m.sampleDiscreteRTTms()
	}

	// Jitter is applied symmetrically around base: base +/- jitter
	if m.jitterRTTms > 0 {
		j := int64(m.jitterRTTms)
		delta := rand.Int63n(2*j+1) - j
		// Clamp at 0.
		if delta < 0 && uint64(-delta) > base {
			base = 0
		} else {
			base = uint64(int64(base) + delta)
		}
	}

	// Spike overrides base upward.
	if m.spikeRTTms > 0 && m.spikeProb > 0 {
		if rand.Float64() < m.spikeProb {
			if m.spikeRTTms > base {
				base = m.spikeRTTms
			}
		}
	}

	return base
}

func (m *TransportLatencyModel) sampleDiscreteRTTms() uint64 {
	// Assumes weightCDF has been built.
	if len(m.valuesRTTms) == 1 {
		return m.valuesRTTms[0]
	}
	r := rand.Float64()
	i := sort.SearchFloat64s(m.weightCDF, r)
	if i < 0 {
		i = 0
	}
	if i >= len(m.valuesRTTms) {
		i = len(m.valuesRTTms) - 1
	}
	return m.valuesRTTms[i]
}

func buildTransportLatencyModel(cfg DemoConfig) (*TransportLatencyModel, error) {
	// Presets are intentionally blunt — the goal is to approximate a fat-tailed RTT distribution.
	switch strings.ToLower(strings.TrimSpace(cfg.TransportLatencyPreset)) {
	case "":
		// No preset; allow custom values below.
	case "global-pessimistic":
		// RTT ~200ms baseline, jitter ±30ms, occasional spikes to 350ms.
		cfg.TransportLatencyValuesMs = []uint64{200}
		cfg.TransportLatencyWeights = []float64{1.0}
		cfg.TransportLatencyJitterMs = 30
		cfg.TransportLatencySpikeMs = 350
		cfg.TransportLatencySpikeProb = 0.05
	case "global-measured":
		// Sample RTT from {90,105,150,220,280,350} with a heavy-ish tail.
		cfg.TransportLatencyValuesMs = []uint64{90, 105, 150, 220, 280, 350}
		// More mass in the tail than a uniform distribution.
		cfg.TransportLatencyWeights = []float64{0.10, 0.10, 0.18, 0.22, 0.25, 0.15}
	case "latency-aware":
		// Latency-clustered election set (multi-DC but not worldwide scatter).
		cfg.TransportLatencyValuesMs = []uint64{50, 70, 90}
		cfg.TransportLatencyWeights = []float64{0.34, 0.33, 0.33}
	default:
		return nil, fmt.Errorf("unknown --latency-preset %q", cfg.TransportLatencyPreset)
	}

	if len(cfg.TransportLatencyValuesMs) == 0 {
		// Model disabled unless values provided.
		return nil, nil
	}
	if len(cfg.TransportLatencyWeights) != 0 && len(cfg.TransportLatencyWeights) != len(cfg.TransportLatencyValuesMs) {
		return nil, fmt.Errorf("transport latency weights length (%d) must match values length (%d)", len(cfg.TransportLatencyWeights), len(cfg.TransportLatencyValuesMs))
	}

	weights := cfg.TransportLatencyWeights
	if len(weights) == 0 {
		weights = make([]float64, len(cfg.TransportLatencyValuesMs))
		for i := range weights {
			weights[i] = 1.0
		}
	}

	// Normalize + build CDF.
	var sum float64
	for _, w := range weights {
		if w < 0 {
			return nil, fmt.Errorf("transport latency weights must be non-negative")
		}
		sum += w
	}
	if sum == 0 {
		return nil, fmt.Errorf("transport latency weights sum to 0")
	}
	cdf := make([]float64, len(weights))
	var acc float64
	for i, w := range weights {
		acc += w / sum
		cdf[i] = acc
	}
	// Ensure last element is exactly 1.0 for SearchFloat64s edge cases.
	cdf[len(cdf)-1] = 1.0

	return &TransportLatencyModel{
		preset:      cfg.TransportLatencyPreset,
		valuesRTTms: cfg.TransportLatencyValuesMs,
		weights:     weights,
		weightCDF:   cdf,
		jitterRTTms: cfg.TransportLatencyJitterMs,
		spikeRTTms:  cfg.TransportLatencySpikeMs,
		spikeProb:   cfg.TransportLatencySpikeProb,
	}, nil
}

// Metrics tracks cluster behavior
type Metrics struct {
	mu                 sync.RWMutex
	leaderChanges      int64
	proposalsSubmitted int64
	proposalsCompleted int64
	proposalsFailed    int64
	snapshotsCreated   int64
	snapshotsReceived  int64
	connectionsEstab   int64
	connectionsFailed  int64
	leaderHistory      []LeaderChange
	initTime           time.Time
	startTime          time.Time

	// Election timing
	lastLeaderLostTime time.Time       // When leader was lost (LeaderID became 0)
	electionDurations  []ElectionEvent // How long each election took
	currentLeaderID    uint64

	// When we intentionally trigger an election (e.g. stop the leader), record
	// the start time so re-election measurements include failure detection time.
	forcedElectionStart  time.Time
	forcedElectionReason string
	forcedElectionLeader uint64
}

type ElectionEvent struct {
	Label    string
	Duration time.Duration
}

type DurationStats struct {
	Count int
	Min   time.Duration
	Max   time.Duration
	Avg   time.Duration
	P50   time.Duration
	P90   time.Duration
	P99   time.Duration
}

func computeDurationStats(in []time.Duration) DurationStats {
	if len(in) == 0 {
		return DurationStats{}
	}
	ds := make([]time.Duration, len(in))
	copy(ds, in)
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	min := ds[0]
	max := ds[len(ds)-1]
	var total time.Duration
	for _, d := range ds {
		total += d
	}
	avg := total / time.Duration(len(ds))
	return DurationStats{
		Count: len(ds),
		Min:   min,
		Max:   max,
		Avg:   avg,
		P50:   percentileNearestRank(ds, 0.50),
		P90:   percentileNearestRank(ds, 0.90),
		P99:   percentileNearestRank(ds, 0.99),
	}
}

func percentileNearestRank(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}
	// Nearest-rank definition: ceil(p*N)th element (1-indexed).
	rank := int(math.Ceil(p*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

func canonicalElectionLabel(label string) string {
	switch {
	case label == "initial":
		return "initial"
	case strings.HasPrefix(label, "stop leader"):
		return "stop leader"
	case strings.HasPrefix(label, "re-election (leader_lost->elected)"):
		return "re-election (leader_lost->elected)"
	case label == "":
		return "unknown"
	default:
		return label
	}
}

func groupElectionEvents(events []ElectionEvent) map[string][]time.Duration {
	out := make(map[string][]time.Duration)
	for _, e := range events {
		k := canonicalElectionLabel(e.Label)
		out[k] = append(out[k], e.Duration)
	}
	return out
}

func printElectionSummary(events []ElectionEvent) {
	groups := groupElectionEvents(events)
	if len(groups) == 0 {
		fmt.Println("No elections recorded yet")
		return
	}
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	fmt.Println("\nBy type:")
	fmt.Printf("  %-30s %5s %8s %8s %8s %8s %8s %8s\n", "type", "n", "min", "avg", "p50", "p90", "p99", "max")
	for _, k := range keys {
		s := computeDurationStats(groups[k])
		fmt.Printf("  %-30s %5d %8d %8d %8d %8d %8d %8d\n",
			k,
			s.Count,
			s.Min.Milliseconds(),
			s.Avg.Milliseconds(),
			s.P50.Milliseconds(),
			s.P90.Milliseconds(),
			s.P99.Milliseconds(),
			s.Max.Milliseconds(),
		)
	}
}

func (m *Metrics) SetStartTime() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.startTime = time.Now()
}

type LeaderChange struct {
	Timestamp time.Time `json:"timestamp"`
	ShardID   uint64    `json:"shard_id"`
	ReplicaID uint64    `json:"replica_id"`
	LeaderID  uint64    `json:"leader_id"`
	Term      uint64    `json:"term"`
}

func NewMetrics() *Metrics {
	return &Metrics{
		leaderHistory: make([]LeaderChange, 0),
		initTime:      time.Now(),
	}
}

func (m *Metrics) StartForcedElection(reason string, leaderID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.forcedElectionStart = time.Now()
	m.forcedElectionReason = reason
	m.forcedElectionLeader = leaderID
}

func (m *Metrics) RecordLeaderChange(info raftio.LeaderInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	prevLeader := m.currentLeaderID
	newLeader := info.LeaderID

	// Skip if this is the same state we already know about
	if newLeader == prevLeader {
		return
	}

	// If we intentionally triggered an election (e.g. killed the leader), record
	// "stop leader -> new leader" duration. This is the apples-to-apples metric
	// you usually care about for SLA, as it includes failure detection time.
	if newLeader != 0 && !m.forcedElectionStart.IsZero() {
		// Only record once, on the first elected leader after the trigger.
		d := now.Sub(m.forcedElectionStart)
		label := m.forcedElectionReason
		if label == "" {
			label = "forced"
		}
		if m.forcedElectionLeader != 0 {
			label = fmt.Sprintf("%s (leader=%d)", label, m.forcedElectionLeader)
		}
		m.electionDurations = append(m.electionDurations, ElectionEvent{Label: label, Duration: d})
		fmt.Printf(">>> FORCED RE-ELECTION COMPLETE: Node %d in %v\n", newLeader, d)
		m.forcedElectionStart = time.Time{}
		m.forcedElectionReason = ""
		m.forcedElectionLeader = 0
	}

	// Track election timing
	if prevLeader != 0 && newLeader == 0 {
		// Leader lost - start timing (only if not already timing).
		// If a forced election is active (e.g. we killed the leader), we avoid
		// recording the "leader_lost->elected" metric because it excludes failure
		// detection time and will look artificially tiny.
		if m.forcedElectionStart.IsZero() && m.lastLeaderLostTime.IsZero() {
			m.lastLeaderLostTime = now
			fmt.Printf(">>> LEADER LOST at %s (was Node %d)\n", now.Format("15:04:05.000"), prevLeader)
		}
	} else if newLeader != 0 && !m.lastLeaderLostTime.IsZero() {
		// New leader elected after loss - record duration (only when not forced)
		if m.forcedElectionStart.IsZero() {
			electionDuration := now.Sub(m.lastLeaderLostTime)
			m.electionDurations = append(m.electionDurations, ElectionEvent{Label: "re-election (leader_lost->elected)", Duration: electionDuration})
			fmt.Printf(">>> NEW LEADER ELECTED: Node %d in %v\n", newLeader, electionDuration)
		}
		m.lastLeaderLostTime = time.Time{} // Reset
	} else if prevLeader == 0 && newLeader != 0 && len(m.electionDurations) == 0 {
		// Initial leader election (only record once)
		electionDuration := now.Sub(m.startTime)
		m.electionDurations = append(m.electionDurations, ElectionEvent{Label: "initial", Duration: electionDuration})
		fmt.Printf(">>> INITIAL LEADER: Node %d elected in %v\n", newLeader, electionDuration)
	}

	m.currentLeaderID = newLeader

	// Count actual leader changes (not LeaderID=0 events)
	if newLeader != 0 {
		atomic.AddInt64(&m.leaderChanges, 1)
	}

	m.leaderHistory = append(m.leaderHistory, LeaderChange{
		Timestamp: now,
		ShardID:   info.ShardID,
		ReplicaID: info.ReplicaID,
		LeaderID:  info.LeaderID,
		Term:      info.Term,
	})
}

func (m *Metrics) IncrementProposalsSubmitted() {
	atomic.AddInt64(&m.proposalsSubmitted, 1)
}

func (m *Metrics) IncrementProposalsCompleted() {
	atomic.AddInt64(&m.proposalsCompleted, 1)
}

func (m *Metrics) IncrementProposalsFailed() {
	atomic.AddInt64(&m.proposalsFailed, 1)
}

func (m *Metrics) GetStats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	elapsed := time.Since(m.initTime)
	submitted := atomic.LoadInt64(&m.proposalsSubmitted)
	completed := atomic.LoadInt64(&m.proposalsCompleted)

	var throughput float64
	if elapsed.Seconds() > 0 {
		throughput = float64(completed) / elapsed.Seconds()
	}

	// Calculate election stats
	var avgElection, minElection, maxElection time.Duration
	if len(m.electionDurations) > 0 {
		var total time.Duration
		minElection = m.electionDurations[0].Duration
		maxElection = m.electionDurations[0].Duration
		for _, e := range m.electionDurations {
			d := e.Duration
			total += d
			if d < minElection {
				minElection = d
			}
			if d > maxElection {
				maxElection = d
			}
		}
		avgElection = total / time.Duration(len(m.electionDurations))
	}

	return map[string]interface{}{
		"elapsed_seconds":         elapsed.Seconds(),
		"leader_changes":          atomic.LoadInt64(&m.leaderChanges),
		"proposals_submitted":     submitted,
		"proposals_completed":     completed,
		"proposals_failed":        atomic.LoadInt64(&m.proposalsFailed),
		"throughput_per_sec":      throughput,
		"snapshots_created":       atomic.LoadInt64(&m.snapshotsCreated),
		"snapshots_received":      atomic.LoadInt64(&m.snapshotsReceived),
		"connections_established": atomic.LoadInt64(&m.connectionsEstab),
		"connections_failed":      atomic.LoadInt64(&m.connectionsFailed),
		"election_count":          len(m.electionDurations),
		"election_avg_ms":         avgElection.Milliseconds(),
		"election_min_ms":         minElection.Milliseconds(),
		"election_max_ms":         maxElection.Milliseconds(),
	}
}

func (m *Metrics) GetElectionDurations() []ElectionEvent {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]ElectionEvent, len(m.electionDurations))
	copy(result, m.electionDurations)
	return result
}

func (m *Metrics) GetLeaderHistory() []LeaderChange {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]LeaderChange, len(m.leaderHistory))
	copy(result, m.leaderHistory)
	return result
}

// RaftEventListener implements raftio.IRaftEventListener
type RaftEventListener struct {
	metrics *Metrics
	nodeID  uint64
}

func (l *RaftEventListener) LeaderUpdated(info raftio.LeaderInfo) {
	l.metrics.RecordLeaderChange(info)
	fmt.Printf("[Node %d] Leader updated: Shard=%d, Leader=%d, Term=%d\n",
		l.nodeID, info.ShardID, info.LeaderID, info.Term)
}

// SystemEventListener implements raftio.ISystemEventListener
type SystemEventListener struct {
	metrics *Metrics
	nodeID  uint64
}

func (l *SystemEventListener) NodeHostShuttingDown() {
	fmt.Printf("[Node %d] NodeHost shutting down\n", l.nodeID)
}

func (l *SystemEventListener) NodeUnloaded(info raftio.NodeInfo) {
	fmt.Printf("[Node %d] Node unloaded: Shard=%d, Replica=%d\n",
		l.nodeID, info.ShardID, info.ReplicaID)
}

func (l *SystemEventListener) NodeDeleted(info raftio.NodeInfo) {
	fmt.Printf("[Node %d] Node deleted: Shard=%d, Replica=%d\n",
		l.nodeID, info.ShardID, info.ReplicaID)
}

func (l *SystemEventListener) NodeReady(info raftio.NodeInfo) {
	fmt.Printf("[Node %d] Node ready: Shard=%d, Replica=%d\n",
		l.nodeID, info.ShardID, info.ReplicaID)
}

func (l *SystemEventListener) MembershipChanged(info raftio.NodeInfo) {
	fmt.Printf("[Node %d] Membership changed: Shard=%d, Replica=%d\n",
		l.nodeID, info.ShardID, info.ReplicaID)
}

func (l *SystemEventListener) ConnectionEstablished(info raftio.ConnectionInfo) {
	atomic.AddInt64(&l.metrics.connectionsEstab, 1)
	fmt.Printf("[Node %d] Connection established: %s (snapshot=%v)\n",
		l.nodeID, info.Address, info.SnapshotConnection)
}

func (l *SystemEventListener) ConnectionFailed(info raftio.ConnectionInfo) {
	atomic.AddInt64(&l.metrics.connectionsFailed, 1)
	fmt.Printf("[Node %d] Connection failed: %s (snapshot=%v)\n",
		l.nodeID, info.Address, info.SnapshotConnection)
}

func (l *SystemEventListener) SendSnapshotStarted(info raftio.SnapshotInfo) {
	fmt.Printf("[Node %d] Snapshot send started: to=%d, index=%d\n",
		l.nodeID, info.ReplicaID, info.Index)
}

func (l *SystemEventListener) SendSnapshotCompleted(info raftio.SnapshotInfo) {
	fmt.Printf("[Node %d] Snapshot send completed: to=%d, index=%d\n",
		l.nodeID, info.ReplicaID, info.Index)
}

func (l *SystemEventListener) SendSnapshotAborted(info raftio.SnapshotInfo) {
	fmt.Printf("[Node %d] Snapshot send aborted: to=%d, index=%d\n",
		l.nodeID, info.ReplicaID, info.Index)
}

func (l *SystemEventListener) SnapshotReceived(info raftio.SnapshotInfo) {
	atomic.AddInt64(&l.metrics.snapshotsReceived, 1)
	fmt.Printf("[Node %d] Snapshot received: from=%d, index=%d\n",
		l.nodeID, info.From, info.Index)
}

func (l *SystemEventListener) SnapshotRecovered(info raftio.SnapshotInfo) {
	fmt.Printf("[Node %d] Snapshot recovered: index=%d\n",
		l.nodeID, info.Index)
}

func (l *SystemEventListener) SnapshotCreated(info raftio.SnapshotInfo) {
	atomic.AddInt64(&l.metrics.snapshotsCreated, 1)
	fmt.Printf("[Node %d] Snapshot created: index=%d\n",
		l.nodeID, info.Index)
}

func (l *SystemEventListener) SnapshotCompacted(info raftio.SnapshotInfo) {
	fmt.Printf("[Node %d] Snapshot compacted: index=%d\n",
		l.nodeID, info.Index)
}

func (l *SystemEventListener) LogCompacted(info raftio.EntryInfo) {
	fmt.Printf("[Node %d] Log compacted: index=%d\n",
		l.nodeID, info.Index)
}

func (l *SystemEventListener) LogDBCompacted(info raftio.EntryInfo) {
	fmt.Printf("[Node %d] LogDB compacted: index=%d\n",
		l.nodeID, info.Index)
}

// DemoCluster manages the cluster of nodes
type DemoCluster struct {
	config  DemoConfig
	nodes   []*dragonboat.NodeHost
	metrics *Metrics
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

func NewDemoCluster(cfg DemoConfig) *DemoCluster {
	return &DemoCluster{
		config:  cfg,
		nodes:   make([]*dragonboat.NodeHost, 0),
		metrics: NewMetrics(),
		stopCh:  make(chan struct{}),
	}
}

func (c *DemoCluster) Start() error {
	// Clean up previous data
	os.RemoveAll(c.config.DataDir)

	latencyModel, err := buildTransportLatencyModel(c.config)
	if err != nil {
		return err
	}

	// Build initial members map
	initialMembers := make(map[uint64]string)
	for i := 1; i <= c.config.NodeCount; i++ {
		addr := fmt.Sprintf("127.0.0.1:%d", c.config.BasePort+i)
		initialMembers[uint64(i)] = addr
	}

	fmt.Printf("\n=== Starting Raft Cluster ===\n")
	fmt.Printf("Configuration:\n")
	fmt.Printf("  Nodes: %d\n", c.config.NodeCount)
	fmt.Printf("  RTT: %d ms\n", c.config.RTTMillisecond)
	fmt.Printf("  HeartbeatRTT: %d (=%d ms)\n", c.config.HeartbeatRTT,
		c.config.HeartbeatRTT*c.config.RTTMillisecond)
	fmt.Printf("  ElectionRTT: %d (=%d ms)\n", c.config.ElectionRTT,
		c.config.ElectionRTT*c.config.RTTMillisecond)
	fmt.Printf("  CheckQuorum: %v\n", c.config.CheckQuorum)
	fmt.Printf("  PreVote: %v\n", c.config.PreVote)
	fmt.Printf("  SnapshotEntries: %d\n", c.config.SnapshotEntries)
	if c.config.StartupSpreadMs > 0 {
		fmt.Printf("  StartupSpread: %d ms (simulating block propagation delay)\n", c.config.StartupSpreadMs)
	}
	fmt.Printf("\n")

	// Generate random startup delays for each node (simulating block arrival times)
	startupDelays := make([]time.Duration, c.config.NodeCount)
	if c.config.StartupSpreadMs > 0 {
		fmt.Printf("Node startup delays (simulating block propagation):\n")
		for i := 0; i < c.config.NodeCount; i++ {
			delay := time.Duration(rand.Intn(c.config.StartupSpreadMs)) * time.Millisecond
			startupDelays[i] = delay
			fmt.Printf("  Node %d: +%dms\n", i+1, delay.Milliseconds())
		}
		fmt.Println()
	}

	var starts []func() error
	for i := 1; i <= c.config.NodeCount; i++ {
		replicaID := uint64(i)
		addr := initialMembers[replicaID]
		dataDir := filepath.Join(c.config.DataDir, fmt.Sprintf("node%d", i))

		nhc := config.NodeHostConfig{
			WALDir:         dataDir,
			NodeHostDir:    dataDir,
			RTTMillisecond: c.config.RTTMillisecond,
			RaftAddress:    addr,
			RaftEventListener: &RaftEventListener{
				metrics: c.metrics,
				nodeID:  replicaID,
			},
			SystemEventListener: &SystemEventListener{
				metrics: c.metrics,
				nodeID:  replicaID,
			},
		}
		nhc.Expert.TransportFactory = &TransportFactory{
			DelayMillisecond: c.config.TransportMaxDelayMs,
			LatencyModel:     latencyModel,
		}

		nh, err := dragonboat.NewNodeHost(nhc)
		if err != nil {
			return fmt.Errorf("failed to create NodeHost %d: %w", i, err)
		}
		c.nodes = append(c.nodes, nh)

		// Raft node configuration
		rc := config.Config{
			ReplicaID:          replicaID,
			ShardID:            c.config.ShardID,
			CheckQuorum:        c.config.CheckQuorum,
			PreVote:            c.config.PreVote,
			ElectionRTT:        c.config.ElectionRTT,
			HeartbeatRTT:       c.config.HeartbeatRTT,
			SnapshotEntries:    c.config.SnapshotEntries,
			CompactionOverhead: c.config.CompactionOverhead,
			Quiesce:            c.config.Quiesce,
		} // Create state machine factory
		createSM := func(shardID, replicaID uint64) sm.IStateMachine {
			return NewKVStateMachine(shardID, replicaID)
		}

		var startupDelay time.Duration
		if startupDelays[i-1] > 0 {
			startupDelay = startupDelays[i-1]
		} // Apply startup delay if configured
		starts = append(starts, func() error {
			time.Sleep(startupDelay)

			if err := nh.StartReplica(initialMembers, false, createSM, rc); err != nil {
				return fmt.Errorf("failed to start replica %d: %w", i, err)
			}

			fmt.Printf("[Node %d] Started at %s (delay: +%dms)\n", i, addr, startupDelay.Milliseconds())
			return nil
		})
	}

	// Start timing initial election from when we begin starting replicas.
	// (Previously this included an unrelated fixed sleep and skewed results.)
	c.metrics.SetStartTime()

	var wg errgroup.Group
	for _, start := range starts {
		wg.Go(start)
	}
	err = wg.Wait()
	if err != nil {
		return err
	}

	// Wait for cluster to stabilize
	fmt.Printf("\nWaiting for cluster to elect leader...\n")
	time.Sleep(time.Duration(c.config.ElectionRTT*c.config.RTTMillisecond) * time.Millisecond)

	return nil
}

func (c *DemoCluster) Stop() {
	close(c.stopCh)
	c.wg.Wait()

	for i, nh := range c.nodes {
		if nh != nil {
			nh.Close()
			fmt.Printf("[Node %d] Stopped\n", i+1)
		}
	}
}

func (c *DemoCluster) GetLeaderInfo() (uint64, uint64, bool) {
	for _, nh := range c.nodes {
		if nh != nil {
			leaderID, term, ok, err := nh.GetLeaderID(c.config.ShardID)
			if err == nil && ok {
				return leaderID, term, true
			}
		}
	}
	return 0, 0, false
}

func (c *DemoCluster) ProposeValue(value string) error {
	c.metrics.IncrementProposalsSubmitted()

	// Find a node to submit proposal
	for _, nh := range c.nodes {
		if nh == nil {
			continue
		}

		// Use a helper function to properly scope the context cancellation
		err := func() error {
			ctx, cancel := context.WithTimeout(context.Background(),
				time.Duration(c.config.RTTMillisecond*c.config.ElectionRTT)*time.Millisecond)
			defer cancel()

			session := nh.GetNoOPSession(c.config.ShardID)

			_, err := nh.SyncPropose(ctx, session, []byte(value))
			return err
		}()

		if err == nil {
			c.metrics.IncrementProposalsCompleted()
			return nil
		}

		// Try next node if this one failed
		if err == dragonboat.ErrShardNotReady || err == dragonboat.ErrTimeout {
			continue
		}

		c.metrics.IncrementProposalsFailed()
		return err
	}

	c.metrics.IncrementProposalsFailed()
	return fmt.Errorf("no node available to accept proposal")
}

func (c *DemoCluster) RunWorkload() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		ticker := time.NewTicker(time.Duration(c.config.ProposalIntervalMs) * time.Millisecond)
		defer ticker.Stop()

		counter := 0
		for {
			select {
			case <-c.stopCh:
				return
			case <-ticker.C:
				counter++
				value := fmt.Sprintf("value-%d-%d", time.Now().UnixNano(), counter)
				if err := c.ProposeValue(value); err != nil {
					fmt.Printf("Proposal failed: %v\n", err)
				}
			}
		}
	}()
}

func (c *DemoCluster) PrintStats() {
	stats := c.metrics.GetStats()

	fmt.Printf("\n=== Cluster Statistics ===\n")
	fmt.Printf("Elapsed time: %.1f seconds\n", stats["elapsed_seconds"])
	fmt.Printf("Leader changes: %d\n", stats["leader_changes"])
	fmt.Printf("Proposals submitted: %d\n", stats["proposals_submitted"])
	fmt.Printf("Proposals completed: %d\n", stats["proposals_completed"])
	fmt.Printf("Proposals failed: %d\n", stats["proposals_failed"])
	fmt.Printf("Throughput: %.2f ops/sec\n", stats["throughput_per_sec"])
	fmt.Printf("Snapshots created: %d\n", stats["snapshots_created"])
	fmt.Printf("Snapshots received: %d\n", stats["snapshots_received"])
	fmt.Printf("Connections established: %d\n", stats["connections_established"])
	fmt.Printf("Connections failed: %d\n", stats["connections_failed"])

	// Election timing stats
	fmt.Printf("\n=== Election Timing ===\n")
	fmt.Printf("Total elections: %d\n", stats["election_count"])
	if stats["election_count"].(int) > 0 {
		fmt.Printf("Average election time: %d ms\n", stats["election_avg_ms"])
		fmt.Printf("Min election time: %d ms\n", stats["election_min_ms"])
		fmt.Printf("Max election time: %d ms\n", stats["election_max_ms"])

		// Show individual election durations
		events := c.metrics.GetElectionDurations()
		fmt.Printf("Individual elections: ")
		for i, e := range events {
			if i > 0 {
				fmt.Printf(", ")
			}
			if e.Label != "" {
				fmt.Printf("%s=%dms", e.Label, e.Duration.Milliseconds())
			} else {
				fmt.Printf("%dms", e.Duration.Milliseconds())
			}
		}
		fmt.Println()
		printElectionSummary(events)
	}

	if leaderID, term, ok := c.GetLeaderInfo(); ok {
		fmt.Printf("\nCurrent leader: Node %d (Term %d)\n", leaderID, term)
	} else {
		fmt.Printf("\nNo leader currently elected\n")
	}

	// Print leader history
	history := c.metrics.GetLeaderHistory()
	if len(history) > 0 {
		fmt.Printf("\n=== Leader Election History ===\n")
		for i, change := range history {
			elapsed := change.Timestamp.Sub(c.metrics.startTime)
			leaderStr := fmt.Sprintf("Leader=%d", change.LeaderID)
			if change.LeaderID == 0 {
				leaderStr = "Leader=NONE"
			}
			fmt.Printf("%d. [%.3fs] %s, Term=%d\n",
				i+1, elapsed.Seconds(), leaderStr, change.Term)
		}
	}
}

func (c *DemoCluster) StopNode(nodeID int) error {
	if nodeID < 1 || nodeID > len(c.nodes) {
		return fmt.Errorf("invalid node ID: %d", nodeID)
	}

	idx := nodeID - 1
	if c.nodes[idx] == nil {
		return fmt.Errorf("node %d already stopped", nodeID)
	}

	// If we're stopping the current leader, capture "stop -> new leader" timing.
	if leaderID, _, ok := c.GetLeaderInfo(); ok && int(leaderID) == nodeID {
		c.metrics.StartForcedElection("stop leader", leaderID)
	}

	c.nodes[idx].Close()
	c.nodes[idx] = nil
	fmt.Printf("[Node %d] Forcefully stopped\n", nodeID)
	return nil
}

func (c *DemoCluster) RestartNode(nodeID int) error {
	if nodeID < 1 || nodeID > c.config.NodeCount {
		return fmt.Errorf("invalid node ID: %d", nodeID)
	}

	idx := nodeID - 1
	if c.nodes[idx] != nil {
		return fmt.Errorf("node %d is still running", nodeID)
	}

	replicaID := uint64(nodeID)
	addr := fmt.Sprintf("127.0.0.1:%d", c.config.BasePort+nodeID)
	dataDir := filepath.Join(c.config.DataDir, fmt.Sprintf("node%d", nodeID))

	nhc := config.NodeHostConfig{
		WALDir:         dataDir,
		NodeHostDir:    dataDir,
		RTTMillisecond: c.config.RTTMillisecond,
		RaftAddress:    addr,
		RaftEventListener: &RaftEventListener{
			metrics: c.metrics,
			nodeID:  replicaID,
		},
		SystemEventListener: &SystemEventListener{
			metrics: c.metrics,
			nodeID:  replicaID,
		},
	}
	nhc.Expert.TransportFactory = &TransportFactory{DelayMillisecond: c.config.TransportMaxDelayMs}

	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return fmt.Errorf("failed to create NodeHost: %w", err)
	}

	rc := config.Config{
		ReplicaID:          replicaID,
		ShardID:            c.config.ShardID,
		CheckQuorum:        c.config.CheckQuorum,
		PreVote:            c.config.PreVote,
		ElectionRTT:        c.config.ElectionRTT,
		HeartbeatRTT:       c.config.HeartbeatRTT,
		SnapshotEntries:    c.config.SnapshotEntries,
		CompactionOverhead: c.config.CompactionOverhead,
		Quiesce:            c.config.Quiesce,
	}

	createSM := func(shardID, replicaID uint64) sm.IStateMachine {
		return NewKVStateMachine(shardID, replicaID)
	}

	// Restart with empty initial members (rejoining existing cluster)
	if err := nh.StartReplica(nil, false, createSM, rc); err != nil {
		nh.Close()
		return fmt.Errorf("failed to start replica: %w", err)
	}

	c.nodes[idx] = nh
	fmt.Printf("[Node %d] Restarted at %s\n", nodeID, addr)
	return nil
}

func runInteractiveMode(cluster *DemoCluster) {
	fmt.Println("\n=== Interactive Mode ===")
	fmt.Println("Commands:")
	fmt.Println("  stats       - Show cluster statistics")
	fmt.Println("  leader      - Show current leader")
	fmt.Println("  history     - Show leader election history")
	fmt.Println("  elections   - Show election timing stats")
	fmt.Println("  stop <n>    - Stop node n (simulates failure)")
	fmt.Println("  stop-leader - Stop the current leader (apples-to-apples re-election timing)")
	fmt.Println("  start <n>   - Restart node n")
	fmt.Println("  propose <v> - Propose value v")
	fmt.Println("  workload    - Start continuous workload")
	fmt.Println("  sleep <ms>  - Sleep for ms (useful to wait for re-election)")
	fmt.Println("  quit        - Exit")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "stats":
			cluster.PrintStats()

		case "leader":
			if leaderID, term, ok := cluster.GetLeaderInfo(); ok {
				fmt.Printf("Current leader: Node %d (Term %d)\n", leaderID, term)
			} else {
				fmt.Println("No leader currently elected")
			}

		case "history":
			history := cluster.metrics.GetLeaderHistory()
			if len(history) == 0 {
				fmt.Println("No leader changes recorded")
			} else {
				fmt.Println("Leader Election History:")
				for i, change := range history {
					elapsed := change.Timestamp.Sub(cluster.metrics.startTime)
					leaderStr := fmt.Sprintf("Leader=%d", change.LeaderID)
					if change.LeaderID == 0 {
						leaderStr = "Leader=NONE"
					}
					fmt.Printf("%d. [%.3fs] %s, Term=%d\n",
						i+1, elapsed.Seconds(), leaderStr, change.Term)
				}
			}

		case "elections":
			stats := cluster.metrics.GetStats()
			fmt.Printf("\n=== Election Timing Statistics ===\n")
			fmt.Printf("Total elections: %d\n", stats["election_count"])
			if stats["election_count"].(int) > 0 {
				fmt.Printf("Average: %d ms\n", stats["election_avg_ms"])
				fmt.Printf("Min: %d ms\n", stats["election_min_ms"])
				fmt.Printf("Max: %d ms\n", stats["election_max_ms"])

				events := cluster.metrics.GetElectionDurations()
				fmt.Println("\nIndividual election times:")
				for i, e := range events {
					label := e.Label
					if label == "" {
						label = fmt.Sprintf("election #%d", i)
					}
					fmt.Printf("  %s: %v\n", label, e.Duration)
				}
				printElectionSummary(events)
			} else {
				fmt.Println("No elections recorded yet")
			}

		case "stop":
			if len(parts) < 2 {
				fmt.Println("Usage: stop <node_id>")
				continue
			}
			nodeID, err := strconv.Atoi(parts[1])
			if err != nil {
				fmt.Printf("Invalid node ID: %s\n", parts[1])
				continue
			}
			if err := cluster.StopNode(nodeID); err != nil {
				fmt.Printf("Error: %v\n", err)
			}

		case "stop-leader", "stopleader", "killleader":
			leaderID, _, ok := cluster.GetLeaderInfo()
			if !ok || leaderID == 0 {
				fmt.Println("No leader currently elected")
				continue
			}
			if err := cluster.StopNode(int(leaderID)); err != nil {
				fmt.Printf("Error: %v\n", err)
			}

		case "start":
			if len(parts) < 2 {
				fmt.Println("Usage: start <node_id>")
				continue
			}
			nodeID, err := strconv.Atoi(parts[1])
			if err != nil {
				fmt.Printf("Invalid node ID: %s\n", parts[1])
				continue
			}
			if err := cluster.RestartNode(nodeID); err != nil {
				fmt.Printf("Error: %v\n", err)
			}

		case "propose":
			if len(parts) < 2 {
				fmt.Println("Usage: propose <value>")
				continue
			}
			value := strings.Join(parts[1:], " ")
			if err := cluster.ProposeValue(value); err != nil {
				fmt.Printf("Proposal failed: %v\n", err)
			} else {
				fmt.Println("Proposal accepted")
			}

		case "workload":
			fmt.Println("Starting continuous workload...")
			cluster.RunWorkload()

		case "sleep", "wait":
			if len(parts) < 2 {
				fmt.Println("Usage: sleep <ms>")
				continue
			}
			ms, err := strconv.Atoi(parts[1])
			if err != nil || ms < 0 {
				fmt.Printf("Invalid ms: %s\n", parts[1])
				continue
			}
			time.Sleep(time.Duration(ms) * time.Millisecond)

		case "quit", "exit", "q":
			fmt.Println("Shutting down...")
			return

		default:
			fmt.Printf("Unknown command: %s\n", cmd)
		}
	}
}

func main() {
	// Seed random number generator
	rand.Seed(time.Now().UnixNano())

	// Command-line flags
	nodeCount := flag.Int("nodes", 3, "Number of nodes in the cluster")
	rtt := flag.Uint64("rtt", 100, "RTT in milliseconds")
	heartbeatRTT := flag.Uint64("heartbeat", 1, "Heartbeat RTT (in RTT units)")
	electionRTT := flag.Uint64("election", 10, "Election RTT (in RTT units)")
	snapshotEntries := flag.Uint64("snapshot", 100, "Entries between snapshots")
	checkQuorum := flag.Bool("check-quorum", true, "Enable CheckQuorum")
	preVote := flag.Bool("prevote", true, "Enable PreVote")
	quiesce := flag.Bool("quiesce", false, "Enable Quiesce mode")
	proposalInterval := flag.Int("proposal-interval", 500, "Proposal interval in ms (for workload)")
	autoWorkload := flag.Bool("auto-workload", false, "Start automatic workload")
	duration := flag.Int("duration", 0, "Test duration in seconds (0 for interactive)")
	startupSpread := flag.Int("startup-spread", 0, "Max random startup delay per node in ms (simulates block propagation)")
	transportMaxDelayMs := flag.Uint64("transport-max-delay-ms", 0, "Transport delay in ms (for testing network issues)")
	latencyPreset := flag.String("latency-preset", "", "Transport latency preset (models RTT distribution): global-pessimistic | global-measured | latency-aware")
	latencyValues := flag.String("latency-values-ms", "", "Custom RTT samples in ms (comma-separated), used when --latency-preset is empty")
	latencyWeights := flag.String("latency-weights", "", "Optional weights for --latency-values-ms (comma-separated, same length)")
	latencyJitter := flag.Uint64("latency-jitter-ms", 0, "Optional symmetric RTT jitter (ms) applied to sampled RTT")
	latencySpikeMs := flag.Uint64("latency-spike-ms", 0, "Optional RTT spike value (ms) applied with probability --latency-spike-prob")
	latencySpikeProb := flag.Float64("latency-spike-prob", 0, "Probability of RTT spike (0..1)")
	configFile := flag.String("config", "", "Load configuration from JSON file")
	saveConfig := flag.String("save-config", "", "Save current configuration to JSON file")
	trials := flag.Int("trials", 0, "Run N automated trials (start fresh cluster, measure initial election, stop leader, measure stop->new-leader)")
	trialsVerbose := flag.Bool("trials-verbose", false, "Print per-trial results when using --trials")
	trialsTimeoutMs := flag.Int("trials-timeout-ms", 10000, "Timeout per trial phase in ms (waiting for leader / waiting for re-election)")

	flag.Parse()

	// Build configuration
	cfg := DefaultConfig()

	// Load from file if specified
	if *configFile != "" {
		data, err := os.ReadFile(*configFile)
		if err != nil {
			fmt.Printf("Failed to read config file: %v\n", err)
			os.Exit(1)
		}
		if err := json.Unmarshal(data, &cfg); err != nil {
			fmt.Printf("Failed to parse config file: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Loaded configuration from %s\n", *configFile)
	}

	// Override with command-line flags
	cfg.NodeCount = *nodeCount
	cfg.RTTMillisecond = *rtt
	cfg.HeartbeatRTT = *heartbeatRTT
	cfg.ElectionRTT = *electionRTT
	cfg.SnapshotEntries = *snapshotEntries
	cfg.CheckQuorum = *checkQuorum
	cfg.PreVote = *preVote
	cfg.Quiesce = *quiesce
	cfg.ProposalIntervalMs = *proposalInterval
	cfg.TestDurationSec = *duration
	cfg.StartupSpreadMs = *startupSpread
	cfg.TransportMaxDelayMs = *transportMaxDelayMs
	cfg.TransportLatencyPreset = *latencyPreset
	cfg.TransportLatencyJitterMs = *latencyJitter
	cfg.TransportLatencySpikeMs = *latencySpikeMs
	cfg.TransportLatencySpikeProb = *latencySpikeProb

	// Parse custom RTT distribution values/weights if provided.
	if strings.TrimSpace(*latencyValues) != "" {
		vals, err := parseCSVUint64(*latencyValues)
		if err != nil {
			fmt.Printf("Invalid --latency-values-ms: %v\n", err)
			os.Exit(1)
		}
		cfg.TransportLatencyValuesMs = vals
	}
	if strings.TrimSpace(*latencyWeights) != "" {
		ws, err := parseCSVFloat64(*latencyWeights)
		if err != nil {
			fmt.Printf("Invalid --latency-weights: %v\n", err)
			os.Exit(1)
		}
		cfg.TransportLatencyWeights = ws
	}

	// Validate configuration
	if cfg.ElectionRTT <= 2*cfg.HeartbeatRTT {
		fmt.Printf("Warning: ElectionRTT (%d) should be > 2*HeartbeatRTT (%d)\n",
			cfg.ElectionRTT, cfg.HeartbeatRTT)
	}
	if cfg.ElectionRTT < 10*cfg.HeartbeatRTT {
		fmt.Printf("Warning: ElectionRTT (%d) recommended to be ~10x HeartbeatRTT (%d)\n",
			cfg.ElectionRTT, cfg.HeartbeatRTT)
	}

	// Save config if requested
	if *saveConfig != "" {
		data, _ := json.MarshalIndent(cfg, "", "  ")
		if err := os.WriteFile(*saveConfig, data, 0644); err != nil {
			fmt.Printf("Failed to save config: %v\n", err)
		} else {
			fmt.Printf("Configuration saved to %s\n", *saveConfig)
		}
	}

	// Create and start cluster
	if *trials > 0 {
		if err := runTrials(cfg, *trials, *trialsVerbose, time.Duration(*trialsTimeoutMs)*time.Millisecond); err != nil {
			fmt.Printf("Trials failed: %v\n", err)
			os.Exit(1)
		}
		return
	}

	cluster := NewDemoCluster(cfg)
	if err := cluster.Start(); err != nil {
		fmt.Printf("Failed to start cluster: %v\n", err)
		os.Exit(1)
	}

	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Println("\nReceived shutdown signal...")
		cluster.PrintStats()
		cluster.Stop()
		os.Exit(0)
	}()

	// Start workload if requested
	if *autoWorkload {
		cluster.RunWorkload()
	}

	// Run for specified duration or interactive mode
	if cfg.TestDurationSec > 0 {
		fmt.Printf("\nRunning for %d seconds...\n", cfg.TestDurationSec)
		time.Sleep(time.Duration(cfg.TestDurationSec) * time.Second)
		cluster.PrintStats()
		cluster.Stop()
	} else {
		runInteractiveMode(cluster)
		cluster.PrintStats()
		cluster.Stop()
	}
}

func runTrials(cfg DemoConfig, trials int, verbose bool, phaseTimeout time.Duration) error {
	if trials <= 0 {
		return fmt.Errorf("trials must be > 0")
	}
	fmt.Printf("\n=== Trials Mode (%d trials) ===\n", trials)
	fmt.Printf("Config: nodes=%d rtt=%dms electionRTT=%d heartbeatRTT=%d startupSpread=%dms prevote=%v checkQuorum=%v\n",
		cfg.NodeCount, cfg.RTTMillisecond, cfg.ElectionRTT, cfg.HeartbeatRTT, cfg.StartupSpreadMs, cfg.PreVote, cfg.CheckQuorum)
	fmt.Printf("Per-phase timeout: %v\n\n", phaseTimeout)

	var initialAll []time.Duration
	var stopAll []time.Duration
	failures := 0

	for i := 1; i <= trials; i++ {
		// Isolate each trial so stale state doesn't leak across runs.
		trialCfg := cfg
		trialCfg.TestDurationSec = 0
		trialCfg.DataDir = filepath.Join(os.TempDir(), fmt.Sprintf("raft-demo-trial-%d-%d", time.Now().UnixNano(), i))
		trialCfg.BasePort = cfg.BasePort + i*100

		cluster := NewDemoCluster(trialCfg)
		if err := cluster.Start(); err != nil {
			failures++
			if verbose {
				fmt.Printf("trial %d: start failed: %v\n", i, err)
			}
			continue
		}

		leaderID, leaderTerm, ok := waitForLeader(cluster, phaseTimeout)
		if !ok {
			failures++
			if verbose {
				fmt.Printf("trial %d: no leader within %v\n", i, phaseTimeout)
			}
			cluster.Stop()
			continue
		}

		initial, ok := findElectionDuration(cluster.metrics.GetElectionDurations(), "initial")
		if ok {
			initialAll = append(initialAll, initial)
		} else {
			failures++
			if verbose {
				fmt.Printf("trial %d: missing initial election event\n", i)
			}
			cluster.Stop()
			continue
		}

		// Force re-election by stopping the current leader.
		if err := cluster.StopNode(int(leaderID)); err != nil {
			failures++
			if verbose {
				fmt.Printf("trial %d: failed to stop leader %d: %v\n", i, leaderID, err)
			}
			cluster.Stop()
			continue
		}

		stopDur, ok := waitForStopLeaderEvent(cluster, phaseTimeout)
		if ok {
			stopAll = append(stopAll, stopDur)
		} else {
			failures++
			if verbose {
				fmt.Printf("trial %d: no stop-leader re-election event within %v\n", i, phaseTimeout)
			}
			cluster.Stop()
			continue
		}

		if verbose {
			newLeaderID, newLeaderTerm, _ := cluster.GetLeaderInfo()
			fmt.Printf("trial %d: initial=%dms leader=%d term=%d | stop->new=%dms newLeader=%d term=%d\n",
				i,
				initial.Milliseconds(),
				leaderID,
				leaderTerm,
				stopDur.Milliseconds(),
				newLeaderID,
				newLeaderTerm,
			)
		}

		cluster.Stop()
	}

	fmt.Printf("\n=== Trial Summary ===\n")
	fmt.Printf("Completed: %d, failures: %d\n", trials-failures, failures)
	fmt.Println("\nInitial election (start -> leader elected):")
	printTrialStats(initialAll)
	fmt.Println("\nStop-leader re-election (stop leader -> new leader elected):")
	printTrialStats(stopAll)

	return nil
}

func parseCSVUint64(s string) ([]uint64, error) {
	parts := strings.Split(s, ",")
	out := make([]uint64, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		v, err := strconv.ParseUint(p, 10, 64)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no values")
	}
	return out, nil
}

func parseCSVFloat64(s string) ([]float64, error) {
	parts := strings.Split(s, ",")
	out := make([]float64, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		v, err := strconv.ParseFloat(p, 64)
		if err != nil {
			return nil, err
		}
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, fmt.Errorf("invalid float %q", p)
		}
		out = append(out, v)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no values")
	}
	return out, nil
}

func printTrialStats(ds []time.Duration) {
	s := computeDurationStats(ds)
	if s.Count == 0 {
		fmt.Println("  no samples")
		return
	}
	fmt.Printf("  n=%d min=%dms avg=%dms p50=%dms p90=%dms p99=%dms max=%dms\n",
		s.Count,
		s.Min.Milliseconds(),
		s.Avg.Milliseconds(),
		s.P50.Milliseconds(),
		s.P90.Milliseconds(),
		s.P99.Milliseconds(),
		s.Max.Milliseconds(),
	)
}

func waitForLeader(cluster *DemoCluster, timeout time.Duration) (leaderID uint64, term uint64, ok bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		lid, t, ok := cluster.GetLeaderInfo()
		if ok && lid != 0 {
			return lid, t, true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return 0, 0, false
}

func findElectionDuration(events []ElectionEvent, label string) (time.Duration, bool) {
	for _, e := range events {
		if e.Label == label {
			return e.Duration, true
		}
	}
	return 0, false
}

func waitForStopLeaderEvent(cluster *DemoCluster, timeout time.Duration) (time.Duration, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		events := cluster.metrics.GetElectionDurations()
		for _, e := range events {
			if strings.HasPrefix(e.Label, "stop leader") {
				return e.Duration, true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return 0, false
}

type TransportFactory struct {
	DelayMillisecond uint64
	LatencyModel     *TransportLatencyModel
}

type Transport struct {
	raftio.ITransport
	DelayMillisecond uint64
	LatencyModel     *TransportLatencyModel
}

type Connection struct {
	raftio.IConnection
	DelayMillisecond uint64
	LatencyModel     *TransportLatencyModel
}

func (c Connection) SendMessageBatch(batch raftpb.MessageBatch) error {
	if c.LatencyModel != nil && c.LatencyModel.Enabled() {
		if d := c.LatencyModel.SampleOneWayDelay(); d > 0 {
			time.Sleep(d)
		}
		return c.IConnection.SendMessageBatch(batch)
	}

	// Allow DelayMillisecond=0 (default) and any small values without underflowing
	// or passing a negative/zero argument to rand.Intn.
	const minDelay uint64 = 25
	if c.DelayMillisecond > 0 {
		sleepMs := func(max uint64) time.Duration {
			// rand.Intn panics for n <= 0, and its parameter is an int.
			// We want uniform in [0, max] ms; implement that safely.
			//
			// Note: if max is extremely large (>> realistic ms values), we clamp to
			// avoid int overflow, and accept that the distribution is truncated.
			maxInt := uint64(^uint(0) >> 1) // math.MaxInt for the current arch
			if max == 0 {
				return 0
			}
			if max >= maxInt-1 {
				// We can't call Intn(int(max)+1) when max == MaxInt (would overflow).
				// Use a saturated bound of MaxInt-1 => Intn(MaxInt) is safe.
				return time.Duration(rand.Intn(int(maxInt))) * time.Millisecond
			}
			return time.Duration(rand.Intn(int(max)+1)) * time.Millisecond
		}
		if c.DelayMillisecond >= minDelay {
			time.Sleep(time.Duration(minDelay)*time.Millisecond + sleepMs(c.DelayMillisecond-minDelay))
		} else {
			time.Sleep(sleepMs(c.DelayMillisecond))
		}
	}
	return c.IConnection.SendMessageBatch(batch)
}

func (t Transport) GetConnection(ctx context.Context, target string) (raftio.IConnection, error) {
	connection, err := t.ITransport.GetConnection(ctx, target)
	if err != nil {
		return nil, err
	}
	return &Connection{IConnection: connection, DelayMillisecond: t.DelayMillisecond, LatencyModel: t.LatencyModel}, nil
}

func (t TransportFactory) Create(hostConfig config.NodeHostConfig, messageHandler raftio.MessageHandler, chunkHandler raftio.ChunkHandler) raftio.ITransport {
	return &Transport{
		ITransport:       transport.NewChanTransport(hostConfig, messageHandler, chunkHandler),
		DelayMillisecond: t.DelayMillisecond,
		LatencyModel:     t.LatencyModel,
	}
}

func (t TransportFactory) Validate(s string) bool {
	return true
}
