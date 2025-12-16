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
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
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
	electionDurations  []time.Duration // How long each election took
	currentLeaderID    uint64
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

	// Track election timing
	if prevLeader != 0 && newLeader == 0 {
		// Leader lost - start timing (only if not already timing)
		if m.lastLeaderLostTime.IsZero() {
			m.lastLeaderLostTime = now
			fmt.Printf(">>> LEADER LOST at %s (was Node %d)\n", now.Format("15:04:05.000"), prevLeader)
		}
	} else if newLeader != 0 && !m.lastLeaderLostTime.IsZero() {
		// New leader elected after loss - record duration
		electionDuration := now.Sub(m.lastLeaderLostTime)
		m.electionDurations = append(m.electionDurations, electionDuration)
		fmt.Printf(">>> NEW LEADER ELECTED: Node %d in %v\n", newLeader, electionDuration)
		m.lastLeaderLostTime = time.Time{} // Reset
	} else if prevLeader == 0 && newLeader != 0 && len(m.electionDurations) == 0 {
		// Initial leader election (only record once)
		electionDuration := now.Sub(m.startTime)
		m.electionDurations = append(m.electionDurations, electionDuration)
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
		minElection = m.electionDurations[0]
		maxElection = m.electionDurations[0]
		for _, d := range m.electionDurations {
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

func (m *Metrics) GetElectionDurations() []time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]time.Duration, len(m.electionDurations))
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
		nhc.Expert.TransportFactory = &TransportFactory{DelayMillisecond: c.config.TransportMaxDelayMs}

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

	time.Sleep(5 * time.Second)
	c.metrics.SetStartTime()

	var wg errgroup.Group
	for _, start := range starts {
		wg.Go(start)
	}
	err := wg.Wait()
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
		durations := c.metrics.GetElectionDurations()
		fmt.Printf("Individual elections: ")
		for i, d := range durations {
			if i > 0 {
				fmt.Printf(", ")
			}
			fmt.Printf("%dms", d.Milliseconds())
		}
		fmt.Println()
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
	fmt.Println("  start <n>   - Restart node n")
	fmt.Println("  propose <v> - Propose value v")
	fmt.Println("  workload    - Start continuous workload")
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

				durations := cluster.metrics.GetElectionDurations()
				fmt.Println("\nIndividual election times:")
				for i, d := range durations {
					label := "initial"
					if i > 0 {
						label = fmt.Sprintf("re-election #%d", i)
					}
					fmt.Printf("  %s: %v\n", label, d)
				}
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
	configFile := flag.String("config", "", "Load configuration from JSON file")
	saveConfig := flag.String("save-config", "", "Save current configuration to JSON file")

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

type TransportFactory struct {
	DelayMillisecond uint64
}

type Transport struct {
	raftio.ITransport
	DelayMillisecond uint64
}

type Connection struct {
	raftio.IConnection
	DelayMillisecond uint64
}

func (c Connection) SendMessageBatch(batch raftpb.MessageBatch) error {
	const minDelay = 25
	time.Sleep(time.Duration(minDelay+rand.Intn(int(c.DelayMillisecond-minDelay))) * time.Millisecond)
	return c.IConnection.SendMessageBatch(batch)
}

func (t Transport) GetConnection(ctx context.Context, target string) (raftio.IConnection, error) {
	connection, err := t.ITransport.GetConnection(ctx, target)
	if err != nil {
		return nil, err
	}
	return &Connection{IConnection: connection, DelayMillisecond: t.DelayMillisecond}, nil
}

func (t TransportFactory) Create(hostConfig config.NodeHostConfig, messageHandler raftio.MessageHandler, chunkHandler raftio.ChunkHandler) raftio.ITransport {
	return &Transport{ITransport: transport.NewChanTransport(hostConfig, messageHandler, chunkHandler), DelayMillisecond: t.DelayMillisecond}
}

func (t TransportFactory) Validate(s string) bool {
	return true
}
