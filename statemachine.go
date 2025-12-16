package main

import (
	"encoding/json"
	"io"
	"sync"

	sm "github.com/lni/dragonboat/v4/statemachine"
)

// KVStateMachine is a simple key-value store state machine
type KVStateMachine struct {
	mu        sync.RWMutex
	shardID   uint64
	replicaID uint64
	data      map[string]string
	count     uint64 // Total number of updates applied
}

// NewKVStateMachine creates a new KV state machine instance
func NewKVStateMachine(shardID, replicaID uint64) *KVStateMachine {
	return &KVStateMachine{
		shardID:   shardID,
		replicaID: replicaID,
		data:      make(map[string]string),
	}
}

// Update applies a command to the state machine
func (s *KVStateMachine) Update(entry sm.Entry) (sm.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Simple protocol: just store the value with auto-generated key
	key := string(entry.Cmd)
	s.data[key] = key
	s.count++
	
	return sm.Result{Value: s.count}, nil
}

// Lookup queries the state machine
func (s *KVStateMachine) Lookup(query interface{}) (interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	switch q := query.(type) {
	case string:
		if q == "__count__" {
			return s.count, nil
		}
		if val, ok := s.data[q]; ok {
			return val, nil
		}
		return nil, nil
	case []byte:
		key := string(q)
		if val, ok := s.data[key]; ok {
			return val, nil
		}
		return nil, nil
	default:
		return s.count, nil
	}
}

// SaveSnapshot saves the state machine state to the writer
func (s *KVStateMachine) SaveSnapshot(w io.Writer, _ sm.ISnapshotFileCollection, _ <-chan struct{}) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	snapshot := struct {
		Data  map[string]string `json:"data"`
		Count uint64            `json:"count"`
	}{
		Data:  s.data,
		Count: s.count,
	}
	
	return json.NewEncoder(w).Encode(snapshot)
}

// RecoverFromSnapshot restores state machine state from the reader
func (s *KVStateMachine) RecoverFromSnapshot(r io.Reader, _ []sm.SnapshotFile, _ <-chan struct{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	var snapshot struct {
		Data  map[string]string `json:"data"`
		Count uint64            `json:"count"`
	}
	
	if err := json.NewDecoder(r).Decode(&snapshot); err != nil {
		return err
	}
	
	s.data = snapshot.Data
	s.count = snapshot.Count
	return nil
}

// Close closes the state machine
func (s *KVStateMachine) Close() error {
	return nil
}
