package raft

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type NopStore struct{}

func (NopStore) Apply(entry LogEntry) interface{} {
	return nil
}

// AppendEntries() RPC tests:
func TestHeartbeat(t *testing.T) {
	t.Run("Reject request from leader with an old term", func(t *testing.T) {
		rs := &RaftServer{
			currentTerm: 2,
		}
		rpcHandler := &RPCHandler{rs}

		req := &AppendEntryReq{
			Term:     1,
			LeaderId: 0,
		}
		res := &AppendEntryRes{}

		err := rpcHandler.AppendEntries(req, res)
		assert.Equal(t, err, nil)
		assert.Equal(t, res.Success, false)
		assert.Equal(t, res.Error, ErrOldTerm)
		assert.Equal(t, res.Term, rs.currentTerm)
	})

	t.Run("Reject request from leader with an inconsistent log", func(t *testing.T) {
		// inconsistent index:
		log := []LogEntry{}
		rs := &RaftServer{
			currentTerm: 2,
			log:         log,
		}
		rpcHandler := &RPCHandler{rs}

		req := &AppendEntryReq{
			Term:         2,
			LeaderId:     0,
			PrevLogIndex: 0,
			PrevLogTerm:  1,
		}
		res := &AppendEntryRes{}

		err := rpcHandler.AppendEntries(req, res)
		assert.Equal(t, err, nil)
		assert.Equal(t, res.Success, false)
		assert.Equal(t, res.Error, ErrLogInconsistent)
		assert.Equal(t, res.Term, rs.currentTerm)

		// inconsistent term:
		log = []LogEntry{
			LogEntry{
				Term:    1,
				Command: nil,
			},
		}
		rs = &RaftServer{
			currentTerm: 2,
			log:         log,
		}
		req = &AppendEntryReq{
			Term:         2,
			LeaderId:     0,
			PrevLogIndex: 0,
			PrevLogTerm:  2,
		}
		res = &AppendEntryRes{}

		err = rpcHandler.AppendEntries(req, res)
		assert.Equal(t, err, nil)
		assert.Equal(t, res.Success, false)
		assert.Equal(t, res.Error, ErrLogInconsistent)
		assert.Equal(t, res.Term, rs.currentTerm)
	})

	t.Run("HeartBeat with empty log works", func(t *testing.T) {
		rs := &RaftServer{
			serverId:               1,
			currentTerm:            2,
			resetElectionTimeoutCh: make(chan struct{}, 1),
		}
		rpcHandler := &RPCHandler{rs}

		req := &AppendEntryReq{
			Term:         2,
			LeaderId:     0,
			PrevLogIndex: -1,
			PrevLogTerm:  -1,
		}
		res := &AppendEntryRes{}

		err := rpcHandler.AppendEntries(req, res)
		assert.Equal(t, err, nil)
		assert.Equal(t, res.Success, true)
		assert.Equal(t, res.Term, rs.currentTerm)
		assert.Equal(t, res.PeerId, rs.serverId)
		assert.Equal(t, res.LeaderPrevLogIndex, req.PrevLogIndex)
	})

	t.Run("HeartBeat with log works", func(t *testing.T) {
		log := []LogEntry{
			LogEntry{
				Term:    1,
				Command: nil,
			},
		}
		rs := &RaftServer{
			serverId:               1,
			currentTerm:            2,
			resetElectionTimeoutCh: make(chan struct{}, 1),
			log:                    log,
		}
		rpcHandler := &RPCHandler{rs}

		req := &AppendEntryReq{
			Term:         2,
			LeaderId:     0,
			PrevLogIndex: 0,
			PrevLogTerm:  1,
		}
		res := &AppendEntryRes{}

		err := rpcHandler.AppendEntries(req, res)
		assert.Equal(t, err, nil)
		assert.Equal(t, res.Success, true)
		assert.Equal(t, res.Term, rs.currentTerm)
		assert.Equal(t, res.PeerId, rs.serverId)
		assert.Equal(t, res.LeaderPrevLogIndex, req.PrevLogIndex)
	})

	t.Run("HeartBeat will apply a newly commited log entry", func(t *testing.T) {
		log := []LogEntry{
			LogEntry{
				Term:    1,
				Command: nil,
			},
		}
		rs := &RaftServer{
			serverId:               1,
			currentTerm:            2,
			resetElectionTimeoutCh: make(chan struct{}, 1),
			log:                    log,
			commitIndex:            -1,
			lastApplied:            -1,
			stateMachine:           NopStore{}, // TODO: Mock this and ensure it was called
		}
		rpcHandler := &RPCHandler{rs}

		req := &AppendEntryReq{
			Term:         2,
			LeaderId:     0,
			PrevLogIndex: 0,
			PrevLogTerm:  1,
			LeaderCommit: 0,
		}
		res := &AppendEntryRes{}

		err := rpcHandler.AppendEntries(req, res)
		assert.Equal(t, err, nil)
		assert.Equal(t, res.Success, true)

		assert.Equal(t, rs.commitIndex, req.LeaderCommit)
		assert.Equal(t, rs.lastApplied, int64(0))
		assert.Equal(t, res.Term, rs.currentTerm)
		assert.Equal(t, res.PeerId, rs.serverId)
		assert.Equal(t, res.LeaderPrevLogIndex, req.PrevLogIndex)
	})
}

func TestAppendEntry(t *testing.T) {

	// TODO: Implement
}
