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

	t.Run("Adds one new entry to log", func(t *testing.T) {
		log := []LogEntry{}
		rs := &RaftServer{
			serverId:               1,
			currentTerm:            2,
			resetElectionTimeoutCh: make(chan struct{}, 1),
			log:                    log,
			commitIndex:            -1,
			lastApplied:            -1,
		}
		rpcHandler := &RPCHandler{rs}

		req := &AppendEntryReq{
			Term:         2,
			LeaderId:     0,
			PrevLogIndex: -1,
			PrevLogTerm:  -1,
			LeaderCommit: -1,
			Entries: &[]LogEntry{
				LogEntry{
					Term:    2,
					Command: []byte{42},
				},
			},
		}
		res := &AppendEntryRes{}

		err := rpcHandler.AppendEntries(req, res)
		assert.Equal(t, err, nil)
		assert.Equal(t, res.Success, true)

		assert.Equal(t, len(rs.log), 1)
		assert.Equal(t, rs.log[0].Term, int64(2))
		assert.Equal(t, rs.log[0].Command, []byte{42})

		// TODO: Mock statemachine and assert it's not called yet
		assert.Equal(t, rs.commitIndex, int64(-1))
		assert.Equal(t, rs.lastApplied, int64(-1))
		assert.Equal(t, res.Term, rs.currentTerm)
		assert.Equal(t, res.PeerId, rs.serverId)
		assert.Equal(t, res.LeaderPrevLogIndex, req.PrevLogIndex)
	})

	t.Run("Adds more than one entry to log", func(t *testing.T) {
		log := []LogEntry{}
		rs := &RaftServer{
			serverId:               1,
			currentTerm:            2,
			resetElectionTimeoutCh: make(chan struct{}, 1),
			log:                    log,
			commitIndex:            -1,
			lastApplied:            -1,
		}
		rpcHandler := &RPCHandler{rs}

		req := &AppendEntryReq{
			Term:         2,
			LeaderId:     0,
			PrevLogIndex: -1,
			PrevLogTerm:  -1,
			LeaderCommit: -1,
			Entries: &[]LogEntry{
				LogEntry{
					Term:    2,
					Command: []byte{42},
				},
				LogEntry{
					Term:    2,
					Command: []byte{43},
				},
			},
		}
		res := &AppendEntryRes{}

		err := rpcHandler.AppendEntries(req, res)
		assert.Equal(t, err, nil)
		assert.Equal(t, res.Success, true)

		assert.Equal(t, len(rs.log), 2)
		assert.Equal(t, rs.log[0].Term, int64(2))
		assert.Equal(t, rs.log[0].Command, []byte{42})
		assert.Equal(t, rs.log[1].Term, int64(2))
		assert.Equal(t, rs.log[1].Command, []byte{43})

		// TODO: Mock statemachine and assert it's not called yet
		assert.Equal(t, rs.commitIndex, int64(-1))
		assert.Equal(t, rs.lastApplied, int64(-1))
		assert.Equal(t, res.Term, rs.currentTerm)
		assert.Equal(t, res.PeerId, rs.serverId)
		assert.Equal(t, res.LeaderPrevLogIndex, req.PrevLogIndex)
	})

	t.Run("Adds one new entry to existing log", func(t *testing.T) {
		log := []LogEntry{
			LogEntry{
				Term:    1,
				Command: []byte{41},
			},
		}
		rs := &RaftServer{
			serverId:               1,
			currentTerm:            2,
			resetElectionTimeoutCh: make(chan struct{}, 1),
			log:                    log,
			commitIndex:            -1,
			lastApplied:            -1,
		}
		rpcHandler := &RPCHandler{rs}

		req := &AppendEntryReq{
			Term:     2,
			LeaderId: 0,

			PrevLogIndex: 0, // TODO: THere's a bug where PrevLogIndex is -1 and it's also inconsistent. Make a failing test for that.
			PrevLogTerm:  1,

			LeaderCommit: -1,
			Entries: &[]LogEntry{
				LogEntry{
					Term:    2,
					Command: []byte{42},
				},
			},
		}
		res := &AppendEntryRes{}

		err := rpcHandler.AppendEntries(req, res)
		assert.Equal(t, err, nil)
		assert.Equal(t, res.Success, true)

		assert.Equal(t, len(rs.log), 2)
		assert.Equal(t, rs.log[1].Term, int64(2))
		assert.Equal(t, rs.log[1].Command, []byte{42})

		// TODO: Mock statemachine and assert it's not called yet
		assert.Equal(t, rs.commitIndex, int64(-1))
		assert.Equal(t, rs.lastApplied, int64(-1))
		assert.Equal(t, res.Term, rs.currentTerm)
		assert.Equal(t, res.PeerId, rs.serverId)
		assert.Equal(t, res.LeaderPrevLogIndex, req.PrevLogIndex)
	})

	t.Run("Adds only one new entry from multiple sent", func(t *testing.T) {
		log := []LogEntry{
			LogEntry{
				Term:    1,
				Command: []byte{41},
			},
		}
		rs := &RaftServer{
			serverId:               1,
			currentTerm:            2,
			resetElectionTimeoutCh: make(chan struct{}, 1),
			log:                    log,
			commitIndex:            -1,
			lastApplied:            -1,
		}
		rpcHandler := &RPCHandler{rs}

		req := &AppendEntryReq{
			Term:     2,
			LeaderId: 0,

			PrevLogIndex: -1,
			PrevLogTerm:  -1,

			LeaderCommit: -1,
			Entries: &[]LogEntry{
				LogEntry{
					Term:    1,
					Command: []byte{41},
				},
				LogEntry{
					Term:    2,
					Command: []byte{42},
				},
			},
		}
		res := &AppendEntryRes{}

		err := rpcHandler.AppendEntries(req, res)
		assert.Equal(t, err, nil)
		assert.Equal(t, res.Success, true)

		assert.Equal(t, len(rs.log), 2)

		assert.Equal(t, rs.log[0].Term, int64(1))
		assert.Equal(t, rs.log[0].Command, []byte{41})
		assert.Equal(t, rs.log[1].Term, int64(2))
		assert.Equal(t, rs.log[1].Command, []byte{42})

		// TODO: Mock statemachine and assert it's not called yet
		assert.Equal(t, rs.commitIndex, int64(-1))
		assert.Equal(t, rs.lastApplied, int64(-1))
		assert.Equal(t, res.Term, rs.currentTerm)
		assert.Equal(t, res.PeerId, rs.serverId)
		assert.Equal(t, res.LeaderPrevLogIndex, req.PrevLogIndex)
	})

	t.Run("Existing entry conflicts are removed", func(t *testing.T) {
		l := []LogEntry{
			LogEntry{
				Term:    1,
				Command: []byte{41},
			},
			// below conflicts should be deleted and replaced by req.Entries:
			LogEntry{
				Term:    1,
				Command: []byte{42},
			},
			LogEntry{
				Term:    1,
				Command: []byte{43},
			},
		}
		rs := &RaftServer{
			serverId:               1,
			currentTerm:            2,
			resetElectionTimeoutCh: make(chan struct{}, 1),
			log:                    l,
			commitIndex:            -1,
			lastApplied:            -1,
		}
		rpcHandler := &RPCHandler{rs}

		req := &AppendEntryReq{
			Term:     2,
			LeaderId: 0,

			PrevLogIndex: 0,
			PrevLogTerm:  1,

			LeaderCommit: -1,
			Entries: &[]LogEntry{
				LogEntry{
					Term:    2,
					Command: []byte{0},
				},
				LogEntry{
					Term:    2,
					Command: []byte{1},
				},
			},
		}
		res := &AppendEntryRes{}

		err := rpcHandler.AppendEntries(req, res)
		assert.Equal(t, err, nil)
		assert.Equal(t, res.Success, true)

		assert.Equal(t, len(rs.log), 3)
		assert.Equal(t, rs.log[0].Term, int64(1))
		assert.Equal(t, rs.log[0].Command, []byte{41})
		assert.Equal(t, rs.log[1].Term, int64(2))
		assert.Equal(t, rs.log[1].Command, []byte{0})
		assert.Equal(t, rs.log[2].Term, int64(2))
		assert.Equal(t, rs.log[2].Command, []byte{1})

		// TODO: Mock statemachine and assert it's not called yet
		assert.Equal(t, rs.commitIndex, int64(-1))
		assert.Equal(t, rs.lastApplied, int64(-1))
		assert.Equal(t, res.Term, rs.currentTerm)
		assert.Equal(t, res.PeerId, rs.serverId)
		assert.Equal(t, res.LeaderPrevLogIndex, req.PrevLogIndex)
	})
}
