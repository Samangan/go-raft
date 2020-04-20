package raft

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

// RequestVote RPC tests:
func TestRequestVote(t *testing.T) {
	t.Run("Don't vote for candidate with an old term", func(t *testing.T) {
		rs := &RaftServer{
			currentTerm: 2,
		}
		rpcHandler := &RPCHandler{rs}

		req := &VoteReq{
			Term:        1,
			CandidateId: 0,
		}
		res := &VoteRes{}

		err := rpcHandler.RequestVote(req, res)
		assert.Equal(t, err, nil)
		assert.Equal(t, res.VoteGranted, false)
	})

	t.Run("Don't vote for candidate if you have already voted this term", func(t *testing.T) {
		i := 1
		rs := &RaftServer{
			currentTerm: 2,
			votedFor:    &i,
		}
		rpcHandler := &RPCHandler{rs}

		req := &VoteReq{
			Term:        2,
			CandidateId: 0,
		}
		res := &VoteRes{}

		err := rpcHandler.RequestVote(req, res)
		assert.Equal(t, err, nil)
		assert.Equal(t, res.VoteGranted, false)
	})

	t.Run("Vote for candidate if you have not voted this term", func(t *testing.T) {
		rs := &RaftServer{
			currentTerm:            2,
			votedFor:               nil,
			resetElectionTimeoutCh: make(chan struct{}, 1),
		}
		rpcHandler := &RPCHandler{rs}

		req := &VoteReq{
			Term:        2,
			CandidateId: 0,
		}
		res := &VoteRes{}

		err := rpcHandler.RequestVote(req, res)
		assert.Equal(t, err, nil)
		assert.Equal(t, res.VoteGranted, true)
	})

	t.Run("Vote for candidate if you have already voted for them this term", func(t *testing.T) {
		i := 0
		rs := &RaftServer{
			currentTerm:            2,
			votedFor:               &i,
			resetElectionTimeoutCh: make(chan struct{}, 1),
		}
		rpcHandler := &RPCHandler{rs}

		req := &VoteReq{
			Term:        2,
			CandidateId: 0,
		}
		res := &VoteRes{}

		err := rpcHandler.RequestVote(req, res)
		assert.Equal(t, err, nil)
		assert.Equal(t, res.VoteGranted, true)
	})

	t.Run("Vote for candidate if you have already voted in previous term", func(t *testing.T) {
		i := 4
		rs := &RaftServer{
			currentTerm:            2,
			votedFor:               &i,
			resetElectionTimeoutCh: make(chan struct{}, 1),
		}
		rpcHandler := &RPCHandler{rs}

		req := &VoteReq{
			Term:        3,
			CandidateId: 0,
		}
		res := &VoteRes{}

		err := rpcHandler.RequestVote(req, res)
		assert.Equal(t, err, nil)
		assert.Equal(t, res.VoteGranted, true)
	})

	t.Run("Don't vote for candidate if their log is out of date", func(t *testing.T) {
		log := []LogEntry{
			LogEntry{
				Term:    1,
				Command: nil,
			},
		}

		rs := &RaftServer{
			currentTerm:            2,
			votedFor:               nil,
			resetElectionTimeoutCh: make(chan struct{}, 1),
			log:                    log,
		}
		rpcHandler := &RPCHandler{rs}

		req := &VoteReq{
			Term:         2,
			CandidateId:  0,
			LastLogIndex: -1,
			LastLogTerm:  -1,
		}
		res := &VoteRes{}

		err := rpcHandler.RequestVote(req, res)
		assert.Equal(t, err, nil)
		assert.Equal(t, res.VoteGranted, false)

	})

}
