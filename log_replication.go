package raft

import (
	"log"
	"time"
)

// heartbeatSupervisor() is the goroutine responsible for:
// * Sending periodic heartbeats to all peers to maintain allegiance if Leader
func heartbeatSupervisor(rs *RaftServer) {
	timeout := 1 * time.Second // TODO: Make these configurable so I can have long debug times locally

	ticker := time.NewTicker(timeout)
	defer ticker.Stop()

	beatResCh := make(chan *AppendEntryRes, len(rs.peerAddrs))

	for {
		select {
		case <-ticker.C:
			rs.lock.RLock()
			if rs.position == Leader {
				log.Printf("Sending heartbeats. . .")
				sendAppendEntries(rs.peerAddrs, rs.serverId, rs.currentTerm, nil, rs.commitIndex, beatResCh)
			}
			rs.lock.RUnlock()

		case r := <-beatResCh:
			rs.lock.Lock()
			if r.Term > rs.currentTerm {
				becomeFollower(rs, r.Term)
			}
			rs.lock.Unlock()
		}
	}
}

// sendAppendEntries() sends new LogEntrys to followers.
// Leave `entries` nil for heartbeats.
func sendAppendEntries(peers []string, serverId int, currentTerm int64, entries *[]LogEntry, commitIndex int64, aeResCh chan *AppendEntryRes) {
	for i, p := range peers {
		if i == serverId {
			continue
		}

		go func(peer string) {
			req := &AppendEntryReq{
				Term:     currentTerm,
				LeaderId: serverId,
				// TODO:
				//PrevLogEntryIndex: 0,
				//PrevLogTerm:       0,
				Entries:      entries,
				LeaderCommit: commitIndex,
			}
			res := &AppendEntryRes{}
			err := sendRPC(peer, "RPCHandler.AppendEntries", req, res)
			if err != nil {
				log.Printf("Error sending AppendEntries RPC: %v", err)
			} else {
				aeResCh <- res
			}
		}(p)
	}
}

//
// AppendEntry RPC endpoint
//
type AppendEntryReq struct {
	Term              int64 // leader's currentTerm
	LeaderId          int   // so follower can redirect clients
	PrevLogEntryIndex int64
	PrevLogTerm       int64
	Entries           *[]LogEntry // new LogEntrys to store (empty for heartbeat)
	LeaderCommit      int64       // leader's current commitIndex
}

type AppendEntryRes struct {
	Term    int64 // CurrentTerm for leader to update itself
	Success bool  // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries is called by the leader to replicate LogEntrys and to maintain a heartbeat
func (rh *RPCHandler) AppendEntries(req *AppendEntryReq, res *AppendEntryRes) error {
	rs := rh.rs
	log.Printf("AppendEntries -> [%v]: \n", rs.address)

	rs.lock.Lock()
	res.Term = rs.currentTerm

	if req.Term < rs.currentTerm {
		// ignore request from a node with a less recent term
		res.Success = false
		res.Term = rs.currentTerm
		return nil
	}

	if rs.position == Candidate || rs.position == Leader && req.Term > rs.currentTerm {
		becomeFollower(rs, req.Term)
	} else if rs.position == Follower && req.Term > rs.currentTerm {
		rs.currentTerm = req.Term
	}
	rs.lock.Unlock()

	// reset election timeout to maintain allegiance to leader:
	rs.resetElectionTimeoutCh <- true

	// TODO: Finish

	return nil
}
