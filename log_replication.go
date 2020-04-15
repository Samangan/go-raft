package raft

import (
	"log"
)

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

	rs.lock.RLock()
	defer rs.lock.RUnlock()

	res.Term = rs.currentTerm

	if req.Term < rs.currentTerm {
		// ignore request from a node with a less recent term
		res.Success = false
		res.Term = rs.currentTerm
		return nil
	}

	// reset election timeout:
	rs.resetElectionTimeoutCh <- true

	// TODO: If this server is currently a candidate, convert to a folower of this new leader
	// TODO: If this server is a leader and req.Term is higher than it's current term
	//       then also convert this to a follower.

	// TODO: Finish

	return nil
}
