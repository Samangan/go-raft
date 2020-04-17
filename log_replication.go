package raft

import (
	"log"
	"time"
)

// heartbeatSupervisor() is the goroutine responsible for:
// * Sending and receiving periodic heartbeats to all peers to maintain allegiance if Leader
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

				prevLogIndex, prevLogTerm := rs.getPrevLogInfo()
				sendAppendEntries(rs.peerAddrs, rs.serverId, rs.currentTerm, rs.commitIndex, prevLogIndex, prevLogTerm, nil, beatResCh)
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

// applyEntrySupervisor() is the goroutine responsible for
// processing non-heartbeat sendAppendEntries() log replication responses from followers.
func applyEntrySupervisor(rs *RaftServer) {
	for {
		select {
		case r := <-rs.aerCh:
			log.Printf("Recieved AppendEntry() response: %v", r)
			rs.lock.Lock()

			if r.Term > rs.currentTerm {
				becomeFollower(rs, r.Term)
			}

			lastLogIndex := r.LeaderPrevLogIndex + 1
			if r.Success {
				// TODO: We need to be supporting adding multiple entries at once, once we support that in AppendEntries() RPC
				entries := r.AddedEntries
				entry := (*entries)[0]

				rs.matchIndex[r.PeerId] = rs.nextIndex[r.PeerId]
				rs.nextIndex[r.PeerId]++

				log.Printf("lastLogIndex: %v; rs.commitIndex: %v; len(rs.log): %v; rs.currentTerm: %v", lastLogIndex, rs.commitIndex, len(rs.log), rs.currentTerm)

				if lastLogIndex > rs.commitIndex && rs.log[lastLogIndex].Term == rs.currentTerm {
					// Check if we have a majority to commit:
					c := 0
					for i, mi := range rs.matchIndex {
						if i != rs.serverId && mi >= lastLogIndex {
							c++
						}
					}
					if c >= len(rs.peerAddrs)/2 {
						log.Printf("Commited LogEntryIndex %v across %v nodes", lastLogIndex, c)
						rs.commitIndex = lastLogIndex
						rs.stateMachine.Apply(entry)
						rs.lastApplied = lastLogIndex
					}
				}
			} else {
				// TODO: if AppendEntries fails because of log inconsistency
				// decrement rs.nextIndex[peerId] and try again
				// <-----

				log.Printf("TODO: Implement the failure path. BOOM!")
			}
			rs.lock.Unlock()
		}
	}
}

// sendAppendEntries() sends new LogEntrys to followers.
// Leave `entries` nil for heartbeats.

// TODO: This function has too many params, just take in peers and AppendEntryRes!
func sendAppendEntries(peers []string, serverId int, currentTerm, commitIndex, prevLogIndex, prevLogTerm int64, entries *[]LogEntry, aeResCh chan *AppendEntryRes) {
	for i, p := range peers {
		if i == serverId {
			continue
		}

		go func(peer string) {
			req := &AppendEntryReq{
				LeaderId:     serverId,
				Term:         currentTerm,
				LeaderCommit: commitIndex,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
			}
			res := &AppendEntryRes{}
			err := sendRPC(peer, "RPCHandler.AppendEntries", req, res)
			if err != nil {
				log.Printf("Error sending AppendEntries RPC: %v", err)
				// TODO: If not a heartbeat, then we should keep retrying forever
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
	Term         int64 // leader's currentTerm
	LeaderId     int   // so follower can redirect clients
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      *[]LogEntry // new LogEntries to store (nil for heartbeat)
	LeaderCommit int64       // leader's current commitIndex
}

type AppendEntryRes struct {
	Term    int64 // CurrentTerm for leader to update itself
	Success bool  // true if follower contained entry matching prevLogIndex and prevLogTerm

	// Added for leader convenience:
	PeerId             int
	LeaderPrevLogIndex int64
	AddedEntries       *[]LogEntry
}

// AppendEntries is called by the leader to replicate LogEntrys and to maintain a heartbeat
func (rh *RPCHandler) AppendEntries(req *AppendEntryReq, res *AppendEntryRes) error {
	rs := rh.rs
	log.Printf("AppendEntries -> [%v]: \n", rs.address)

	rs.lock.Lock()
	defer rs.lock.Unlock()

	res.Term = rs.currentTerm
	res.PeerId = rs.serverId
	res.LeaderPrevLogIndex = req.PrevLogIndex

	if req.Term < rs.currentTerm {
		// ignore request from a node with a less recent term
		res.Success = false
		return nil
	}

	if req.PrevLogIndex != -1 && (int64(len(rs.log)-1) < req.PrevLogIndex || rs.log[req.PrevLogIndex].Term != req.PrevLogTerm) {
		// Reply false if log doesn’t contain an entry at prevLogIndex
		// whose term matches prevLogTerm

		// TODO: I should add an error reason to explain why Im returning success=false each time
		res.Success = false
		return nil
	}

	// Add new entries to rs.log if not a heartbeat:
	if req.Entries != nil {
		log.Printf("New Entries: %v", req.Entries)

		// TODO: If an existing entry conflicts with a new one (same index
		// but different terms), delete the existing entry and all that
		// follow it

		// Append any new entries not already in the log
		i := req.PrevLogIndex + 1
		for _, e := range *req.Entries {
			if i > int64(len(rs.log)-1) {
				rs.log = append(rs.log, e)
			}
			i++
		}
		res.AddedEntries = req.Entries
	}

	if req.LeaderCommit > rs.commitIndex {
		rs.commitIndex = min(req.LeaderCommit, int64(len(rs.log)-1))
	}

	// Apply any newly commited log entries to this server's state machine:
	if rs.commitIndex > rs.lastApplied {
		rs.lastApplied++
		e := rs.log[rs.lastApplied]
		rs.stateMachine.Apply(e)
	}

	if rs.position == Candidate || rs.position == Leader && req.Term > rs.currentTerm {
		becomeFollower(rs, req.Term)
	} else if rs.position == Follower && req.Term > rs.currentTerm {
		rs.currentTerm = req.Term
	}

	// reset election timeout to maintain allegiance to leader:
	rs.resetElectionTimeoutCh <- true

	res.Success = true
	return nil
}
