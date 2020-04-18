package raft

import (
	"log"
	"time"
)

// heartbeatSupervisor() is the goroutine responsible for
// sending and receiving periodic heartbeats to all peers to maintain allegiance
func heartbeatSupervisor(rs *RaftServer) {
	ticker := time.NewTicker(rs.config.HeartbeatTimeout)
	defer ticker.Stop()

	beatResCh := make(chan *AppendEntryRes, len(rs.peerAddrs))

	for {
		select {
		case <-ticker.C:
			rs.lock.RLock()
			if rs.position == Leader {
				log.Printf("Sending heartbeats. . .")

				aes := map[string]*AppendEntryReq{}
				for i, p := range rs.peerAddrs {
					if i == rs.serverId {
						continue
					}
					aes[p] = &AppendEntryReq{
						LeaderId:     rs.serverId,
						Term:         rs.currentTerm,
						LeaderCommit: rs.commitIndex,
						// heartbeats don't send any new LogEntries:
						PrevLogIndex: -1,
						PrevLogTerm:  -1,
						Entries:      nil,
					}
				}
				sendAppendEntries(aes, beatResCh)
			}
			rs.lock.RUnlock()

		case r := <-beatResCh:
			rs.lock.Lock()
			if r.Term > rs.currentTerm {
				becomeFollower(rs, r.Term)
			}
			rs.lock.Unlock()
		case <-rs.killCh:
			log.Println("Shutting off heartbeatSupervisor. . .")
			return
		}
	}
}

// applyEntrySupervisor() is the goroutine responsible for
// processing non-heartbeat sendAppendEntries() log replication responses from followers.
func applyEntrySupervisor(rs *RaftServer) {
	for {
		select {
		case r := <-rs.aerCh:
			rs.lock.Lock()
			if r.Term > rs.currentTerm {
				becomeFollower(rs, r.Term)
			}

			if r.Success {
				entries := r.AddedEntries
				for i, e := range *entries {
					rs.matchIndex[r.PeerId] = rs.nextIndex[r.PeerId]
					rs.nextIndex[r.PeerId]++

					idx := r.LeaderPrevLogIndex + 1 + int64(i)
					if idx > rs.commitIndex && rs.log[idx].Term == rs.currentTerm {
						// Check if we have a majority to commit:
						c := 0
						for i, mi := range rs.matchIndex {
							if i != rs.serverId && mi >= idx {
								c++
							}
						}
						if c >= len(rs.peerAddrs)/2+1 {
							log.Printf("====Commited LogEntryIndex %v across %v nodes====", idx, c)
							rs.commitIndex = idx
							rs.stateMachine.Apply(e)
							rs.lastApplied = idx
						}
					}
				}
			} else {
				// TODO: if AppendEntries fails because of log inconsistency:
				// * Decrement rs.nextIndex[peerId]
				// * Call sendAppendEntries() with a req map only for this peer
				//   (now that I have allowed for that) to retry
				log.Printf("TODO: Implement the failure path. BOOM!")
			}
			rs.lock.Unlock()
		case <-rs.killCh:
			log.Println("Shutting off applyEntrySupervisor. . .")
			return
		}
	}
}

// sendAppendEntries() sends new LogEntrys to followers.
// Leave `entries` nil for heartbeats.
func sendAppendEntries(aes map[string]*AppendEntryReq, aeResCh chan *AppendEntryRes) {
	for peer, req := range aes {
		go func(p string) {
			res := &AppendEntryRes{}
			err := sendRPC(p, "RPCHandler.AppendEntries", req, res)
			if err != nil {
				log.Printf("Error sending AppendEntries RPC: %v", err)
				// TODO: If not a heartbeat, then we should keep retrying forever
			} else {
				aeResCh <- res
			}
		}(peer)
	}
}

//
// AppendEntry RPC endpoint
//
type AppendEntryReq struct {
	Term         int64       // leader's currentTerm
	LeaderId     int         // so follower can redirect clients
	PrevLogIndex int64       // LogEntry index previous to the first entry in `Entries` below
	PrevLogTerm  int64       // Term for that previous LogEntry to the first newest entry
	Entries      *[]LogEntry // new LogEntries to store (nil for heartbeat)
	LeaderCommit int64       // leader's current commitIndex
}

type AppendEntryRes struct {
	Term    int64  // CurrentTerm for leader to update itself
	Success bool   // true if follower contained entry matching prevLogIndex and prevLogTerm
	Error   string // TODO: Make these actual error types instead of matching on strings

	// Added for leader convenience:
	PeerId             int
	LeaderPrevLogIndex int64
	AddedEntries       *[]LogEntry
}

// AppendEntries() is called by the leader to replicate LogEntrys and to maintain a heartbeat
func (rh *RPCHandler) AppendEntries(req *AppendEntryReq, res *AppendEntryRes) error {
	rs := rh.rs
	rs.lock.Lock()
	defer rs.lock.Unlock()

	res.Term = rs.currentTerm
	res.PeerId = rs.serverId
	res.LeaderPrevLogIndex = req.PrevLogIndex

	if req.Term < rs.currentTerm {
		// ignore request from a node with a less recent term
		res.Success = false
		res.Error = "wrong_term"
		return nil
	}

	if req.PrevLogIndex != -1 && (int64(len(rs.log)-1) < req.PrevLogIndex || rs.log[req.PrevLogIndex].Term != req.PrevLogTerm) {
		// Reply false if log doesn’t contain an entry at prevLogIndex
		// whose term matches prevLogTerm
		res.Success = false
		res.Error = "log_inconsistent"
		return nil
	}

	// Add new entries to rs.log if not a heartbeat:
	if req.Entries != nil {
		log.Printf("Adding %v New Entries: ", len(*req.Entries))

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
