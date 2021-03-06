package raft

import (
	"errors"
	"log"
	"time"
)

var (
	ErrOldTerm         error = errors.New("leader has outdated term")
	ErrLogInconsistent error = errors.New("leader has inconsistent log")
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
				entries := r.Entries
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
			} else if r.Error == ErrLogInconsistent {
				log.Println("Sending another AppendEntry() to resolve log inconsistency")
				// retry with more data (eventually they will line up):
				rs.nextIndex[r.PeerId]--
				aes := map[string]*AppendEntryReq{
					rs.peerAddrs[r.PeerId]: rs.getAppendEntryReqForPeer(r.PeerId),
				}
				sendAppendEntries(aes, rs.aerCh)
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
	Term    int64 // CurrentTerm for leader to update itself
	Success bool  // true if follower contained entry matching prevLogIndex and prevLogTerm
	Error   error

	// Added for leader convenience:
	PeerId             int
	LeaderPrevLogIndex int64
	Entries            *[]LogEntry
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
		res.Error = ErrOldTerm
		return nil
	}

	if req.PrevLogIndex != -1 && (int64(len(rs.log)-1) < req.PrevLogIndex || rs.log[req.PrevLogIndex].Term != req.PrevLogTerm) {
		// Reply false if log doesn’t contain an entry at prevLogIndex
		// whose term matches prevLogTerm
		res.Success = false
		res.Error = ErrLogInconsistent
		return nil
	}

	if req.Term > rs.currentTerm {
		becomeFollower(rs, req.Term)
	}

	// Add new entries to rs.log if not a heartbeat:
	if req.Entries != nil {
		log.Printf("Processing %v New Entries: ", len(*req.Entries))

		// Append any new entries not already in the log
		i := req.PrevLogIndex + 1
		for _, e := range *req.Entries {
			if i < int64(len(rs.log))-1 && rs.log[i].Term != e.Term {
				log.Printf("Removing conflicts on index: %v", i)
				// `e` conflicts with an existing entry.
				// delete this entry and all that follow it:
				for idx, len := i, int64(len(rs.log)); i < len; i++ {
					rs.log = append(rs.log[:idx], rs.log[idx+1:]...)
				}
			}

			if i > int64(len(rs.log)-1) {
				rs.log = append(rs.log, e)
			}
			i++
		}
		res.Entries = req.Entries
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

	// reset election timeout to maintain allegiance to leader:
	rs.resetElectionTimeoutCh <- struct{}{}

	res.Success = true
	return nil
}
