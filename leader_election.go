package raft

import (
	"log"
	"math/rand"
	"time"
)

type ElectoralPosition string

const (
	Follower  ElectoralPosition = "FOLLOWER"
	Candidate ElectoralPosition = "CANDIDATE"
	Leader    ElectoralPosition = "LEADER"
)

func initElectionTimeout(rs *RaftServer) {
	rand.Seed(time.Now().UTC().UnixNano())
	timeout := int64(rand.Intn(100)) + 1 // TODO: Remove the debug LOONG timeouts

	log.Printf("Election Timeout: %v ", timeout)

	d := time.Duration(timeout * int64(time.Second))
	ticker := time.NewTicker(d)
	defer ticker.Stop()

	for {
		select {
		case <-rs.resetElectionTimeoutCh:
			ticker = resetElectionTimer(ticker, d)
		case t := <-ticker.C:
			log.Printf("Election Timeout @ %v \n", t) // TODO: make these debug logs only

			rs.lock.Lock()
			if rs.position != Leader {
				// ??? TODO: I assume leaders dont have an election timeout ???
				if rs.position == Follower {
					log.Printf("Transitioning to Candidate: [%v -> CANDIDATE]\n", rs.position)
					rs.position = Candidate
				}

				ticker = resetElectionTimer(ticker, d)
				startElection(rs)
			}
			rs.lock.Unlock()
		}
	}

}

func resetElectionTimer(t *time.Ticker, d time.Duration) *time.Ticker {
	log.Printf("Reset Election Timeout")

	// PERFORMANCE TODO: This is allocating a new channel each heartbeat lol...
	//                   Change initElectionTimeout() to just use time.Sleep() manually.
	t.Stop()
	return time.NewTicker(d)
}

// Warning: need to acquire rs.lock before calling.
func startElection(rs *RaftServer) {
	log.Printf("Starting a new election: Term=%v", rs.currentTerm+1)

	rs.currentTerm++
	rs.votedFor = &rs.serverId
	sendRequestVotes(rs.peerAddrs, rs.serverId, rs.currentTerm)
}

func sendRequestVotes(peers []string, serverId int, currentTerm int64) {
	for i, p := range peers {
		if i == serverId {
			continue
		}

		go func(peer string) {
			req := &VoteReq{
				Term:        currentTerm,
				CandidateId: serverId,
			}
			res := &VoteRes{}
			err := sendRPC(peer, "RPCHandler.RequestVote", req, res)
			if err != nil {
				log.Printf("Error sending RPC: %v \n", err)
			}
			// TODO: Do something with errors and response output:
			// * If votes received from majority of servers: become leader
		}(p)
	}
}

//
// RequestVote RPC endpoint
//
type VoteReq struct {
	Term        int64 // candidate's term
	CandidateId int   // candidate serverId requesting the vote

	LastLogIndex int64 // index of the candidate's last LogEntry
	LastLogTerm  int64 // term of the candidate's last LogEntry

}
type VoteRes struct {
	Term        int64 // this node's currentTerm
	VoteGranted bool  // true if canidate received vote
}

func (rh *RPCHandler) RequestVote(req *VoteReq, res *VoteRes) error {
	rs := rh.rs
	log.Printf("RequestVote -> [%v]: \n", rs.address)

	rs.lock.RLock()
	defer rs.lock.RUnlock()

	res.Term = rs.currentTerm
	res.VoteGranted = false

	if req.Term < rs.currentTerm {
		// ignore request from a node with a less recent term
		return nil
	}

	// if receiver hasn't voted yet and has at least as up to date of log then vote yes
	if rs.votedFor == nil || *rs.votedFor == req.CandidateId {
		if rs.commitIndex <= req.LastLogIndex {
			if rs.commitIndex == 0 || rs.log[rs.commitIndex-1].Term <= req.LastLogTerm {
				log.Printf("Voting for candidate")

				res.Term = rs.currentTerm
				res.VoteGranted = true
				return nil
			}
		}
	}

	return nil
}
