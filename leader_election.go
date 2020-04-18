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

var (
	maxElectionTimeout  int
	minElectionTimeout  int
	electionTimeoutUnit time.Duration
)

// electionSupervisor() is the primary election goroutine responsible for:
// * Election Timeout ticking and resetting
// * Starting an election after the election timeout
// * Vote counting during an election phase
func electionSupervisor(rs *RaftServer) {
	rand.Seed(time.Now().UTC().UnixNano())

	maxElectionTimeout = rs.config.ElectionTimeoutMax
	minElectionTimeout = rs.config.ElectionTimeoutMin
	electionTimeoutUnit = rs.config.ElectionTimeoutUnit

	t := int64(rand.Intn(maxElectionTimeout-minElectionTimeout) + minElectionTimeout)
	d := time.Duration(t * int64(electionTimeoutUnit))
	log.Printf("Election Timeout: %v ", t)

	ticker := time.NewTicker(d)
	defer ticker.Stop()

	voteCh := make(chan *VoteRes)
	voteCount := 0

	for {
		select {
		case <-rs.resetElectionTimeoutCh:
			ticker = resetElectionTimer(ticker)
		case t := <-ticker.C:
			// TODO: make these debug logs only
			log.Printf("Election Timeout @ %v \n", t)

			rs.lock.Lock()
			if rs.position != Leader {
				if rs.position == Follower {
					log.Printf("Transitioning to Candidate: [%v -> CANDIDATE]\n", rs.position)
					rs.position = Candidate
				}

				ticker = resetElectionTimer(ticker)
				voteCount = 0
				startElection(rs, voteCh)
			}
			rs.lock.Unlock()
		case v := <-voteCh:
			// Tally vote:
			rs.lock.Lock()
			if v.Term > rs.currentTerm {
				becomeFollower(rs, v.Term)
			}

			if v.Term == rs.currentTerm && rs.position == Candidate {
				voteCount++
				if voteCount >= len(rs.peerAddrs)/2+1 {
					becomeLeader(rs, voteCount)
				}
			}
			rs.lock.Unlock()
		case <-rs.killCh:
			log.Println("Shutting off electionSupervisor. . .")
			return
		}
	}

}

func resetElectionTimer(ticker *time.Ticker) *time.Ticker {
	log.Printf("Reset Election Timeout")

	// PERFORMANCE TODO: This is allocating a new channel each heartbeat lol...
	//                   Change initElectionTimeout() to just use time.Sleep() manually.
	ticker.Stop()

	t := int64(rand.Intn(maxElectionTimeout-minElectionTimeout) + minElectionTimeout)
	d := time.Duration(t * int64(electionTimeoutUnit))
	return time.NewTicker(d)
}

// Warning: need to acquire rs.lock before calling.
func startElection(rs *RaftServer, voteCh chan *VoteRes) {
	log.Printf("Starting a new election: Term=%v", rs.currentTerm+1)

	rs.currentTerm++
	rs.votedFor = &rs.serverId
	lastLogIndex, lastLogTerm := rs.getLastLogInfo()
	sendRequestVotes(rs.peerAddrs, rs.serverId, rs.currentTerm, lastLogIndex, lastLogTerm, voteCh)
}

func sendRequestVotes(peers []string, serverId int, currentTerm, lastLogIndex, lastLogTerm int64, voteCh chan *VoteRes) {
	for i, p := range peers {
		if i == serverId {
			continue
		}

		go func(peer string) {
			req := &VoteReq{
				Term:         currentTerm,
				CandidateId:  serverId,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			res := &VoteRes{}
			err := sendRPC(peer, "RPCHandler.RequestVote", req, res)
			if err != nil {
				log.Printf("Error sending RequestVote RPC: %v \n", err)
			} else {
				voteCh <- res
			}
		}(p)
	}
}

// becomeFollower() will be called once a candidate or leader finds a
// more up to date Leader to follow.
// Warning: need to acquire rs.lock before calling.
func becomeFollower(rs *RaftServer, newTerm int64) {
	log.Printf("Transitioning to Follower: [%v -> Follower]\n", rs.position)
	rs.position = Follower
	rs.currentTerm = newTerm
}

func becomeLeader(rs *RaftServer, voteCount int) {
	log.Printf("Transitioning to Leader: [%v -> LEADER] with %v votes", rs.position, voteCount)
	rs.position = Leader

	// init nextIndex / matchIndex:
	lastLogIndex, _ := rs.getLastLogInfo()
	rs.nextIndex = make([]int64, len(rs.peerAddrs))
	rs.matchIndex = make([]int64, len(rs.peerAddrs))
	for i, _ := range rs.nextIndex {
		rs.nextIndex[i] = lastLogIndex + 1
	}

	// TODO: Ideally I would kill this applyEntrySupervisor goroutine when this node
	//       stops being a leader.
	go applyEntrySupervisor(rs)
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
		lastLogIndex, lastLogTerm := rs.getLastLogInfo()
		if lastLogIndex <= req.LastLogIndex && lastLogTerm <= req.LastLogTerm {
			log.Printf("Voting for candidate")
			rs.votedFor = &req.CandidateId

			// reset election timeout:
			rs.resetElectionTimeoutCh <- true

			res.Term = req.Term
			res.VoteGranted = true
			return nil
		}
	}

	return nil
}
