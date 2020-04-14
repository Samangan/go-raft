package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

// NewServer() will create and start a new Raft peer.
// This will setup the RPC endpoints and begin the countdown for leader election.
func NewServer(me int, peerAddrs []string, applyCh chan ApplyMessage) (*RaftServer, error) {
	address := peerAddrs[me]
	rs := &RaftServer{
		serverId:               me,
		address:                address,
		peerAddrs:              peerAddrs,
		applyCh:                applyCh,
		resetElectionTimeoutCh: make(chan bool),

		position: Follower,
	}

	err := initRPC(address, rs)
	if err != nil {
		return nil, err
	}

	go initElectionTimeout(rs)

	return rs, nil
}

type RaftServer struct {
	// Persistent state
	// TODO: These need to be written to disk each time they change
	//       and read from disk on NewServer()
	currentTerm int64      // latest term server has seen
	votedFor    *int       // candidateID that recieved vote in currentTerm (null if none)
	log         []LogEntry // log of replicated commands

	// Volatile state:
	commitIndex int64 // index of highest known committed LogEntry
	lastApplied int64 // index of highest LogEntry applied to this server

	// Volatile, Leader only state:
	nextIndex  *[]int64 // index of next LogEntry to send to each server
	matchIndex *[]int64 // index of the highest LogEntry known to be replicated to each server

	// OTHER SHIT I NEED TO ORGANIZE:
	serverId  int      // the index of it's address in `peerAddrs`
	address   string   // this server's network address
	peerAddrs []string // peer network addresses (sorted consistently across all nodes)

	lock     sync.RWMutex      // locks the mutable elements of RaftServer (TODO: Make this obv here what is mutable vs immutable)
	position ElectoralPosition // current position in current term

	resetElectionTimeoutCh chan bool // triggers an election timeout reset

	applyCh chan ApplyMessage // client will use to listen for newly commited log entries
}

type ApplyMessage struct{}

// StartAgreement() starts to process a new entry in the replicated log.
// It will return immediately. Use `applyCh` to listen to know when `entry` has been successfully
// committed to the replicated log.
func (*RaftServer) StartAgreement(entry interface{}) (index int64, term int64, isLeader bool) {
	return -1, -1, false
}

func (*RaftServer) GetState() (term int64, isLeader bool) {
	return -1, false
}

func (*RaftServer) Kill() {}

type LogEntry struct{}

// TODO: Should probably put below in different go files / package than main. But Im going to get this all working first before refactoring.
type RPCEndpointData struct { // TODO: Not shitty name
	rs *RaftServer
}

//
// Leader Election:
//
type VoteReq struct{}
type VoteRes struct{}

func (rd *RPCEndpointData) RequestVote(req *VoteReq, res *VoteRes) error {
	log.Printf("RequestVote -> [%v]: \n", rd.rs.address)

	// TODO: Implement
	// <---

	return nil
}

func sendRequestVotes(peers []string, me int) {
	for i, p := range peers {
		if i == me {
			continue
		}

		go func(peer string) {
			req := &VoteReq{}
			res := &VoteRes{}
			err := sendRPC(peer, "RPCEndpointData.RequestVote", req, res)
			if err != nil {
				log.Printf("Error sending RPC: %v \n", err)
			}
			// TODO: Do something with errors and response output:
			// * If votes received from majority of servers: become leader
		}(p)
	}
}

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
	sendRequestVotes(rs.peerAddrs, rs.serverId)
}

//
// Log Replication:
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
func (rd *RPCEndpointData) AppendEntries(req *AppendEntryReq, res *AppendEntryRes) error {
	log.Printf("AppendEntries -> [%v]: \n", rd.rs.address)

	res.Term = rd.rs.currentTerm

	if req.Term < rd.rs.currentTerm {
		// ignore request from a node with a less recent term
		res.Success = false
		return nil
	}

	// reset election timeout:
	rd.rs.resetElectionTimeoutCh <- true

	// TODO: If this server is currently a candidate, convert to a folower of this new leader

	// TODO: Finish

	return nil
}
