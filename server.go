package raft

import (
	"log"
)

func NewServer(me int, peerAddrs []string) (*RaftServer, error) {
	address := peerAddrs[me]
	rs := &RaftServer{
		serverId: me,
		address:  address,
	}

	err := initRPC(address, rs)
	if err != nil {
		return nil, err
	}

	return rs, nil
}

type RaftServer struct {
	// Persistent state (TODO: Updated on stable storage before responding to RPCs):

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

	position ElectoralPosition // current position in current term
}

func (*RaftServer) Start() {}

// func Stop/Kill?

func (*RaftServer) GetState() {}

type LogEntry struct{}

// TODO: Should probably put below in different go files / package than main. But Im going to get this all working first before refactoring.
type RPCEndpointData struct {
	rs *RaftServer
}

//
// Leader Election:
//
type VoteReq struct{}
type VoteRes struct{}

func (rd *RPCEndpointData) RequestVote(req *VoteReq, res *VoteRes) error {
	return nil
}

type ElectoralPosition string

const (
	Follower  ElectoralPosition = "FOLLOWER"
	Candidate ElectoralPosition = "CANDIDATE"
	Leader    ElectoralPosition = "LEADER"
)

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
	log.Printf("AppendEntries [%v]: \n", rd.rs.address)

	res.Term = rd.rs.currentTerm

	if req.Term < rd.rs.currentTerm {
		// ignore request from a node with a less recent term
		res.Success = false
		return nil
	}

	// TODO: Finish

	return nil
}
