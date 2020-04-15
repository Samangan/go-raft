package raft

import (
	"sync"
)

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

type LogEntry struct {
	Command interface{} // command is the actual state change replicated across the peers
	Term    int64       // the election term when entry was recieved by leader
}

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
