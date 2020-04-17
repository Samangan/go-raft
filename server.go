package raft

import (
	"errors"
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
	lastApplied int64 // index of highest LogEntry applied to this server's state machine

	// Volatile, Leader only state:
	nextIndex  []int64 // index of next LogEntry to send to each server
	matchIndex []int64 // index of the highest LogEntry known to be replicated to each server

	lock      sync.RWMutex      // locks the mutable elements of RaftServer (TODO: Make this obv here what is mutable vs immutable)
	serverId  int               // the index of it's address in `peerAddrs`
	address   string            // this server's network address
	peerAddrs []string          // peer network addresses (sorted consistently across all nodes)
	position  ElectoralPosition // current position in current term

	resetElectionTimeoutCh chan bool            // triggers an election timeout reset
	aerCh                  chan *AppendEntryRes // used to process replication log responses from followers

	stateMachine FSM // client supplied state machine where committed commands will be applied
}

// NewServer() will create and start a new Raft peer.
// This will setup the RPC endpoints and begin the countdown for leader election.
func NewServer(me int, peerAddrs []string, stateMachine FSM) (*RaftServer, error) {
	address := peerAddrs[me]
	rs := &RaftServer{
		serverId:               me,
		address:                address,
		peerAddrs:              peerAddrs,
		stateMachine:           stateMachine,
		resetElectionTimeoutCh: make(chan bool),
		aerCh:                  make(chan *AppendEntryRes), // TODO: Should this be a buffered channel?
		commitIndex:            -1,
		lastApplied:            -1,
		position:               Follower,
	}

	err := initRPC(address, rs)
	if err != nil {
		return nil, err
	}

	go electionSupervisor(rs)

	go heartbeatSupervisor(rs)

	return rs, nil
}

type LogEntry struct {
	Command []byte // command is the actual state change replicated across the peers
	Term    int64  // the election term when entry was recieved by leader

}
type ApplyMessage struct {
	Entry   LogEntry
	Success bool
}
type FSM interface {
	Apply(LogEntry) interface{}
}

// ApplyEntry() starts to process a new command in the replicated log.
// It will return immediately. rs.stateMachine.Apply() will be called when `command` has been successfully
// committed and applied to a node.
//
// If this server is not the leader, then an error will be returned and the client must
// use `GetLeader()` to communicate with the known leader instead.
//
// This means that the client must do some sort of leader forwarding if they dont want to propagate
// this error to the client of their service.
func (rs *RaftServer) ApplyEntry(command []byte) (index int64, term int64, err error) {
	rs.lock.Lock()
	defer rs.lock.Unlock()

	if !rs.IsAlive() {
		return -1, -1, errors.New("Server was killed")
	}

	if rs.position != Leader {
		return -1, -1, errors.New("Server not leader")
	}

	entry := LogEntry{
		Command: command,
		Term:    rs.currentTerm,
	}

	rs.log = append(rs.log, entry)

	prevLogIndex, prevLogTerm := rs.getPrevLogInfo()
	entries := &[]LogEntry{entry}

	// TODO: I shouldn't only send the latest LogEntry to each peer, I need to send all the ones
	//       that they are missing via rs.nextIndex.
	//       (Also be sure to change the nextIndex/matchIndex success logic below to match this change)
	sendAppendEntries(rs.peerAddrs, rs.serverId, rs.currentTerm, rs.commitIndex, prevLogIndex, prevLogTerm, entries, rs.aerCh)

	return prevLogIndex + 1, rs.currentTerm, nil
}

// GetLeader() returns what this server thinks is the current leader.
func (rs *RaftServer) GetLeader() (leaderAddr string, err error) {
	rs.lock.RLock()
	defer rs.lock.RUnlock()

	if rs.position == Leader {
		return rs.address, nil
	}

	if rs.position == Follower && rs.votedFor != nil {
		return rs.peerAddrs[*rs.votedFor], nil
	}

	return "", errors.New("No leader elected yet")
}

func (*RaftServer) Kill() {}

func (rs *RaftServer) GetState() (term int64, isLeader bool) {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.currentTerm, rs.position == Leader
}

func (*RaftServer) IsAlive() bool {
	return true
}

func (rs *RaftServer) getLastLogInfo() (lastLogIndex int64, lostLogTerm int64) {
	lastLogIndex = int64(len(rs.log) - 1)
	var lastLogTerm int64
	if lastLogIndex > 0 {
		lastLogTerm = rs.log[lastLogIndex].Term
	} else {
		lastLogTerm = -1
	}
	return lastLogIndex, lastLogTerm
}

// TODO: Combine both of these into one function
func (rs *RaftServer) getPrevLogInfo() (prevLogIndex int64, prevLogTerm int64) {
	if len(rs.log) > 1 {
		prevLogIndex := int64(len(rs.log) - 2)
		prevLogTerm = rs.log[prevLogIndex].Term
		return prevLogIndex, prevLogTerm
	}
	return -1, -1
}
