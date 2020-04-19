package raft

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrServerKilled    error = errors.New("server was killed")
	ErrServerNotLeader error = errors.New("this server is not leader")
	ErrNoLeader        error = errors.New("no leader elected")
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

	resetElectionTimeoutCh chan struct{}        // triggers an election timeout reset
	aerCh                  chan *AppendEntryRes // used to process replication log responses from followers

	stateMachine FSM           // client supplied state machine where committed commands will be applied
	config       *Config       // optional configuration overrides various default values
	alive        bool          // true if the server is currently active
	killCh       chan struct{} // used to kill all goroutines in the event of shutting down the server
}

// NewServer() will create and start a new Raft peer.
// This will setup the RPC endpoints and begin the countdown for leader election.
func NewServer(me int, peerAddrs []string, stateMachine FSM, config *Config) (*RaftServer, error) {
	if config == nil {
		config = ConfigDefaults()
	}

	address := peerAddrs[me]
	rs := &RaftServer{
		serverId:               me,
		address:                address,
		peerAddrs:              peerAddrs,
		stateMachine:           stateMachine,
		resetElectionTimeoutCh: make(chan struct{}),
		aerCh:                  make(chan *AppendEntryRes),
		commitIndex:            -1,
		lastApplied:            -1,
		position:               Follower,
		config:                 config,
		alive:                  true,
		killCh:                 make(chan struct{}, 4),
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

type FSM interface {
	Apply(LogEntry) interface{}
}

type Config struct {
	RpcPort             int
	HeartbeatTimeout    time.Duration
	ElectionTimeoutMax  int
	ElectionTimeoutMin  int
	ElectionTimeoutUnit time.Duration
}

func ConfigDefaults() *Config {
	return &Config{
		RpcPort:             8000,
		HeartbeatTimeout:    5 * time.Millisecond,
		ElectionTimeoutMax:  300,
		ElectionTimeoutMin:  150,
		ElectionTimeoutUnit: time.Millisecond,
	}
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
		return -1, -1, ErrServerKilled
	}

	if rs.position != Leader {
		return -1, -1, ErrServerNotLeader
	}

	entry := LogEntry{
		Command: command,
		Term:    rs.currentTerm,
	}

	rs.log = append(rs.log, entry)

	// build AppendEntryReq for each follower:
	aes := map[string]*AppendEntryReq{}
	for i, nextIndex := range rs.nextIndex {
		if i == rs.serverId {
			continue
		}
		es := []LogEntry{}
		for idx := nextIndex; idx <= int64(len(rs.log)-1); idx++ {
			es = append(es, rs.log[idx])
		}

		prevLogIndex := nextIndex - int64(1)
		var prevLogTerm int64
		if prevLogIndex >= 0 {
			prevLogTerm = rs.log[prevLogIndex].Term
		} else {
			prevLogTerm = -1
		}

		aes[rs.peerAddrs[i]] = &AppendEntryReq{
			LeaderId:     rs.serverId,
			Term:         rs.currentTerm,
			LeaderCommit: rs.commitIndex,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      &es,
		}
	}
	sendAppendEntries(aes, rs.aerCh)

	return int64(len(rs.log) - 1), rs.currentTerm, nil
}

// GetLeader() returns what this server thinks is the current leader.
func (rs *RaftServer) GetLeader() (leaderAddr string, err error) {
	rs.lock.RLock()
	defer rs.lock.RUnlock()

	if !rs.IsAlive() {
		return "", ErrServerKilled
	}

	if rs.position == Leader {
		return rs.address, nil
	}

	if rs.position == Follower && rs.votedFor != nil {
		return rs.peerAddrs[*rs.votedFor], nil
	}

	return "", ErrNoLeader
}

// Kill() shuts down this raft server:
// * Stop the 4 long running goroutines
// * Stop TCP listener used for incoming rpc connections
// * Close all channels
func (rs *RaftServer) Kill() {
	rs.lock.Lock()
	defer rs.lock.Unlock()

	if !rs.IsAlive() {
		return
	}

	// stop heartbeatSupervisor, electionSupervisor, appendEntrysupervisor,
	// and acceptRPC goroutines:
	for i := 0; i < 4; i++ {
		rs.killCh <- struct{}{}
	}
	// send myself an RPC to flush the currently blocking lis.Accept():
	sendRPC(rs.address, "RPCHandler.AcceptEntry", nil, nil)

	close(rs.resetElectionTimeoutCh)
	close(rs.aerCh)
	close(rs.killCh)

	rs.alive = false
}

func (rs *RaftServer) GetState() (term int64, isLeader bool, err error) {
	rs.lock.RLock()
	defer rs.lock.RUnlock()

	if !rs.IsAlive() {
		return -1, false, ErrServerKilled
	}

	return rs.currentTerm, rs.position == Leader, nil
}

func (rs *RaftServer) IsAlive() bool {
	return rs.alive
}

func (rs *RaftServer) getLastLogInfo() (logIndex int64, logTerm int64) {
	if len(rs.log)-1 < 0 {
		return -1, -1
	}
	logIndex = int64(len(rs.log) - 1)
	logTerm = rs.log[logIndex].Term
	return logIndex, logTerm
}
