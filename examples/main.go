package main

// USAGE (from pkg root):
// * docker build -f examples/DemoDockerFile -t raft .
// * docker-compose -f examples/docker-compose.yml  up
// * ...

import (
	"encoding/json"
	"github.com/Samangan/go-raft"
	"log"
	"os"
	"strconv"
	"time"
)

type KVStore struct {
	store map[string]string
}

func (kv KVStore) Apply(entry raft.LogEntry) interface{} {
	log.Printf("[Apply()] Applying new entry to stateMachine")

	command := &KVCommand{}
	err := json.Unmarshal(entry.Command, command)
	if err != nil {
		panic(err)
	}
	log.Printf("-- command: %v --", command)

	if command.Operation == Put {
		kv.store[command.Key] = command.Value
	}
	return command.Value
}

type KVCommand struct {
	Key       string
	Value     string
	Operation Op
}
type Op string

const (
	Put    Op = "PUT"
	Delete Op = "DELETE"
)

func main() {
	// NOTE: defined in docker-compose.yml
	peerAddrs := []string{"node0", "node1", "node2", "node3"}
	me, _ := strconv.Atoi(os.Getenv("ID"))

	log.Printf("[CLIENT] Starting up server [%v: %v]", me, peerAddrs[me])
	kvs := KVStore{make(map[string]string)}

	r, err := raft.NewServer(me, peerAddrs, kvs)
	if err != nil {
		log.Fatal(err)
	}

	// TODO: Testing this is going to be hard until I have an actual client usage of this.
	// I should make:
	// * Unit tests
	// * "Integration" test suite that will test through common things with multiple nodes

	// NOTE: For now doing some more manual testing on each node:
	for _, err := r.GetLeader(); err != nil; {
		log.Printf("[CLIENT] Waiting for a leader to be elected. . .")
		time.Sleep(10 * time.Second)
		_, err = r.GetLeader()
	}

	if _, isLeader := r.GetState(); isLeader {
		log.Println("[CLIENT] Calling ApplyEntry. . .")
		c := &KVCommand{
			Key:       "x",
			Value:     "42",
			Operation: Put,
		}
		b, err := json.Marshal(c)
		if err != nil {
			panic(err)
		}
		r.ApplyEntry(b)

		log.Println("[CLIENT] Calling ApplyEntry again. . .")
		c = &KVCommand{
			Key:       "y",
			Value:     "1000",
			Operation: Put,
		}
		b, err = json.Marshal(c)
		if err != nil {
			panic(err)
		}
		r.ApplyEntry(b)

		c.Value = "-1"
		b, err = json.Marshal(c)
		if err != nil {
			panic(err)
		}
		r.ApplyEntry(b)

		// TODO: This isnt going to ALWAYS work because, each time I immediately
		// add the new entry to the leader's log and Im only sending one entry each time
		// and more importantly I havent implemented the AppendEntry() response failure logic
		// to work backwards sending older entries until the followers are caught up.
		// <---- (This is a good case to test that that works though)
	}

	for {
	}
}
