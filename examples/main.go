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

// A very simple example KVStore client:
type KVStore struct {
	store map[string]string
}

func (kv KVStore) Apply(entry raft.LogEntry) interface{} {
	command := &KVCommand{}
	err := json.Unmarshal(entry.Command, command)
	if err != nil {
		panic(err)
	}

	log.Printf("[Apply()] Applying new entry to stateMachine")
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
	peerAddrs := []string{"node0", "node1", "node2", "node3", "node4"}
	me, _ := strconv.Atoi(os.Getenv("ID"))

	log.Printf("[CLIENT] Starting up server [%v: %v]", me, peerAddrs[me])
	kvs := KVStore{make(map[string]string)}
	config := &raft.Config{
		RpcPort:             8000,
		HeartbeatTimeout:    1 * time.Second,
		ElectionTimeoutMax:  50,
		ElectionTimeoutMin:  10,
		ElectionTimeoutUnit: time.Second,
	}

	r, err := raft.NewServer(me, peerAddrs, kvs, config)
	if err != nil {
		log.Fatal(err)
	}

	for _, err := r.GetLeader(); err != nil; {
		log.Printf("[CLIENT] Waiting for a leader to be elected. . .")
		time.Sleep(10 * time.Second)
		_, err = r.GetLeader()
	}

	if _, isLeader, _ := r.GetState(); isLeader {
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

		log.Println("[CLIENT] Calling ApplyEntry again. . .")

		c.Value = "-1"
		b, err = json.Marshal(c)
		if err != nil {
			panic(err)
		}
		r.ApplyEntry(b)

		// time.Sleep(30 * time.Second)
		// log.Println("[CLIENT] Killing leader...")
		// r.Kill()

		// log.Println("[CLIENT] Killed.")

		// time.Sleep(1 * time.Second)
		// log.Println("[CLIENT] Starting server up again")

		// r, err = raft.NewServer(me, peerAddrs, kvs, config)
		// if err != nil {
		// 	log.Fatal(err)
		// }
	}

	for {
	}
}
