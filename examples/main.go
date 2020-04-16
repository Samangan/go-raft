package main

// USAGE (from pkg root):
// * docker build -f examples/DemoDockerFile -t raft .
// * docker-compose -f examples/docker-compose.yml  up
// * ...

import (
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
	log.Printf("[Apply()] entry: %v", entry)
	return -1
}

func main() {
	// NOTE: defined in docker-compose.yml
	peerAddrs := []string{"node0", "node1", "node2", "node3"}
	me, _ := strconv.Atoi(os.Getenv("ID"))

	log.Printf("[CLIENT] Starting up server [%v: %v]", me, peerAddrs[me])

	applyMsgCh := make(chan raft.ApplyMessage)
	kvs := KVStore{make(map[string]string)}

	r, err := raft.NewServer(me, peerAddrs, applyMsgCh, kvs)
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
		r.ApplyEntry(42)
		//log.Println("[CLIENT] Calling ApplyEntry. . .")
		//r.ApplyEntry(43)
	}

	res := <-applyMsgCh
	log.Printf("Recieved: %v", res.Entry.Command)

	for {
	}
}
