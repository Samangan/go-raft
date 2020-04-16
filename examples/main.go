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

func main() {
	// NOTE: defined in docker-compose.yml
	peerAddrs := []string{"node0", "node1", "node2", "node3"}
	me, _ := strconv.Atoi(os.Getenv("ID"))

	log.Printf("[CLIENT] Starting up server [%v: %v]", me, peerAddrs[me])

	applyMsgCh := make(chan raft.ApplyMessage)
	r, err := raft.NewServer(me, peerAddrs, applyMsgCh)
	if err != nil {
		log.Fatal(err)
	}

	// TODO: Testing this is going to be hard until I have an actual client usage of this.
	// I should make:
	// * Unit tests
	// * "Integration" test suite that will test through common things with multiple nodes

	// NOTE: For now doing some more manual testing on each node:
	_, err = r.GetLeader()
	for err != nil {
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

	for {
	}
}
