package main

// USAGE (from pkg root):
// * docker build -f examples/DemoDockerFile -t raft .
// * docker-compose -f examples/docker-compose.yml  up
// * ...

import (
	"github.com/Samangan/go-raft"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

func main() {
	// NOTE: defined in docker-compose.yml
	peerAddrs := []string{"node0", "node1", "node2"}
	me, _ := strconv.Atoi(os.Getenv("ID"))

	log.Printf("Starting up server [%v: %v]", me, peerAddrs[me])

	applyMsgCh := make(chan raft.ApplyMessage)
	_, err := raft.NewServer(me, peerAddrs, applyMsgCh)
	if err != nil {
		log.Fatal(err)
	}

	// NOTE: Just testing random shit here:
	time.Sleep(2 * time.Second)

	client, err := rpc.Dial("tcp", peerAddrs[1]+":8000")
	if err != nil {
		log.Fatal(err)
	}

	req := &raft.AppendEntryReq{}
	res := &raft.AppendEntryRes{}
	err = client.Call("RPCEndpointData.AppendEntries", req, res)
	if err != nil {
		log.Fatal(err)
	}

	for {
	}
}
