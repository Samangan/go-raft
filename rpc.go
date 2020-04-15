package raft

import (
	"fmt"
	"net"
	"net/rpc"
)

const (
	RPCPort = 8000 // TODO: Make configurable
)

type RPCHandler struct {
	rs *RaftServer
}

func initRPC(address string, rs *RaftServer) error {
	// Register RPC endpoints:
	re := &RPCHandler{rs}
	err := rpc.Register(re)
	if err != nil {
		return err
	}

	// Start accepting incoming RPC endpoints from peers:
	addr, err := net.ResolveTCPAddr("tcp", makeAddr(address))
	if err != nil {
		return err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}

	go rpc.Accept(l)

	return nil
}

func sendRPC(address, endpoint string, req interface{}, res interface{}) error {
	client, err := rpc.Dial("tcp", makeAddr(address))
	if err != nil {
		return err
	}

	return client.Call(endpoint, req, res)
}

func makeAddr(peerAddr string) string {
	return fmt.Sprintf("%s:%v", peerAddr, RPCPort)
}
