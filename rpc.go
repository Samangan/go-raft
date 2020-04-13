package raft

import (
	"fmt"
	"net"
	"net/rpc"
)

const (
	RPCPort = 8000 // TODO: Make configurable
)

func initRPC(address string, rs *RaftServer) error {
	// Register RPC endpoints:
	re := &RPCEndpointData{rs}
	err := rpc.Register(re)
	if err != nil {
		return err
	}

	// Start accepting incoming RPC endpoints from peers:
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%v", address, RPCPort))
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
