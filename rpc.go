package raft

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
)

var RPCPort int

type RPCHandler struct {
	rs *RaftServer
}

func initRPC(address string, rs *RaftServer) error {
	RPCPort = rs.config.RpcPort

	// Register RPC endpoints:
	re := &RPCHandler{rs}
	server := rpc.NewServer()
	err := server.Register(re)
	if err != nil {
		return err
	}

	// Start accepting incoming RPC calls from peers:
	addr, err := net.ResolveTCPAddr("tcp", makeAddr(address))
	if err != nil {
		return err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}

	go acceptRPCs(rs, l, server)

	return nil
}

func acceptRPCs(rs *RaftServer, lis net.Listener, server *rpc.Server) {
	for {
		select {
		case <-rs.killCh:
			log.Println("Shutting off RPC goroutine. . .")
			lis.Close()
			return
		default:
			conn, err := lis.Accept()
			if err != nil {
				log.Print("rpc.Serve: accept:", err.Error())
				lis.Close()
				return
			}

			go server.ServeConn(conn)
		}
	}
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
