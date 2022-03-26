package coordinator

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"

	"github.com/sirupsen/logrus"
	"github.com/sunyakun/toymapreduce-go/pkg/log"
	rpctypes "github.com/sunyakun/toymapreduce-go/pkg/rpc"
)

type RPCServer struct {
	address     string
	port        uint16
	coordinator *Coordinator
	logger      *logrus.Logger
}

func NewRPCServer(coordinator *Coordinator, addr string, port uint16) *RPCServer {
	return &RPCServer{
		address:     addr,
		port:        port,
		coordinator: coordinator,
		logger:      log.GetLogger(),
	}
}

func (server *RPCServer) Start() error {
	rpc.Register(server)
	rpc.HandleHTTP()
	addr := fmt.Sprintf("%s:%d", server.address, server.port)
	listen, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go func() {
		server.logger.WithField("address", addr).Info("start rpc server")
		if err := http.Serve(listen, nil); err != nil {
			panic(err)
		}
	}()

	<-server.coordinator.ctx.Done()
	return nil
}

func (server *RPCServer) HeartBeat(req *rpctypes.HeartBeatRequest, resp *rpctypes.HeartBeatResponse) error {
	return nil
}
