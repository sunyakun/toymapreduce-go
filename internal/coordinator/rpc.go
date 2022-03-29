package coordinator

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"

	"github.com/sirupsen/logrus"
	rpctypes "github.com/sunyakun/toymapreduce-go/pkg/rpc"
)

type RPCServer struct {
	address     string
	port        uint16
	coordinator *Coordinator
	logger      *logrus.Logger
}

func NewRPCServer(coordinator *Coordinator, addr string, port uint16, logger *logrus.Logger) *RPCServer {
	return &RPCServer{
		address:     addr,
		port:        port,
		coordinator: coordinator,
		logger:      logger,
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
	server.coordinator.HeartBeat(req.UUID)
	return nil
}

func (server *RPCServer) ApplyTask(req *rpctypes.ApplyTaskRequest, resp *rpctypes.ApplyTaskResponse) error {
	// FIX how to process when coordinator.ApplyTask success but the response sent to client fail
	resp.Task = server.coordinator.ApplyTask(req.WorkerUUID)
	return nil
}

func (server *RPCServer) TaskDone(req *rpctypes.TaskDoneRequest, resp *rpctypes.TaskDoneResponse) error {
	server.coordinator.TaskDone(req.WorkerUUID, req.TaskUUID, req.OutputFilePaths)
	return nil
}

func (server *RPCServer) TaskFail(req *rpctypes.TaskFailRequest, resp *rpctypes.TaskFailResponse) error {
	server.coordinator.TaskFail(req.WorkerUUID, req.TaskUUID)
	return nil
}

func (server *RPCServer) Done(req *rpctypes.DoneRequest, resp *rpctypes.DoneResponse) error {
	resp.Done = server.coordinator.Done()
	return nil
}
