package worker

import (
	"context"
	"errors"
	"fmt"
	"net/rpc"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sunyakun/toymapreduce-go/pkg/log"
	rpctypes "github.com/sunyakun/toymapreduce-go/pkg/rpc"
)

type Worker struct {
	uuid            string
	task            *rpctypes.Task
	coordinatorAddr string
	rpcClient       *rpc.Client
	logger          *logrus.Logger
	ctx             context.Context
	cancelf         context.CancelFunc
}

func NewWorker(address string, port uint32) *Worker {
	ctx, cancelf := context.WithCancel(context.Background())
	return &Worker{
		uuid:            uuid.NewString(),
		logger:          log.GetLogger(),
		ctx:             ctx,
		cancelf:         cancelf,
		coordinatorAddr: fmt.Sprintf("%s:%d", address, port),
	}
}

func (w *Worker) Start() (err error) {
	if w.rpcClient, err = rpc.DialHTTP("tcp", w.coordinatorAddr); err != nil {
		w.logger.WithError(err).Error("connect rpc server error")
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for {
			select {
			case <-time.Tick(3 * time.Second):
				if err := w.rpcClient.Call("RPCServer.HeartBeat", &rpctypes.HeartBeatRequest{UUID: w.uuid}, &rpctypes.HeartBeatResponse{}); err != nil {
					if errors.Is(err, rpc.ErrShutdown) {
						w.logger.Info("remote server shutdown")
						wg.Done()
						return
					}
					w.logger.WithError(err).Error("heartbeat error")
				}
			case <-w.ctx.Done():
				w.logger.Info("stop heartbeat goroutine")
				wg.Done()
				return
			}
		}
	}()

	// FIXME we should let heartbeat execute immediately
	time.Sleep(0)

	for {

		// Step 1: if all tasks have been done, exit
		doneResp := &rpctypes.DoneResponse{}
		if err = w.rpcClient.Call("RPCServer.Done", &rpctypes.DoneRequest{}, doneResp); err != nil {
			w.logger.WithError(err)
			goto EXIT
		}
		if doneResp.Done {
			w.logger.Info("all task have done")
			goto EXIT
		}

		// Step 2: apply task
		applyTaskResp := &rpctypes.ApplyTaskResponse{}
		if err = w.rpcClient.Call("RPCServer.ApplyTask", &rpctypes.ApplyTaskRequest{WorkerUUID: w.uuid}, applyTaskResp); err != nil {
			w.logger.WithError(err).Error("call ApplyTask")
			goto EXIT
		}
		if applyTaskResp.Task == nil {
			// if there have no task to do, sleep and apply again
			time.Sleep(3 * time.Second)
			continue
		}

		// Step3: process task
		w.task = applyTaskResp.Task
		outputFiles, err := w.doTask(*w.task)
		if err != nil {
			// tell coordinator the task fail
			if err = w.rpcClient.Call(
				"RPCServer.TaskFail",
				&rpctypes.TaskFailRequest{WorkerUUID: w.uuid, TaskUUID: w.task.UUID},
				&rpctypes.TaskFailResponse{}); err != nil {

				w.logger.WithError(err).Error("call TaskFail")
				goto EXIT
			}
		}
		// tell coordinator the task success
		if err = w.rpcClient.Call(
			"RPCServer.TaskDone",
			&rpctypes.TaskDoneRequest{WorkerUUID: w.uuid, TaskUUID: w.task.UUID, OutputFilePaths: outputFiles},
			&rpctypes.TaskDoneResponse{}); err != nil {

			w.logger.WithError(err).Error("call TaskDone")
			goto EXIT
		}

	}

EXIT:
	w.logger.Info("going to shutdown")
	w.cancelf()
	wg.Wait()
	return nil
}

func (w *Worker) doTask(task rpctypes.Task) ([]rpctypes.OutputFileSolt, error) {
	return nil, nil
}
