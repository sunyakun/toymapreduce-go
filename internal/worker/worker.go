package worker

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/rpc"
	"plugin"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sunyakun/toymapreduce-go/pkg/fsutil"
	"github.com/sunyakun/toymapreduce-go/pkg/log"
	"github.com/sunyakun/toymapreduce-go/pkg/mr"
	rpctypes "github.com/sunyakun/toymapreduce-go/pkg/rpc"
)

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func loadPlugin(p string) (mpf mr.MapFunc, redf mr.ReduceFunc, err error) {
	var symbol plugin.Symbol

	plug, err := plugin.Open(p)
	if err != nil {
		return nil, nil, err
	}

	MapFuncType := reflect.TypeOf((*mr.MapFunc)(nil)).Elem()
	ReduceFuncType := reflect.TypeOf((*mr.ReduceFunc)(nil)).Elem()

	if symbol, err = plug.Lookup("Map"); err != nil {
		return nil, nil, err
	}
	if !reflect.ValueOf(symbol).CanConvert(MapFuncType) {
		return nil, nil, errors.New("'Map' func is not a type of mr.MapFunc")
	}
	mpf = reflect.ValueOf(symbol).Convert(MapFuncType).Interface().(mr.MapFunc)

	if symbol, err = plug.Lookup("Reduce"); err != nil {
		return nil, nil, err
	}
	if !reflect.ValueOf(symbol).CanConvert(ReduceFuncType) {
		return nil, nil, errors.New("'Reduce' func is not a type of mr.ReduceFunc")
	}
	redf = reflect.ValueOf(symbol).Convert(ReduceFuncType).Interface().(mr.ReduceFunc)

	return mpf, redf, nil
}

type Worker struct {
	uuid            string
	task            *rpctypes.Task
	coordinatorAddr string
	rpcClient       *rpc.Client
	logger          *logrus.Logger
	ctx             context.Context
	cancelf         context.CancelFunc
	mrplugin        string

	mapf    mr.MapFunc
	reducef mr.ReduceFunc

	workerConfig rpctypes.FetchConfigResponse
}

func NewWorker(address string, port uint32, mrplugin string) *Worker {
	ctx, cancelf := context.WithCancel(context.Background())
	return &Worker{
		uuid:            uuid.NewString(),
		logger:          log.GetLogger(),
		ctx:             ctx,
		cancelf:         cancelf,
		coordinatorAddr: fmt.Sprintf("%s:%d", address, port),
		mrplugin:        mrplugin,
	}
}

func (w *Worker) Start() (err error) {
	w.mapf, w.reducef, err = loadPlugin(w.mrplugin)
	if err != nil {
		w.logger.WithError(err).Error("load plugin fail")
		return err
	}

	if w.rpcClient, err = rpc.DialHTTP("tcp", w.coordinatorAddr); err != nil {
		w.logger.WithError(err).Error("connect rpc server error")
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := w.rpcClient.Call("RPCServer.HeartBeat", &rpctypes.HeartBeatRequest{UUID: w.uuid}, &rpctypes.HeartBeatResponse{}); err != nil {
					if errors.Is(err, rpc.ErrShutdown) {
						w.logger.Info("remote server shutdown")
						return
					}
					w.logger.WithError(err).Error("heartbeat error")
				}
			case <-w.ctx.Done():
				w.logger.Info("stop heartbeat goroutine")
				return
			}
		}
	}()

	// FIXME we should let heartbeat execute immediately
	time.Sleep(0)

	if err := w.rpcClient.Call("RPCServer.FetchConfig", &rpctypes.FetchConfigRequest{}, &w.workerConfig); err != nil {
		w.logger.WithError(err).Info("fetch config from coordinator fail")
		goto EXIT
	}

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
			w.logger.WithError(err).WithFields(logrus.Fields{
				"task_uuid":  w.task.UUID,
				"task_type":  w.task.Type,
				"task_input": w.task.InputFiles,
			}).Error("do task fail")
			// tell coordinator the task fail
			if err = w.rpcClient.Call(
				"RPCServer.TaskFail",
				&rpctypes.TaskFailRequest{WorkerUUID: w.uuid, TaskUUID: w.task.UUID},
				&rpctypes.TaskFailResponse{}); err != nil {

				w.logger.WithError(err).Error("call TaskFail")
			}
			goto EXIT
		} else {
			// tell coordinator the task success
			if err = w.rpcClient.Call(
				"RPCServer.TaskDone",
				&rpctypes.TaskDoneRequest{WorkerUUID: w.uuid, TaskUUID: w.task.UUID, OutputFilePaths: outputFiles},
				&rpctypes.TaskDoneResponse{}); err != nil {

				w.logger.WithError(err).Error("call TaskDone")
				goto EXIT
			}
		}

	}

EXIT:
	w.logger.Info("going to shutdown")
	w.cancelf()
	wg.Wait()
	return nil
}

func (w *Worker) domap(input string) ([]rpctypes.OutputFileSolt, error) {
	content, err := fsutil.ReadAll(input)
	if err != nil {
		w.logger.WithField("input", input).WithError(err).Info("open input file fail")
	}

	kvs, err := w.mapf(input, string(content))
	if err != nil {
		return nil, err
	}

	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})

	fsclient, err := fsutil.NewFsClient(w.workerConfig.DFS)
	if err != nil {
		return nil, err
	}

	fscache := make(map[string]io.WriteCloser)
	output := make([]rpctypes.OutputFileSolt, w.workerConfig.NReduce)

	for _, kv := range kvs {
		reduceix := ihash(kv.Key) % int(w.workerConfig.NReduce)
		fname := fmt.Sprintf("%s/mrout-worker-%s-nreduce-%d", w.workerConfig.DFS, w.uuid, reduceix)

		if _, ok := fscache[fname]; !ok {
			var err error
			fscache[fname], err = fsclient.Create(fname)
			if err != nil {
				return nil, err
			}
			output[reduceix] = rpctypes.OutputFileSolt{
				ReduceIndex: reduceix,
				FilePath:    strings.TrimRight(w.workerConfig.DFS, "/") + fname,
			}
		}
		_, err := fscache[fname].Write([]byte(fmt.Sprintf("%s %s\n", kv.Key, kv.Value)))
		if err != nil {
			return nil, err
		}
	}

	for fname, writer := range fscache {
		if err := writer.Close(); err != nil {
			w.logger.WithField("filename", fname).Error("close file writer fail")
		}
	}
	if err := fsclient.Close(); err != nil {
		w.logger.WithField("dir", w.workerConfig.DFS).Error("close fsclient fail")
	}
	return output, nil
}

func (w *Worker) doreduce(input, output string) error {
	client, err := fsutil.NewFsClient(input)
	if err != nil {
		w.logger.WithError(err).WithField("input", input).Error("create fs client fail")
		return err
	}
	reader, err := client.Open(input)
	if err != nil {
		w.logger.WithError(err).WithField("input", input).Error("open fs fail")
		return err
	}
	defer reader.Close()

	writer, err := client.Append(output)
	if err != nil {
		w.logger.WithError(err).WithField("output", output).Error("create output fail")
		return err
	}
	defer writer.Close()

	iter := mr.NewKVIterator(reader)
	for iter.NextKey() {
		resultIter, err := w.reducef(iter)
		if err != nil {
			w.logger.WithError(err).WithField("input", input).Error("reduce fail")
			return err
		}

		for v, err := resultIter.Next(); err != mr.ErrStopIter; v, err = resultIter.Next() {
			if err != nil {
				w.logger.WithError(err).Error("next result fail")
				return err
			}
			_, err = writer.Write([]byte(v.(string) + "\n"))
			if err != nil {
				w.logger.WithError(err).WithField("output", output).Error("write result fail")
				return err
			}
		}
	}

	return nil
}

func (w *Worker) doTask(task rpctypes.Task) ([]rpctypes.OutputFileSolt, error) {
	if task.Type == rpctypes.TaskTypeMap {
		return w.domap(task.InputFiles[0])
	} else if task.Type == rpctypes.TaskTypeReduce {
		output := fmt.Sprintf("%s/mrout-worker-%s-reduce", w.workerConfig.DFS, w.uuid)
		client, err := fsutil.NewFsClient(output)
		if err != nil {
			w.logger.WithError(err).WithField("output", output).Error("create fs client fail")
			return nil, err
		}

		writer, err := client.Create(output)
		if err != nil {
			w.logger.WithError(err).WithField("output", output).Error("create output file fail")
			return nil, err
		}
		writer.Close()

		for _, input := range task.InputFiles {
			if err := w.doreduce(input, output); err != nil {
				return nil, err
			}
		}
		return []rpctypes.OutputFileSolt{{FilePath: output}}, nil
	}
	return nil, nil
}
