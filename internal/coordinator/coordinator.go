package coordinator

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	rpctypes "github.com/sunyakun/toymapreduce-go/pkg/rpc"
)

type Task struct {
	UUID        string
	Type        string
	InputFiles  []string
	OutputFiles []string
}

type Worker struct {
	UUID          string
	phase         string
	lastHeartBeat uint32
	taskUUID      string
	finish        bool

	mutex sync.RWMutex
}

type Coordinator struct {
	ctx           context.Context
	cancelf       context.CancelFunc
	tasks         sync.Map
	workers       sync.Map
	maptasks      chan Task
	reducetasks   chan Task
	doneTaskUUIDs []string
	nreduce       uint32
	clock         uint32

	logger logrus.Logger
}

func NewCoordinator(inputFiles []string, nreduce uint32) *Coordinator {
	ctx, cancelf := context.WithCancel(context.Background())
	c := &Coordinator{
		ctx:         ctx,
		cancelf:     cancelf,
		maptasks:    make(chan Task),
		reducetasks: make(chan Task),
		nreduce:     nreduce,
	}
	for _, in := range inputFiles {
		taskuuid := uuid.NewString()
		t := &Task{
			UUID:       taskuuid,
			Type:       rpctypes.TaskTypeMap,
			InputFiles: []string{in},
		}
		c.tasks.Store(taskuuid, t)
		c.maptasks <- *t
	}
	return c
}

func (c *Coordinator) HeartBeat(workerUUID string) {
	if worker, ok := c.workers.Load(workerUUID); ok {
		w := worker.(*Worker)
		atomic.StoreUint32(&w.lastHeartBeat, atomic.LoadUint32(&c.clock))
	} else {
		c.workers.LoadOrStore(workerUUID, &Worker{
			UUID:          workerUUID,
			phase:         rpctypes.WorkerPhaseIdle,
			lastHeartBeat: atomic.LoadUint32(&c.clock),
			finish:        false,
		})
	}
}

func (c *Coordinator) ApplyTask(workerUUID string) *rpctypes.Task {
	var t Task
	select {
	case t = <-c.maptasks:
	case t = <-c.reducetasks:
	case <-time.After(time.Second):
		c.logger.WithField("worker_uuid", workerUUID).Info("there have no task to schedule!")
		return nil
	}
	return &rpctypes.Task{
		UUID:       t.UUID,
		Type:       t.Type,
		InputFiles: t.InputFiles,
	}
}

func (c *Coordinator) TaskDone(workerUUID, taskUUID string, outputFiles []string) {

}

func (c *Coordinator) TaskFail(workerUUID, taskUUID string) {

}
