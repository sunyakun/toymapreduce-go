package coordinator

import (
	"context"
	"errors"
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
	WorkerUUID  string
	InputFiles  []string
	OutputFiles []rpctypes.OutputFileSolt
}

type Worker struct {
	UUID          string
	phase         string
	lastHeartBeat uint32
	taskUUID      string
	finish        bool
	task          *Task

	mutex sync.RWMutex
}

func (w *Worker) SetPhase(phase string) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.phase = phase
}

func (w *Worker) GetPhase() string {
	w.mutex.RLock()
	defer w.mutex.RUnlock()
	return w.phase
}

func (w *Worker) SetTask(t Task) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.task = &t
}

func (w *Worker) GetTask() (Task, bool) {
	w.mutex.RLock()
	defer w.mutex.RUnlock()
	if w.task == nil {
		return Task{}, false
	}
	return *w.task, true
}

func (w *Worker) WithWorkerLock(cb func() error) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	return cb()
}

type DoneTaskList struct {
	tasks []Task

	mutex sync.RWMutex
}

func (d *DoneTaskList) Append(val Task) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.tasks = append(d.tasks, val)
}

func (d *DoneTaskList) Len() int {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	return len(d.tasks)
}

func (d *DoneTaskList) Range(cb func(task Task) bool) {
	length := d.Len()
	for i := 0; i < length; i++ {
		if !cb(d.tasks[i]) {
			break
		}
	}
}

type Coordinator struct {
	ctx            context.Context
	cancelf        context.CancelFunc
	tasks          sync.Map
	workers        sync.Map
	maptasks       chan Task
	reducetasks    chan Task
	donetasks      DoneTaskList
	nreduce        uint32
	clock          uint32
	maxFailedClock uint32
	maxRemoveClock uint32
	maptaskNum     uint32
	dfs            string

	logger *logrus.Logger
}

func NewCoordinator(inputFiles []string, nreduce uint32, dfs string, logger *logrus.Logger) *Coordinator {
	ctx, cancelf := context.WithCancel(context.Background())
	c := &Coordinator{
		ctx:         ctx,
		cancelf:     cancelf,
		maptasks:    make(chan Task, len(inputFiles)),
		reducetasks: make(chan Task, nreduce),
		nreduce:     nreduce,
		logger:      logger,
		dfs:         dfs,
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
	c.maptaskNum = uint32(len(inputFiles))

	logger.Info("*** Init coordinator ***")
	logger.Info("*** NReduce: ", nreduce)
	logger.Info("*** Input: ", inputFiles)
	return c
}

func (c *Coordinator) GetWorkerConfig() rpctypes.FetchConfigResponse {
	return rpctypes.FetchConfigResponse{
		NReduce: c.nreduce,
		DFS:     c.dfs,
	}
}

func (c *Coordinator) ScanWorkers() {
	for {
		c.workers.Range(func(key, val any) bool {
			v := val.(*Worker)
			heartGap := atomic.LoadUint32(&c.clock) - atomic.LoadUint32(&v.lastHeartBeat)

			if heartGap < c.maxFailedClock {
				atomic.StoreUint32(&v.lastHeartBeat, atomic.LoadUint32(&c.clock))
				return true
			} else if heartGap < c.maxRemoveClock {
				c.logger.WithField("worker_uuid", v.UUID).Info("worker failed!")
				v.SetPhase(rpctypes.WorkerPhaseFailed)
				return true
			} else {
				c.logger.WithField("worker_uuid", v.UUID).Info("worker removed!")
				v.WithWorkerLock(func() error {
					if v.task != nil {
						if v.task.Type == rpctypes.TaskTypeMap {
							c.maptasks <- *v.task
						} else {
							c.reducetasks <- *v.task
						}
					}
					c.workers.Delete(key)
					return nil
				})
			}
			return true
		})
		select {
		case <-time.After(time.Second):
		case <-c.ctx.Done():
			return
		}
	}
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
	if worker, ok := c.workers.Load(workerUUID); ok {
		var t Task
		w := worker.(*Worker)
		err := w.WithWorkerLock(func() error {
			if w.phase == rpctypes.WorkerPhaseFailed {
				c.logger.WithField("worker_uuid", workerUUID).Warn("receive apply task from a failed worker, ignore it")
				return errors.New("worker is at failed phase")
			}

			select {
			case t = <-c.maptasks:
			case t = <-c.reducetasks:
			case <-time.After(time.Second):
				c.logger.WithField("worker_uuid", workerUUID).Info("there have no task to schedule!")
				return errors.New("no task to schedule")
			}
			w.task = &t
			return nil
		})
		if err != nil {
			return nil
		}
		return &rpctypes.Task{
			UUID:       t.UUID,
			Type:       t.Type,
			InputFiles: t.InputFiles,
		}
	} else {
		c.logger.WithField("worker_uuid", workerUUID).Warn("receive apply task from not exists worker")
	}
	return nil
}

func (c *Coordinator) TaskDone(workerUUID, taskUUID string, outputFiles []rpctypes.OutputFileSolt) {
	if worker, ok := c.workers.Load(workerUUID); ok {
		w := worker.(*Worker)
		w.WithWorkerLock(func() error {
			if w.task.UUID == taskUUID {
				c.logger.WithFields(logrus.Fields{
					"worker_uuid": workerUUID,
					"task_uuid":   taskUUID,
				}).Info("task done!")
				doneTask := *w.task
				doneTask.OutputFiles = outputFiles
				c.donetasks.Append(doneTask)
				w.task = nil
			} else {
				c.logger.WithFields(logrus.Fields{
					"worker_uuid": workerUUID,
					"task_uuid":   taskUUID,
				}).Warn("task done conflict")
			}
			return nil
		})

		// all map task done, generate reduce task.
		if c.donetasks.Len() == int(c.maptaskNum) {
			reduceTasks := make([]Task, c.nreduce)
			c.donetasks.Range(func(task Task) bool {
				for _, file := range task.OutputFiles {
					if uint32(file.ReduceIndex) < c.nreduce {
						reduceTasks[file.ReduceIndex].InputFiles = append(reduceTasks[file.ReduceIndex].InputFiles, file.FilePath)
					}
				}
				return true
			})
			for _, task := range reduceTasks {
				task.UUID = uuid.NewString()
				task.Type = rpctypes.TaskTypeReduce
				c.reducetasks <- task
			}
		} else if c.donetasks.Len() == int(c.maptaskNum+c.nreduce) {
			c.cancelf()
		}
	} else {
		c.logger.WithField("worker_uuid", workerUUID).Warn("receive task done from not exists worker")
	}
}

func (c *Coordinator) TaskFail(workerUUID, taskUUID string) {
	if worker, ok := c.workers.Load(workerUUID); ok {
		w := worker.(*Worker)
		w.WithWorkerLock(func() error {
			if w.task.UUID == taskUUID {
				c.logger.WithFields(logrus.Fields{
					"worker_uuid": workerUUID,
					"task_uuid":   taskUUID,
				}).Info("task fail!")
				if w.task.Type == rpctypes.TaskTypeMap {
					c.maptasks <- *w.task
				} else {
					c.reducetasks <- *w.task
				}
				w.task = nil
			} else {
				c.logger.WithFields(logrus.Fields{
					"worker_uuid": workerUUID,
					"task_uuid":   taskUUID,
				}).Warn("task fail conflict")
			}
			return nil
		})
	} else {
		c.logger.WithField("worker_uuid", workerUUID).Warn("receive task fail from not exists worker")
	}
}

func (c *Coordinator) Done() bool {
	if c.donetasks.Len() == int(c.maptaskNum+c.nreduce) {
		return true
	}
	return false
}
