package rpc

const (
	TaskTypeMap    = "MAP"
	TaskTypeReduce = "REDUCE"

	TaskStatusPendding = "PENDDING"
	TaskStatusRunning  = "RUNNING"
	TaskStatusFailed   = "FAILED"
	TaskStatusSuccess  = "SUCCESS"

	WorkerPhaseIdle    = "IDLE"
	WorkerPhaseRunning = "RUNNING"
)

type HeartBeatRequest struct{}

type HeartBeatResponse struct{}

type ApplyTaskRequest struct{}

type ApplyTaskResponse struct{}

type TaskDoneRequest struct{}

type TaskDoneResponse struct{}

type TaskFailRequest struct{}

type TaskFailResponse struct{}
