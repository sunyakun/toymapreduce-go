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

type Task struct {
	UUID       string
	Type       string
	InputFiles []string
}

type HeartBeatRequest struct {
	UUID string
}

type HeartBeatResponse struct{}

type ApplyTaskRequest struct {
	WorkerUUID string
}

type ApplyTaskResponse struct {
	Task *Task
}

type TaskDoneRequest struct {
	WorkerUUID      string
	TaskUUID        string
	OutputFilePaths []string
}

type TaskDoneResponse struct{}

type TaskFailRequest struct {
	WorkerUUID string
	TaskUUID   string
}

type TaskFailResponse struct{}
