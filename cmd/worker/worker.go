package worker

import (
	"github.com/sunyakun/toymapreduce-go/internal/worker"
)

type WorkerArguments struct {
	PluginPath string
	Address    string
	Port       uint64
}

func WorkerCommand(args WorkerArguments) {
	worker.NewWorker().Start()
}
