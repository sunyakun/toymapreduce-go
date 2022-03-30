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
	err := worker.NewWorker(args.Address, uint32(args.Port), args.PluginPath).Start()
	if err != nil {
		panic(err)
	}
}
