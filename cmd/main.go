package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/sunyakun/toymapreduce-go/cmd/coord"
	"github.com/sunyakun/toymapreduce-go/cmd/worker"
)

func Usage() {
	fmt.Printf("Usage: %s worker|coordinator\n", os.Args[0])
}

func main() {
	coordArgs := coord.CoordinatorArguments{}
	workerArgs := worker.WorkerArguments{}

	coordFlags := flag.NewFlagSet("coodinator", flag.ExitOnError)
	coordFlags.Var(&coordArgs.Inputfiles, "input", "comma-separated string instead of input files")
	coordFlags.Uint64Var(&coordArgs.NReduce, "nreduce", 4, "concurrent number of reduce workers")
	coordFlags.StringVar(&coordArgs.Address, "address", "localhost", "rpc server bind address")
	coordFlags.Uint64Var(&coordArgs.Port, "port", 6789, "rpc server listening port")

	workerFlags := flag.NewFlagSet("worker", flag.ExitOnError)
	workerFlags.StringVar(&workerArgs.PluginPath, "mrpath", "", "mapreduce function plugin path")
	workerFlags.StringVar(&workerArgs.Address, "address", "localhost", "coordinator address")
	workerFlags.Uint64Var(&workerArgs.Port, "port", 6789, "coordinator port")

	if len(os.Args) < 2 {
		Usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "coord", "coordinator":
		coordFlags.Parse(os.Args[2:])
		if coordArgs.Inputfiles.String() == "" {
			coordFlags.Usage()
			os.Exit(1)
		}
		coord.CoordinatorCommand(coordArgs)
	case "worker":
		workerFlags.Parse(os.Args[1:])
		worker.WorkerCommand(workerArgs)
	default:
		Usage()
		os.Exit(1)
	}

}
