package coord

import (
	"path/filepath"
	"strings"

	"github.com/sunyakun/toymapreduce-go/internal/coordinator"
	"github.com/sunyakun/toymapreduce-go/pkg/log"
)

type SliceVar struct {
	data []string
}

func (s *SliceVar) Set(val string) error {
	val = strings.Trim(val, ",")
	s.data = strings.Split(val, ",")
	return nil
}

func (s *SliceVar) String() string {
	return strings.Join(s.data, ",")
}

type CoordinatorArguments struct {
	Inputfiles SliceVar
	NReduce    uint64
	Address    string
	Port       uint64
}

func CoordinatorCommand(args CoordinatorArguments) {
	logger := log.GetLogger()

	var err error
	for ix := range args.Inputfiles.data {
		args.Inputfiles.data[ix], err = filepath.Abs(args.Inputfiles.data[ix])
		if err != nil {
			panic(err)
		}
		args.Inputfiles.data[ix] = "file://" + args.Inputfiles.data[ix]
	}

	coor := coordinator.NewCoordinator(args.Inputfiles.data, uint32(args.NReduce), logger)
	if err := coordinator.NewRPCServer(coor, args.Address, uint16(args.Port), logger).Start(); err != nil {
		panic(err)
	}
}
