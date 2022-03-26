package coord

import (
	"strings"

	"github.com/sunyakun/toymapreduce-go/internal/coordinator"
)

type SliceVar struct {
	data []string
}

func (s *SliceVar) Set(val string) error {
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
	coor := coordinator.NewCoordinator()
	if err := coordinator.NewRPCServer(coor, args.Address, uint16(args.Port)).Start(); err != nil {
		panic(err)
	}
}
