package coordinator

import "context"

type Coordinator struct {
	ctx     context.Context
	cancelf context.CancelFunc
}

func NewCoordinator() *Coordinator {
	ctx, cancelf := context.WithCancel(context.Background())
	return &Coordinator{
		ctx:     ctx,
		cancelf: cancelf,
	}
}

func (c *Coordinator) Done() bool {
	return false
}
