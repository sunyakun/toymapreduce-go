package worker

type Worker struct{}

func NewWorker() *Worker {
	return &Worker{}
}

func (w *Worker) Start() error {
	return nil
}
