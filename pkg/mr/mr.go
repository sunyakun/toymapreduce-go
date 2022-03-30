package mr

type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type MapFunc func(filename string, content string) ([]KeyValue, error)

type ReduceFunc func(key string, values []string) (string, error)
