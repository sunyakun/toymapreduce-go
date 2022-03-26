package config

type CoordinatorConfig struct {
	MaxFailedClockGap uint32
	MaxRemoveClockGap uint32
	NReduce           uint32
}

type WorkerConfig struct {
}
