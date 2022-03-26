package log

import (
	"sync"

	"github.com/sirupsen/logrus"
)

type Logger *logrus.Logger

var (
	logger  *logrus.Logger
	loglock = sync.Mutex{}
)

func GetLogger() *logrus.Logger {
	loglock.Lock()
	defer loglock.Unlock()
	if logger != nil {
		return logger
	}
	return logrus.New()
}
