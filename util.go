package workout

import (
	"errors"
	"fmt"
	"github.com/bububa/verboselogger"
	"strconv"
)

var logger *log4go.VerboseLogger

func SetLogger(alogger *log4go.VerboseLogger) {
	logger = alogger
}

func Error(f string, v ...interface{}) error {
	return errors.New(fmt.Sprintf(f, v...))
}

func parseInt(val string) (n int64) {
	n, _ = strconv.ParseInt(val, 10, 64)
	return
}
