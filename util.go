package workout

import (
	"errors"
	"fmt"
	log "github.com/kdar/factorlog"
	"strconv"
)

//var logger *log.FactorLog = log.New(os.Stdout, factorlog.NewStdFormatter(`%{Color "red:white" "CRITICAL"}%{Color "red" "ERROR"}%{Color "yellow" "WARN"}%{Color "green" "INFO"}%{Color "cyan" "DEBUG"}%{Color "blue" "TRACE"}[%{Date} %{Time}] [%{SEVERITY}:%{ShortFile}:%{Line}] %{Message}%{Color "reset"}`))
var logger *log.FactorLog

func SetLogger(alogger *log.FactorLog) {
	logger = alogger
}

func Error(f string, v ...interface{}) error {
	return errors.New(fmt.Sprintf(f, v...))
}

func parseInt(val string) (n int64) {
	n, _ = strconv.ParseInt(val, 10, 64)
	return
}
