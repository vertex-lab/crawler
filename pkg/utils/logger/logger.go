// The package logger defines a simple logger with INFO, WARN and ERROR prints.
package logger

import (
	"io"
	"log"
	"os"
)

type Aggregate struct {
	InfoLogger  *log.Logger
	WarnLogger  *log.Logger
	ErrorLogger *log.Logger
}

// New() returns an initialized Logger
func New(out io.Writer) *Aggregate {
	infoLogger := log.New(out, "INFO: ", log.LstdFlags)
	warnLogger := log.New(out, "WARN: ", log.LstdFlags)
	errorLogger := log.New(out, "ERROR: ", log.LstdFlags)

	return &Aggregate{
		InfoLogger:  infoLogger,
		WarnLogger:  warnLogger,
		ErrorLogger: errorLogger,
	}
}

// Info() prints an INFO log
func (l *Aggregate) Info(s string, v ...interface{}) {
	l.InfoLogger.Printf(s, v...)
}

// Warn() prints an WARN log
func (l *Aggregate) Warn(s string, v ...interface{}) {
	l.WarnLogger.Printf(s, v...)
}

// Error() prints an ERROR log
func (l *Aggregate) Error(s string, v ...interface{}) {
	l.ErrorLogger.Printf(s, v...)
}

// Init() initialise the logger and the file it prints to.
func Init(filePath string) (*Aggregate, *os.File) {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}
	l := New(file)
	return l, file
}
