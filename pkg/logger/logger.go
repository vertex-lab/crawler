package logger

import (
	"io"
	"log"
	"os"
)

type Aggregate struct {
	infoLogger  *log.Logger
	warnLogger  *log.Logger
	errorLogger *log.Logger
}

// New() returns an initialized Logger
func New(out io.Writer) *Aggregate {
	infoLogger := log.New(out, "INFO: ", log.LstdFlags)
	warnLogger := log.New(out, "WARN: ", log.LstdFlags)
	errorLogger := log.New(out, "ERROR: ", log.LstdFlags)

	return &Aggregate{
		infoLogger:  infoLogger,
		warnLogger:  warnLogger,
		errorLogger: errorLogger,
	}
}

// Info() prints an INFO log
func (l *Aggregate) Info(s string, v ...interface{}) {
	l.infoLogger.Printf(s, v...)
}

// Warn() prints an WARN log
func (l *Aggregate) Warn(s string, v ...interface{}) {
	l.warnLogger.Printf(s, v...)
}

// Error() prints an ERROR log
func (l *Aggregate) Error(s string, v ...interface{}) {
	l.errorLogger.Printf(s, v...)
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
