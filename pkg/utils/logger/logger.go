// The package logger defines a simple logger with INFO, WARN and ERROR prints.
package logger

import (
	"io"
	"log"
)

type Aggregate struct {
	InfoLogger  *log.Logger
	WarnLogger  *log.Logger
	ErrorLogger *log.Logger
}

// New() returns an initialized Logger
func New(out io.Writer) *Aggregate {
	return &Aggregate{
		InfoLogger:  log.New(out, "INFO: ", log.LstdFlags),
		WarnLogger:  log.New(out, "WARN: ", log.LstdFlags),
		ErrorLogger: log.New(out, "ERROR: ", log.LstdFlags),
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
