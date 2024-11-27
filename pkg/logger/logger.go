package logger

import (
	"log"
	"os"
)

type Aggregate struct {
	infoLogger  *log.Logger
	warnLogger  *log.Logger
	errorLogger *log.Logger
}

// New() returns an initialized Logger
func New(file *os.File) *Aggregate {
	infoLogger := log.New(file, "INFO: ", log.LstdFlags)
	warnLogger := log.New(file, "WARN: ", log.LstdFlags)
	errorLogger := log.New(file, "ERROR: ", log.LstdFlags)

	return &Aggregate{
		infoLogger:  infoLogger,
		warnLogger:  warnLogger,
		errorLogger: errorLogger,
	}
}

// Info() prints an INFO log
func (l *Aggregate) Info(v ...interface{}) {
	l.infoLogger.Println(v...)
}

// Warn() prints an WARN log
func (l *Aggregate) Warn(v ...interface{}) {
	l.warnLogger.Println(v...)
}

// Error() prints an ERROR log
func (l *Aggregate) Error(v ...interface{}) {
	l.errorLogger.Println(v...)
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
