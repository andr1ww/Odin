package logger

import "log"

type Logger interface {
	Success(format string, args ...interface{})
	Warning(format string, args ...interface{})
	Error(format string, args ...interface{})
}

type defaultLogger struct{}

func (*defaultLogger) Success(format string, args ...interface{}) {
	log.Printf("success: "+format, args...)
}
func (*defaultLogger) Warning(format string, args ...interface{}) {
	log.Printf("warning: "+format, args...)
}
func (*defaultLogger) Error(format string, args ...interface{}) { log.Printf("err: "+format, args...) }

type silentLogger struct{}

func (*silentLogger) Success(string, ...interface{}) {}
func (*silentLogger) Warning(string, ...interface{}) {}
func (*silentLogger) Error(string, ...interface{})   {}

var instance Logger = &defaultLogger{}

func SetLogger(l Logger) {
	if l == nil {
		instance = &silentLogger{}
	} else {
		instance = l
	}
}

func DisableLogging() { instance = &silentLogger{} }

func Success(format string, args ...interface{}) { instance.Success(format, args...) }
func Warning(format string, args ...interface{}) { instance.Warning(format, args...) }
func Error(format string, args ...interface{})   { instance.Error(format, args...) }
