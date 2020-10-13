package queue

import (
	"fmt"
	"log"
)

var logger Logger = &defaultLogger{}

type Logger interface {
	Info(format string, v ...interface{})
	Error(format string, v ...interface{})
	Enable(e bool)
}

type defaultLogger struct {
	enable bool
}

func (l *defaultLogger) Info(format string, v ...interface{}) {
	if !l.enable {
		return
	}
	log.Println(fmt.Sprintf(format, v...))
}

func (l *defaultLogger) Error(format string, v ...interface{}) {
	if !l.enable {
		return
	}
	log.Println(fmt.Sprintf(format, v...))
}

func (l *defaultLogger) Enable(e bool) {
	l.enable = e
}