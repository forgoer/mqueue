package mqueue

import (
	"errors"
	"github.com/forgoer/mqueue/queue"
)

type Config struct {
	Name   string
	Url    string
	Driver string
}

type Queue interface {
	Send(body []byte) error
	Delay(body []byte, expire string) error
	Receive(callback func(body []byte)) error
}

func New(c Config) (Queue, error) {
	switch c.Driver {
	case "rabbit":
		return queue.NewRabbitQueue(c.Name, c.Url)
	}

	return nil, errors.New("unsupported")
}
