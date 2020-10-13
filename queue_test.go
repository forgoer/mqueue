package mqueue

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
	"time"
)

func getQueue() (Queue, error) {
	queue, err := New(Config{
		Name:   "Test",
		Url:    "amqp://test:123456@127.0.0.1:5672/test-vhost",
		Driver: "rabbit",
	})
	if err != nil {
		panic(err)
	}

	return queue, err
}

func TestNew(t *testing.T) {
	var err error
	queue, _ := getQueue()
	queue.EnableLog(true)

	var send = func(i int) error {
		message := fmt.Sprintf("%d message", i)
		log.Println(fmt.Sprintf("Send a message: %s", message))
		return queue.Delay([]byte(message), "1000")
	}

	err = queue.Receive(func(body []byte) {
		log.Println(fmt.Sprintf("Received：%s\n", string(body)))
	})
	assert.NoError(t, err)

	go func() {
		for i := 0; ; i++ {
			err := send(i)
			assert.NoError(t, err)
			time.Sleep(3 * time.Second)
		}
	}()

	//time.Sleep(10 * time.Second)
	forever := make(chan bool)
	<-forever
}
