package mqueue

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func getQueue() (Queue, error) {
	queue, err := New(Config{
		Name: "Test",
		Url:    "amqp://test:paddword@127.0.0.1:5672/test-local",
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
	var send = func(i int) error {
		message := fmt.Sprintf("%d message", i)
		fmt.Println(fmt.Sprintf("Send a message: %s", message))
		return queue.Delay([]byte(message), "3000")
	}

	var i int

	err = queue.Receive(func(body []byte) {
		fmt.Println(fmt.Sprintf("Received：%s", string(body)))
		err := send(i + 1)
		i++
		assert.NoError(t, err)
	})

	assert.NoError(t, err)

	err = send(0)

	assert.NoError(t, err)

	time.Sleep(10 * time.Second)
	//forever := make(chan bool)
	//<-forever
}
