package queue

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

const url = "amqp://test:paddword@127.0.0.1:5672/test-local"

func getRabbitQueue() (*Queue, error) {
	client, err := NewRabbitQueue("test", url)
	if err != nil {
		panic(err)
	}
	return client, err
}

func TestNewQueue(t *testing.T) {
	client, err := getRabbitQueue()
	msg := "message for test"
	err = client.Send([]byte(msg))
	assert.NoError(t, err)

	err = client.Receive(func(body []byte) {
		t.Log(string(body))
		assert.Equal(t, msg, body)
	})
	assert.NoError(t, err)
}

func TestRabbitMQ_Delay(t *testing.T) {
	client, err := getRabbitQueue()
	assert.NoError(t, err)

	msg := "message for test"

	receiveMsg := []byte("")
	go func() {
		err = client.Receive(func(body []byte) {
			t.Log(string(body))
			receiveMsg = body
		})
		assert.NoError(t, err)
	}()

	d, err:= time.ParseDuration("302.464584ms")
	assert.NoError(t, err)

	fmt.Println( strconv.FormatInt(d.Milliseconds(), 10))

	err = client.Delay([]byte(msg), strconv.FormatInt(d.Milliseconds(), 10))
	assert.NoError(t, err)

	assert.Equal(t, []byte(""), receiveMsg)
	time.Sleep(4 * time.Second)
	assert.Equal(t, []byte(msg), receiveMsg)
}
