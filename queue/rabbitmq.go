package queue

import (
	"github.com/streadway/amqp"
)

type Connection struct {
	name      string
	connction *amqp.Connection
	channels  []*amqp.Channel
}

func (c *Connection) getChannel() (*amqp.Channel, error) {
	return c.connction.Channel()
}

type Queue struct {
	connctions []*Connection
	url        string
	name       string
	delay      bool
}

func NewRabbitQueue(name, url string) (*Queue, error) {
	var err error

	q := &Queue{
		url:  url,
		name: name,
	}

	connection, err := amqp.Dial(q.url)
	if err != nil {
		return nil, err
	}

	q.connctions = append(q.connctions, &Connection{
		connction: connection,
	})

	conn, err := q.getConnection()
	if err != nil {
		return nil, err
	}
	ch, err := conn.getChannel()

	if err != nil {
		return nil, err
	}

	defer ch.Close()

	_, err = ch.QueueDeclare(q.name, true, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	exchange := q.name + "_exchange"
	err = ch.ExchangeDeclare(exchange, "fanout", true, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	err = ch.QueueBind(q.name, "", exchange, false, nil)
	if err != nil {
		return nil, err
	}

	return q, nil
}

func (q *Queue) startDelay() error {
	if q.delay {
		return nil
	}
	conn, err := q.getConnection()
	if err != nil {
		return err
	}
	ch, err := conn.getChannel()

	if err != nil {
		return err
	}

	defer ch.Close()

	name := q.name + "_delay"
	// 声明一个延时队列,这个队列不做消费,而是让消息变成死信后再进行转发
	_, err = ch.QueueDeclare(
		name,  // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		amqp.Table{
			"x-dead-letter-exchange": q.name + "_exchange",
		}, // arguments
	)
	if err != nil {
		return err
	}

	exchange := name + "_exchange"
	err = ch.ExchangeDeclare(exchange, "fanout", true, false, false, false, nil)
	if err != nil {
		return err
	}
	err = ch.QueueBind(name, "", exchange, false, nil)
	if err != nil {
		return err
	}

	q.delay = true
	return nil
}

func (q *Queue) getConnection() (*Connection, error) {
	conn := q.connctions[0]
	if conn.connction.IsClosed() {
		connction, err := amqp.Dial(q.url)
		if err != nil {
			return nil, err
		}
		conn.connction = connction
		q.connctions[0] = conn
	}

	return conn, nil
}

func (q *Queue) Send(body []byte) error {
	return q.Delay(body, "")
}

func (q *Queue) Delay(body []byte, expire string) error {
	name := q.name
	if expire != "" {
		err := q.startDelay()
		if err != nil {
			return err
		}
		name = name + "_delay"
	}

	conn, err := q.getConnection()
	if err != nil {
		return err
	}
	ch, err := conn.getChannel()

	if err != nil {
		return err
	}

	defer ch.Close()
	err = ch.Publish(
		name+"_exchange", // exchange
		"",               // routing key
		false,            // mandatory
		false,            // immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         body,
			Expiration:   expire, //
			DeliveryMode: 2,
		})

	return err
}

func (q *Queue) Receive(callback func(body []byte)) error {
	conn, err := q.getConnection()
	if err != nil {
		return err
	}
	ch, err := conn.getChannel()

	if err != nil {
		return err
	}

	//defer ch.Close()

	msgs, err := ch.Consume(q.name, "", true, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for d := range msgs {
			callback(d.Body)
		}
	}()

	return nil
}
