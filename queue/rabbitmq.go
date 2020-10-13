package queue

import (
	"github.com/streadway/amqp"
	"time"
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
	bound      bool
	logger     Logger
}

func NewRabbitQueue(name, url string) (*Queue, error) {
	var err error

	q := &Queue{
		url:    url,
		name:   name,
		logger: &defaultLogger{},
	}

	connection, err := amqp.Dial(q.url)
	if err != nil {
		return nil, err
	}

	q.connctions = append(q.connctions, &Connection{
		connction: connection,
	})

	q.queueBindToExchange()

	return q, nil
}

func (q *Queue) queueBindToExchange() error {
	if q.bound {
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

	_, err = ch.QueueDeclare(q.name, true, false, false, false, nil)
	if err != nil {
		return err
	}

	exchange := q.getExchangeName()
	err = ch.ExchangeDeclare(exchange, "fanout", true, false, false, false, nil)
	if err != nil {
		return err
	}

	err = ch.QueueBind(q.name, "", exchange, false, nil)
	if err != nil {
		return err
	}

	q.bound = true

	return nil
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
			"x-dead-letter-exchange": q.getExchangeName(),
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

func (q *Queue) getExchangeName() string {
	return q.name + "_exchange"
}
func (q *Queue) getDelayExchangeName() string {
	return q.name + "_delay_exchange"
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
	exchangeName := q.getExchangeName()
	if expire != "" {
		err := q.startDelay()
		if err != nil {
			return err
		}
		exchangeName = q.getDelayExchangeName()
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
		exchangeName, // exchange
		"",           // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         body,
			Expiration:   expire, //
			DeliveryMode: 2,
		})

	return err
}

func (q *Queue) Receive(callback func(body []byte)) error {
	go q.receive(callback)
	return nil
}

func (q *Queue) receive(callback func(body []byte)) {
	defer func() {
		if err := recover(); err != nil {
			q.logger.Error("rabbit receive error: %+v", err)
			time.Sleep(3 * time.Second)
			q.receive(callback)
		}
	}()

	conn, err := q.getConnection()
	if err != nil {
		panic(err)
	}
	ch, err := conn.getChannel()

	if err != nil {
		panic(err)
	}

	q.logger.Info("rabbit start receive")

	defer ch.Close()
	closeChan := make(chan *amqp.Error, 1)
	notifyClose := ch.NotifyClose(closeChan)
	closeFlag := false

	msgs, err := ch.Consume(q.name, "", true, false, false, false, nil)
	if err != nil {
		panic(err)
	}

	for {
		select {
		case e := <-notifyClose:
			q.logger.Error("rabbit channel error: %s", e.Error())
			time.Sleep(5 * time.Second)
			//StartAMQPConsume()
			closeFlag = true
		case d := <-msgs:
			callback(d.Body)
		}
		if closeFlag {
			break
		}
	}

	panic("rabbit receive interrupt")
}

func (q *Queue) SetLogger(l Logger) () {
	q.logger = l
}

func (q *Queue) EnableLog(e bool) {
	q.logger.Enable(e)
}
