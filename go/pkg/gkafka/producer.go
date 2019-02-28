package gkafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sirupsen/logrus"
)

type Message struct {
	Topic string
	Key   []byte
	Value []byte
}

type Producer struct {
	client    *kafka.Producer
	doneC     chan struct{}
	messagesC chan Message
	logger    *logrus.Logger
}

func NewProducer(broker string, logger *logrus.Logger) (*Producer, error) {
	client, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
	})

	if err != nil {
		return nil, err
	}

	p := &Producer{
		client:    client,
		doneC:     make(chan struct{}),
		messagesC: make(chan Message),
		logger:    logger,
	}

	go p.Start()
	go p.processFeedback()

	return p, nil
}

func (p *Producer) Start() {
	for {
		select {
		case done := <-p.doneC:
			p.logger.Debugf("Caught signal %v: terminating", done)
			return
		case message := <-p.messagesC:
			kafkaMessage := &kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &message.Topic,
					Partition: kafka.PartitionAny,
				},
				Key:   message.Key,
				Value: message.Value,
			}
			p.client.ProduceChannel() <- kafkaMessage
		}
	}
}

func (p *Producer) Produce(message Message) {
	p.messagesC <- message
}

func (p *Producer) processFeedback() {
	for {
		select {
		case e := <-p.client.Events():
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					p.logger.Debugf("Delivery failed: %v", m.TopicPartition.Error)
				} else {
					p.logger.Debugf("Delivered message to topic %s [%d] at offset %v",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
			}
		}
	}
}

func (p *Producer) Close() {
	p.doneC <- struct{}{}
	p.client.Close()
}
