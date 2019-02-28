package gkafka

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sirupsen/logrus"
)

var ErrConsumeFuncNotFound = errors.New("ConsumeFunc must be defined")

type ConsumeFunc func(uint64, io.Reader)

type Consumer struct {
	client      *kafka.Consumer
	doneC       chan struct{}
	consumeFunc ConsumeFunc
	logger      *logrus.Logger
}

func NewConsumer(topics []string, broker string, group string, logger *logrus.Logger) (*Consumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               broker,
		"group.id":                        group,
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		// Enable generation of PartitionEOF when the
		// end of a partition is reached.
		"enable.partition.eof": true,
		"auto.offset.reset":    "earliest",
	})

	if err != nil {
		return nil, err
	}

	err = c.SubscribeTopics(topics, nil)
	if err != nil {
		return nil, err
	}

	consumer := &Consumer{
		client: c,
		doneC:  make(chan struct{}),
		logger: logger,
	}

	go consumer.Start()

	return consumer, nil
}

func (c *Consumer) Start() {
	c.logger.Debug("Consumer started")

	for {
		select {
		case done := <-c.doneC:
			c.logger.Debugf("Caught signal %v: terminating", done)
			return

		case ev := <-c.client.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				c.logger.Debugf("Partition assigned %v", e)
				c.client.Assign(e.Partitions)
			case *kafka.Message:
				c.consume(e)
			case kafka.Error:
				c.Close()
			}
		}
	}
}

func (c *Consumer) Close() {
	c.logger.Debug("Broadcast termination")

	c.doneC <- struct{}{}
	c.client.Close()
}

func (c *Consumer) ConsumeWith(f ConsumeFunc) {
	c.consumeFunc = f
}

func (c *Consumer) consume(message *kafka.Message) {
	if c.consumeFunc == nil {
		panic(ErrConsumeFuncNotFound)
	}
	buff := bytes.NewBuffer(message.Value)

	// magic byte
	buff.ReadByte()

	schemaID := binary.BigEndian.Uint32(buff.Next(4))

	c.logger.WithFields(logrus.Fields{
		"topicPartition": message.TopicPartition,
		"schemaID":       schemaID,
		"key":            string(message.Key),
	}).Debugf("Received message")

	c.consumeFunc(uint64(schemaID), buff)
}
