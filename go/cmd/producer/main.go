package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"

	"github.com/ricardohsd/kafka-playground/go/events"
	"github.com/ricardohsd/kafka-playground/go/pkg/avro"
	"github.com/ricardohsd/kafka-playground/go/pkg/gkafka"
)

func main() {
	broker := "localhost:9092"
	topic := "vehicle_updates"
	registryURL := "http://localhost:8081/"

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetLevel(logrus.DebugLevel)

	p, err := gkafka.NewProducer(broker, logger)
	if err != nil {
		panic(err)
	}

	serde, err := avro.New(registryURL)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			id := uuid.New()
			msg := &events.VehicleUpdated{
				Id:        id.String(),
				Latitude:  52.5177399,
				Longitude: 13.401178,
				Producer:  "go",
				CreatedAt: time.Now().Unix(),
				Comments:  "A comment",
			}

			data, err := serde.Serialize("vehicle_updates-value", msg)
			if err != nil {
				panic(err)
			}

			p.Produce(gkafka.Message{
				Topic: topic,
				Key:   []byte(id.String()),
				Value: data,
			})

			time.Sleep(1 * time.Second)
		}
	}()

	<-sigchan

	p.Close()
}
