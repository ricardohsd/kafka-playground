package main

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/ricardohsd/kafka-playground/go/events"
	"github.com/ricardohsd/kafka-playground/go/pkg/gkafka"
	"github.com/sirupsen/logrus"
)

func main() {
	broker := "localhost:9092"
	group := "goConsumer1"
	topics := []string{"vehicle_updates"}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetLevel(logrus.DebugLevel)

	c, err := gkafka.NewConsumer(topics, broker, group, logger)
	if err != nil {
		panic(err)
	}
	c.ConsumeWith(func(_schemaID uint64, r io.Reader) {
		event, err := events.DeserializeVehicleUpdated(r)
		if err != nil {
			panic(err)
		}
		logger.Infof("Event: %+v", event)
	})

	<-sigchan

	fmt.Printf("Closing consumer\n")
	c.Close()
}
