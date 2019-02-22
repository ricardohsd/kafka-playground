package com.ricardohsd.kafka.subscriber

object Main extends App {
  val kafkaUrl = "http://localhost:9092"
  val registryUrl = "http://localhost:8081"

  val consumer =
    new VehicleUpdateConsumer(kafkaUrl, registryUrl, "vehicle_updates")

  //close consumer on ctrl-c
  Runtime.getRuntime.addShutdownHook(new Thread(() => consumer.shutdown()))

  consumer.run()
}
