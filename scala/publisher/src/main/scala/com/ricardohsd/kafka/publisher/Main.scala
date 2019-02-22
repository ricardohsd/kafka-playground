package com.ricardohsd.kafka.publisher

object Main extends App {
  val kafkaUrl = "http://localhost:9092"
  val registryUrl = "http://localhost:8081"

  val producer =
    new VehicleUpdateProducer(kafkaUrl, registryUrl, "vehicle_updates")

  //close consumer on ctrl-c
  Runtime.getRuntime.addShutdownHook(new Thread(() => producer.shutdown()))

  producer.run()
}
