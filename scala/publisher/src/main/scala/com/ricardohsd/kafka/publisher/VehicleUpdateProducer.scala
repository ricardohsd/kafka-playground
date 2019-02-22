package com.ricardohsd.kafka.publisher

import java.time.Instant
import java.util.Properties
import java.util.concurrent.{ExecutorService, Executors}

import com.ricardohsd.events.VehicleUpdated
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroSerializer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

class VehicleUpdateProducer(val kafkaURL: String, val registryURL: String, val topic: String) {

  val producerProps: Properties = config()
  val producer = new KafkaProducer[String, VehicleUpdated](producerProps)
  var executor: ExecutorService = _

  def shutdown(): Unit = {
    producer.close()

    if (executor != null)
      executor.shutdown()
  }

  def run(): Unit = {
    Executors.newSingleThreadExecutor.execute(new Runnable {
      override def run(): Unit = {
        while (true) {
          val currentTime = Instant.now.getEpochSecond
          val uuid: String = java.util.UUID.randomUUID.toString
          val latitude: Double = 52.5177399
          val longitude: Double = 13.401178

          producer.send(
            new ProducerRecord(
              "vehicle_updates",
              uuid,
              new VehicleUpdated(uuid, latitude, longitude, "scala", currentTime, "comment ")
            )
          )

          println("publishing message")

          Thread.sleep(1000)
        }
      }
    })
  }

  def config(): Properties = {
    //producer properties
    val producerProps: Properties = new Properties()

    //set key serializer
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)

    //set value serializer to avro serializer
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getCanonicalName)

    //set kafka url
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaURL)

    //set schema registry url
    producerProps.put("schema.registry.url", registryURL)

    //disable auto register schemas
    producerProps.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, "false")

    producerProps
  }
}
