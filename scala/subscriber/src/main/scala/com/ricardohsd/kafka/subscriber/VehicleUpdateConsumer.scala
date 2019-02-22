package com.ricardohsd.kafka.subscriber

import java.util
import java.util.Properties
import java.util.concurrent.{ExecutorService, Executors}

import com.ricardohsd.events.VehicleUpdated
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

class VehicleUpdateConsumer(val kafkaURL: String, val registryURL: String, val topic: String) {

  val props: Properties = consumerConfig()
  val consumer = new KafkaConsumer[String, VehicleUpdated](props)
  var executor: ExecutorService = _

  def shutdown(): Unit = {
    consumer.close()

    if (executor != null)
      executor.shutdown()
  }

  def run(): Unit = {
    //subscribe to producer's topic
    consumer.subscribe(util.Arrays.asList(topic))

    Executors.newSingleThreadExecutor.execute(new Runnable {
      override def run(): Unit = {
        //poll for new messages every two seconds
        while (true) {
          val records = consumer.poll(2000)

          records.forEach(record => {
            val m = Map(
              "key" -> record.key,
              "id" -> record.value.id,
              "latitude" -> record.value.latitude,
              "longitude" -> record.value.longitude,
              "producer" -> record.value.producer,
              "createdAt" -> record.value.createdAt,
              "comments" -> record.value.comments
            )
            println("Consumed message: " + m)
          })

          //commit offsets on last poll
          consumer.commitSync()
        }
      }
    })
  }

  def consumerConfig(): Properties = {
    //consumer properties
    val consumerProps: Properties = new Properties()

    //set deserializer to same classes as producer's serializer
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getCanonicalName)

    //set kafka url and schema registry url
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaURL)
    consumerProps.put("schema.registry.url", registryURL)

    //set group id and set specific avro reader to true
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "vehicleScala1")
    consumerProps.put("specific.avro.reader", "true")

    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    consumerProps
  }
}
