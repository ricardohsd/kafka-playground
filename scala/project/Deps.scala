import sbt._

object Deps {
  lazy val kafka = "org.apache.kafka" % "kafka_2.12" % "2.1.1"
  lazy val avro = "org.apache.avro" % "avro" % "1.8.2"
  lazy val avroSerializer = "io.confluent" % "kafka-avro-serializer" % "5.0.0"
  lazy val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"
}
