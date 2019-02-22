import Deps._
import sbt.Keys.scalaVersion

lazy val root = (project in file("."))
  .aggregate(publisher, subscriber)
  .settings(
    inThisBuild(
      List(
        organization := "com.ricardohsd",
        scalaVersion := "2.12.8",
        resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
        resolvers += "io.confluent" at "http://packages.confluent.io/maven/"
      )
    ),
    name := "KafkaAvro"
  )

lazy val publisher = (project in file("publisher"))
  .settings(
    name := "publisher",
    libraryDependencies ++= Seq(
      logback,
    ),
    mainClass in (Compile, assembly) := Some("com.ricardohsd.kafka.publisher.Main"),
    assemblyJarName in assembly := "publisher.jar"
  )
  .dependsOn(common)

lazy val subscriber = (project in file("subscriber"))
  .settings(
    name := "subscriber",
    libraryDependencies ++= Seq(
      logback,
    ),
    mainClass in (Compile, assembly) := Some("com.ricardohsd.kafka.subscriber.Main"),
    assemblyJarName in assembly := "subscriber.jar"
  )
  .dependsOn(publisher, common)

lazy val common = (project in file("common")).settings(
  name := "common",
  sourceGenerators in Compile += (avroScalaGenerateSpecific in Compile).taskValue,
  libraryDependencies ++= Seq(
    kafka,
    avro,
    avroSerializer
  ),
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
    case "application.conf" => MergeStrategy.concat
    case "unwanted.txt" => MergeStrategy.discard
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  }
)
