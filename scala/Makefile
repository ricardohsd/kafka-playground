build:
	sbt publisher/assembly && sbt subscriber/assembly

producer:
	java -jar publisher/target/scala-2.12/publisher.jar

consumer:
	java -jar subscriber/target/scala-2.12/subscriber.jar

runSubscriber:
	sbt "subscriber/runMain com.ricardohsd.kafka.subscriber.Main"

runPublisher:
	sbt "publisher/runMain com.ricardohsd.kafka.publisher.Main"
