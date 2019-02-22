up:
	docker-compose up

clear:
	docker-compose rm -vf

list-topics:
	docker-compose exec kafka1 kafka-topics --list --zookeeper zoo1:2181

create-topic:
	docker-compose exec kafka1 kafka-topics --create --topic $(name) --partitions 3 --replication-factor 1 --if-not-exists --zookeeper zoo1:2181

delete-topic:
	docker-compose exec kafka1 kafka-topics --zookeeper zoo1:2181 --describe --topic $(name)

schemas-list:
	curl -H "Accept: application/vnd.schemaregistry.v1+json" -vvv http://localhost:8081/subjects
