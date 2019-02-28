# Kafka + Avro playground

This repo is a personal playground where I experiment how to produce & consume
Avro Kafka events and using Schema Registry through different languages.

**Disclaimer: This repo isn't designed to be a base for production code.**

Initially my goals are:
- [x] Produce/consume from Ruby
- [x] Produce/consume from Scala
- [x] Produce/consume from Golang
- [ ] Produce/consume from Elixir
- [ ] Use Akka Streams to process and save events into a db
- [ ] Use Kafka Streams to re-partition the data

# Setup

Start docker-compose:
```
$ make up
```

Create topic:
```
$ make create-topic name=vehicle_updates
```

# Ruby

Go to `./ruby` folder. Then run `bundle install`.

To produce events:
```
$ make producer
```

To consume events:
```
$ make consumer
```

# Scala

Go to `./scala` folder and build the app's jar: `make build`.

To produce events:
```
$ make producer
```

To consume events:
```
$ make consumer
```
