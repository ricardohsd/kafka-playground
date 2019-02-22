module Playground
  class Producer
    def initialize(topic:, kafka:, logger:, encoder:)
      @topic = topic
      @logger = logger || Logger.new(STDOUT)
      @kafka = kafka || Kafka.new(['localhost:9092'],
                                  client_id: 'my-application',
                                  logger: @logger)
      @encoder = encoder

      start
    end

    def stop
      producer.shutdown
    end

    def produce(key:, data:)
      encoded_data = encoder.encode(data)
      producer.produce(encoded_data, topic: topic, key: key)

      logger.info('message produced')
    end

    private

    attr_reader :topic, :kafka, :logger, :encoder

    def start
      logger.info('Producer started')

      producer.deliver_messages
    end

    def producer
      @producer ||= kafka.async_producer(
        delivery_interval: 10
      )
    end
  end
end
