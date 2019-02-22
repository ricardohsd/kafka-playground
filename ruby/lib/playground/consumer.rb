module Playground
  class Consumer
    def initialize(topic:, kafka: nil, logger: nil, decoder:, consumer_options: {})
      @topic = topic
      @logger = logger || Logger.new(STDOUT)
      @kafka = kafka || Kafka.new(['localhost:9092'],
                                  client_id: 'my-application',
                                  logger: @logger)
      @decoder = decoder
      @consumer_options = default_consumer_options.merge(consumer_options)

      start
    end

    def stop
      consumer.stop
    end

    def each_message(&block)
      consumer.each_message do |event|
        message = decoder.decode(event.value)
        block.call(
          offset: event.offset,
          partition: event.partition,
          key: event.key,
          message: message
        )
      end
    end

    private

    attr_reader :topic, :logger, :kafka, :decoder

    def start
      logger.info('Consumer started')

      consumer.subscribe('vehicle_updates')
    end

    def consumer
      @consumer ||= kafka.consumer(**@consumer_options)
    end

    def default_consumer_options
      {
        group_id: 'ruby-consumer',
        offset_commit_interval: 30,
        offset_commit_threshold: 100
      }
    end
  end
end
