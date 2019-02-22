module Playground
  module Events
    class VehicleUpdated
      def initialize(registry_url:, schemas_path:)
        schema_store = AvroSerde::SchemaStore.new(path: schemas_path)
        @serde = AvroSerde::Messaging.new(
          registry_url: registry_url,
          schema_store: schema_store
        )
      end

      def decode(data)
        @serde.decode(data)
      end

      def encode(event)
        @serde.encode(event,
                    subject: 'vehicle_updates-value', # follow schema's subject name used by io.confluent.kafka-avro-serializer java lib
                    schema_name: 'VehicleUpdated',
                    namespace: 'com.ricardohsd.events')
      end
    end
  end
end
