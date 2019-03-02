defmodule KafkaAvro.Application do
  use Application

  def start(_type, _args) do
    import Supervisor.Spec

    children = [
      {KafkaAvro.SchemaRegistry, %{}},
      KafkaAvro.Producer,
      supervisor(
        KafkaEx.ConsumerGroup,
        [
          Application.get_env(:kafka_avro, :consumer_impl),
          KafkaEx.Config.consumer_group(),
          Application.get_env(:kafka_avro, :topics),
          []
        ]
      )
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
