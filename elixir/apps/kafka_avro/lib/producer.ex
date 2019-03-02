defmodule KafkaAvro.Producer do
  @moduledoc """
  Produces a message every 1 second to vehicle_updates topic
  """
  use GenServer
  require Logger

  alias KafkaEx.Protocol.Produce.Message
  alias KafkaEx.Protocol.Produce.Request

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{})
  end

  def init(state) do
    schedule()
    {:ok, state}
  end

  def handle_info(:schedule, state) do
    message = %{
      "id" => UUID.uuid4(),
      "latitude" => 52.5177399,
      "longitude" => 13.401178,
      "producer" => "elixir",
      "createdAt" => System.system_time(:second),
      "comments" => "A comment"
    }

    {:ok, payload} = KafkaAvro.Serde.serialize(:latest, "vehicle_updates-value", message)

    message = %Message{value: payload, key: message["id"]}

    produce_request = %Request{
      partition: 0,
      topic: "vehicle_updates",
      messages: [message]
    }

    KafkaEx.produce(produce_request)

    schedule()
    {:noreply, state}
  end

  defp schedule() do
    Process.send_after(self(), :schedule, 1_000)
  end
end
