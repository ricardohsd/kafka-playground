defmodule KafkaAvro.Consumer do
  use KafkaEx.GenConsumer

  alias KafkaEx.Protocol.Fetch.Message

  require Logger

  # note - messages are delivered in batches
  def handle_message_set(message_set, state) do
    for message <- message_set do
      handle_message(message)
    end

    {:sync_commit, state}
  end

  def handle_message(%Message{value: value} = message) do
    with {:ok, event} <- KafkaAvro.Serde.deserialize(value) do
      Logger.debug("Key #{message.key} Offset #{inspect(message.offset)} Event #{inspect(event)}")
    else
      {:error, error} ->
        Logger.debug("ERROR error: #{inspect(error)}")
    end
  end
end
