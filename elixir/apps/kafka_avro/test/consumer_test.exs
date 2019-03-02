defmodule KafkaAvro.ConsumerTest do
  use ExUnit.Case, async: true
  doctest KafkaAvro.Consumer

  alias KafkaEx.Protocol.Fetch.Message

  @topic "topic"
  @partition 0

  setup do
    {:ok, state} = KafkaAvro.Consumer.init(@topic, @partition)
    {:ok, %{state: state}}
  end

  test "it acks a message", %{state: state} do
    message_set = [%Message{offset: 0, value: "hello"}]
    {response, _new_state} = KafkaAvro.Consumer.handle_message_set(message_set, state)
    assert response == :sync_commit
  end
end
