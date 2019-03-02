defmodule KafkaAvro.Serde do
  @moduledoc """
  Wrapper for serializing/deserializing Avro messages sent through Kafka with the following format:
  << Magic Byte (default 0), Schema ID (32 bits), Payload (binary) >>

  In order to deserialize it is necessary to fetch the schema from Schema Registry.

  This format is used by Confluent's Kafka Avro Serializer java/scala package.
  """

  @doc """
  Deserialize the given binary value.

  ## Examples

      iex> KafkaAvro.Serde.deserialize(<<0, 5010 :: size(32), 8, 74, 111, 104, 110, 20>>)
      {:ok, %{"name" => "John", "age" => 10}}
      iex> KafkaAvro.Serde.deserialize(<<1, 8, 74, 111, 104, 110, 20>>)
      {:error, :missing_magic_byte}
      iex> KafkaAvro.Serde.deserialize(<<0, 8>>)
      {:error, :invalid_binary}
  """
  def deserialize(<<0, schema_id::size(32), payload::binary>> = _value) do
    with {:ok, schema} <- fetch_schema(schema_id),
         {:ok, payload} <- decode(schema, payload) do
      {:ok, payload}
    else
      error ->
        error
    end
  end

  def deserialize(<<m, _schema_id::size(32), _payload::binary>> = _value) when m != 0 do
    {:error, :missing_magic_byte}
  end

  def deserialize(_payload) do
    {:error, :invalid_binary}
  end

  @doc """
  Serialize the given message in the given subject.
  It will fetch the latest schema from the Schema Registry.

  ## Examples

      iex> KafkaAvro.Serde.serialize(:latest, "users-value", %{"name" => "John", "age" => 10})
      {:ok, <<0, 5020 :: size(32), 8, 74, 111, 104, 110, 20>>}
  """
  def serialize(:latest, subject, message) do
    with {:ok, schema_id, schema} <- latest_schema(subject),
         {:ok, avro} <- AvroEx.parse_schema(schema),
         {:ok, encoded} <- encode(avro, message) do
      payload = <<0, schema_id::size(32)>> <> encoded
      {:ok, payload}
    end
  end

  defp encode(avro, message) do
    AvroEx.encode(avro, message)
  rescue
    _ ->
      {:error, :unmatching_schema}
  end

  defp latest_schema(subject) do
    with {:ok, %{"id" => schema_id, "schema" => schema}} <-
           KafkaAvro.SchemaRegistry.latest(subject) do
      {:ok, schema_id, schema}
    end
  end

  defp fetch_schema(schema_id) do
    with {:ok, %{"schema" => schema}} <- KafkaAvro.SchemaRegistry.fetch(schema_id) do
      AvroEx.parse_schema(schema)
    else
      {:error, _error} ->
        raise(RuntimeError, message: "failed to fetch schema")
    end
  end

  defp decode(schema, encoded) do
    AvroEx.decode(schema, encoded)
  rescue
    _ ->
      {:error, :unmatching_schema}
  end
end
