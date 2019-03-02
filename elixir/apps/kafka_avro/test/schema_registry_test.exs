defmodule KafkaAvro.SchemaRegistryTest do
  use ExUnit.Case
  doctest KafkaAvro.SchemaRegistry

  setup_all do
    bypass = Bypass.open()

    schema =
      Poison.encode!(%{
        type: "record",
        namespace: "com.example",
        name: "User",
        fields: [%{name: "name", type: "string"}, %{name: "address", type: "string"}]
      })

    Application.put_env(:kafka_avro, :schema_registry_uri, "http://localhost:#{bypass.port}")

    [bypass: bypass, schema: schema]
  end

  setup %{schema: schema} do
    Agent.update(KafkaAvro.SchemaRegistry, fn _cache ->
      %{10 => %{"id" => 10, "schema" => schema}}
    end)
  end

  test "returns cached schema", %{schema: raw_schema} do
    {:ok, %{"schema" => schema}} = KafkaAvro.SchemaRegistry.fetch(10)

    assert schema == raw_schema
  end

  test "return data from remote", %{bypass: bypass, schema: raw_schema} do
    KafkaAvroTest.BypassHelper.mock_schema_registry(bypass, raw_schema, 123)

    {:ok, %{"schema" => schema}} = KafkaAvro.SchemaRegistry.fetch(123)

    assert schema == raw_schema

    cache = KafkaAvro.SchemaRegistry.get()

    %{123 => %{"schema" => schema}} = cache

    assert schema == raw_schema
  end

  test "when schema doesn't exists", %{bypass: bypass} do
    KafkaAvroTest.BypassHelper.mock_invalid_request(bypass, 400)

    assert {:error, err} = KafkaAvro.SchemaRegistry.fetch(400)
  end
end
