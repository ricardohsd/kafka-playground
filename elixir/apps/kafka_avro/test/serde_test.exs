defmodule KafkaAvro.SerdeTest do
  use ExUnit.Case
  alias KafkaAvroTest.BypassHelper
  doctest KafkaAvro.Serde

  setup_all do
    bypass = Bypass.open()

    schema =
      Poison.encode!(%{
        type: "record",
        namespace: "com.example",
        name: "User",
        fields: [%{name: "name", type: "string"}, %{name: "age", type: "int"}]
      })

    BypassHelper.mock_schema_registry(bypass, schema, 5010)
    BypassHelper.mock_get_latest_schema(bypass, "users-value", schema, 5020)
    Application.put_env(:kafka_avro, :schema_registry_uri, "http://localhost:#{bypass.port}")

    [bypass: bypass, schema: schema]
  end

  describe "deserialize" do
    test "from binary", %{bypass: bypass, schema: schema} do
      BypassHelper.mock_schema_registry(bypass, schema, 1010)

      # encoded represents an avro message in the following format:
      # <<magic_byte, schema_id(32 bits), data::binary>>
      encoded = <<0, 1010::size(32), 8, 74, 111, 104, 110, 20>>

      {:ok, message} = KafkaAvro.Serde.deserialize(encoded)

      assert message == %{"name" => "John", "age" => 10}
    end

    test "returns error when missing magic byte" do
      encoded = <<1, 123::size(32), 8, 74, 111, 104, 110, 20>>

      assert {:error, :missing_magic_byte} = KafkaAvro.Serde.deserialize(encoded)
    end

    test "returns error when invalid schema id", %{bypass: bypass} do
      BypassHelper.mock_invalid_request(bypass, 139_095_912)

      encoded = <<0, 8, 74, 111, 104, 110, 20>>

      assert_raise RuntimeError, "failed to fetch schema", fn ->
        KafkaAvro.Serde.deserialize(encoded)
      end
    end

    test "returns error when schema doesn't match", %{bypass: bypass, schema: schema} do
      BypassHelper.mock_schema_registry(bypass, schema, 1020)

      # { "name" => "John", "address" => "1st Crate, Mars"}
      encoded =
        <<0, 1020::size(32), 8, 74, 111, 104, 110, 30, 49, 115, 116, 32, 67, 114, 97, 116, 101,
          44, 32, 77, 97, 114, 115>>

      assert {:error, :unmatching_schema} = KafkaAvro.Serde.deserialize(encoded)
    end
  end

  describe "serialize" do
    test "returns error when schema doesn't match", %{bypass: bypass} do
      schema =
        Poison.encode!(%{
          type: "record",
          namespace: "com.example",
          name: "User",
          fields: [%{name: "name", type: "string"}, %{name: "address", type: "string"}]
        })

      BypassHelper.mock_get_latest_schema(bypass, "user_addresses-value", schema, 5021)

      {:error, :unmatching_schema} =
        KafkaAvro.Serde.serialize(:latest, "user_addresses-value", %{
          "name" => "John",
          "age" => 10
        })
    end
  end
end
