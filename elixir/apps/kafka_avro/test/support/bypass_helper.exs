defmodule KafkaAvroTest.BypassHelper do
  def mock_schema_registry(bypass, schema, id) do
    Bypass.expect_once(bypass, "GET", "/schemas/ids/#{id}", fn conn ->
      Plug.Conn.resp(
        conn,
        200,
        Poison.encode!(%{schema: schema})
      )
    end)
  end

  def mock_get_latest_schema(bypass, subject, schema, id) do
    Bypass.expect_once(bypass, "GET", "/subjects/#{subject}/versions/latest", fn conn ->
      Plug.Conn.resp(
        conn,
        200,
        Poison.encode!(%{schema: schema, id: id})
      )
    end)
  end

  def mock_invalid_request(bypass, id) do
    Bypass.expect_once(bypass, "GET", "/schemas/ids/#{id}", fn conn ->
      Plug.Conn.resp(conn, 404, ~s<{"error_code": "40403", "message": "Schema not found."}>)
    end)
  end
end
