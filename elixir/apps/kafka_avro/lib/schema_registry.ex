defmodule KafkaAvro.SchemaRegistry do
  use Agent

  def start_link(cache) do
    Agent.start_link(fn -> cache end, name: __MODULE__)
  end

  def latest(subject) do
    Agent.get_and_update(__MODULE__, fn cache -> _latest(cache, subject) end)
  end

  defp _latest(cache, subject) do
    with {:ok, schema} <- Map.fetch(cache, subject) do
      {{:ok, schema}, cache}
    else
      _err ->
        _latest_from_remote(cache, subject)
    end
  end

  defp _latest_from_remote(cache, subject) do
    with {:ok, %{"id" => schema_id, "schema" => _schema} = schema} <-
           Schemex.latest(registry_url(), subject) do
      cache = Map.put(cache, schema_id, schema)
      cache = Map.put(cache, subject, schema)

      {{:ok, schema}, cache}
    end
  end

  def fetch(schema_id) do
    Agent.get_and_update(__MODULE__, fn cache -> _fetch(cache, schema_id) end)
  end

  def get() do
    Agent.get(__MODULE__, & &1)
  end

  defp _fetch(cache, schema_id) do
    with {:ok, schema} <- Map.fetch(cache, schema_id) do
      {{:ok, schema}, cache}
    else
      _err ->
        fetch_from_remote(cache, schema_id)
    end
  end

  defp fetch_from_remote(cache, schema_id) do
    with {:ok, %{"schema" => _schema} = schema} <- Schemex.schema(registry_url(), schema_id) do
      cache = Map.put(cache, schema_id, schema)

      {{:ok, schema}, cache}
    else
      {:error, err} ->
        {{:error, err}, cache}
    end
  end

  defp registry_url do
    Application.get_env(:kafka_avro, :schema_registry_uri)
  end
end
