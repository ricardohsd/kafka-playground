defmodule KafkaAvro.MixProject do
  use Mix.Project

  def project do
    [
      app: :kafka_avro,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.9-dev",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {KafkaAvro.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"},
      # {:sibling_app_in_umbrella, in_umbrella: true}
      {:kafka_ex, "~> 0.8.3"},
      {:avro_ex, "~> 0.1.0-beta.6"},
      {:schemex, "~> 0.1.1"},
      {:poison, "~> 3.1"},
      {:elixir_uuid, "~> 1.2"},
      {:bypass, "~> 1.0", only: :test}
    ]
  end
end
