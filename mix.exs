defmodule SurrealDB.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/surrealdb/surrealdb.elixir"

  def project do
    [
      app: :surrealdb,
      version: @version,
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      name: "SurrealDB",
      description: description(),
      package: package(),
      docs: docs(),
      source_url: @source_url
    ]
  end

  def application do
    [
      extra_applications: [:logger, :crypto, :ssl]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:mint, "~> 1.6"},
      {:mint_web_socket, "~> 1.0"},
      {:castore, "~> 1.0"},
      {:jason, "~> 1.4"},
      {:decimal, "~> 2.1"},
      {:bypass, "~> 2.1", only: :test},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end

  defp description do
    "Elixir SDK for SurrealDB and Agent Memory. WebSocket and HTTP engines, CBOR and JSON " <>
      "protocols, live queries, and a typed agent memory client."
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib mix.exs README.md LICENSE CHANGELOG.md .formatter.exs)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "CHANGELOG.md"],
      source_ref: "v#{@version}",
      groups_for_modules: [
        "Database client": [
          SurrealDB,
          SurrealDB.Connection,
          SurrealDB.Auth,
          SurrealDB.Error
        ],
        "Transport engines": [
          SurrealDB.Engine,
          SurrealDB.Engine.WebSocket,
          SurrealDB.Engine.HTTP
        ],
        Protocols: [
          SurrealDB.Codec,
          SurrealDB.Codec.CBOR,
          SurrealDB.Codec.JSON
        ],
        "Value types": [
          SurrealDB.RecordId,
          SurrealDB.Table,
          SurrealDB.Uuid,
          SurrealDB.Duration,
          SurrealDB.Datetime,
          SurrealDB.Geometry,
          SurrealDB.Range,
          SurrealDB.Future,
          SurrealDB.Bytes,
          SurrealDB.None
        ],
        "Agent Memory": [
          SurrealDB.Memory,
          SurrealDB.Memory.Documents,
          SurrealDB.Memory.Entities,
          SurrealDB.Memory.Sessions,
          SurrealDB.Memory.Lifecycle,
          SurrealDB.Memory.Traces,
          SurrealDB.Memory.Principals,
          SurrealDB.Memory.Scopes,
          SurrealDB.Memory.Keys,
          SurrealDB.Memory.Error
        ]
      ]
    ]
  end
end
