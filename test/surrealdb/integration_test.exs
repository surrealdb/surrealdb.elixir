defmodule SurrealDB.IntegrationTest do
  @moduledoc """
  End-to-end tests against a real SurrealDB server. Excluded by default.

  Start a server and run them with:

      docker run --rm -p 8000:8000 surrealdb/surrealdb:latest \
        start --user root --pass root
      mix test --include integration

  Override the URL with the SURREALDB_URL environment variable (default
  `ws://localhost:8000/rpc`).
  """

  use ExUnit.Case

  @moduletag :integration

  @url System.get_env("SURREALDB_URL", "ws://localhost:8000/rpc")

  defp connect(opts \\ []) do
    base = [
      url: @url,
      namespace: "test",
      database: "test",
      auth: %SurrealDB.Auth.Root{user: "root", pass: "root"}
    ]

    start_supervised!({SurrealDB.Connection, Keyword.merge(base, opts)})
  end

  test "create, select, update, and delete a record" do
    db = connect()

    assert {:ok, [created]} = SurrealDB.create(db, "person", %{"name" => "Tobie", "age" => 30})
    assert created["name"] == "Tobie"

    assert {:ok, people} = SurrealDB.select(db, "person")
    assert is_list(people)

    assert {:ok, [updated]} = SurrealDB.merge(db, "person", %{"active" => true})
    assert updated["active"] == true

    assert {:ok, _} = SurrealDB.delete(db, "person")
  end

  test "query with bound variables" do
    db = connect()
    assert {:ok, [result]} = SurrealDB.query(db, "RETURN $a + $b", %{a: 2, b: 3})
    assert result == 5
  end

  test "live query receives a notification" do
    db = connect()
    assert {:ok, live_id} = SurrealDB.live(db, "watched")

    {:ok, _} = SurrealDB.create(db, "watched", %{"value" => 1})

    assert_receive {:surrealdb, :live, _id, %{action: :create}}, 2_000
    assert {:ok, _} = SurrealDB.kill(db, live_id)
  end

  test "works over the HTTP engine too" do
    http_url = String.replace(@url, ~r/^ws/, "http")
    db = connect(url: http_url)
    assert {:ok, version} = SurrealDB.version(db)
    assert is_binary(version)
  end
end
