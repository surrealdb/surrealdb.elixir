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

  # A fresh database per test, so tests never see each other's data and every run starts
  # from a database that does not exist yet.
  setup do
    Process.put(
      :it_database,
      "it_#{System.system_time(:microsecond)}_#{System.unique_integer([:positive])}"
    )

    :ok
  end

  defp connect(opts \\ []) do
    base = [
      url: @url,
      namespace: "test",
      database: Process.get(:it_database, "test"),
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
    {:ok, _} = SurrealDB.query(db, "DEFINE TABLE watched")
    assert {:ok, live_id} = SurrealDB.live(db, "watched")

    {:ok, _} = SurrealDB.create(db, "watched", %{"value" => 1})

    assert_receive {:surrealdb, :live, _id, %{action: :create}}, 2_000
    assert {:ok, _} = SurrealDB.kill(db, live_id)
  end

  test "signs in before selecting a database that does not exist yet" do
    db = connect()
    assert {:ok, [created]} = SurrealDB.create(db, "fresh", %{"n" => 1})
    assert created["n"] == 1
  end

  test "use with only a database keeps the namespace on the server" do
    db = connect()
    {:ok, _} = SurrealDB.create(db, "marker", %{"n" => 1})

    assert {:ok, _} = SurrealDB.use(db, namespace: "test", database: nil)
    assert {:ok, [[%{"n" => 1}]]} = SurrealDB.query(db, "SELECT n FROM marker")

    assert {:ok, _} = SurrealDB.use(db, database: :unset)

    assert {:ok, [%{"namespace" => "test", "database" => db_name}]} =
             SurrealDB.query(db, "RETURN { namespace: session::ns(), database: session::db() }")

    assert db_name in [nil, %SurrealDB.None{}]
  end

  describe "losing the connection" do
    alias SurrealDB.Test.TCPProxy

    defp proxied(scheme) do
      %URI{host: host, port: port} = URI.parse(@url)
      proxy = start_supervised!({TCPProxy, upstream: {String.to_charlist(host), port}})
      {proxy, "#{scheme}://127.0.0.1:#{TCPProxy.port(proxy)}/rpc"}
    end

    @tag :capture_log
    test "a dropped WebSocket stops the connection instead of leaving it dead" do
      Process.flag(:trap_exit, true)
      {proxy, url} = proxied("ws")

      {:ok, db} =
        SurrealDB.Connection.start_link(
          url: url,
          namespace: "test",
          database: Process.get(:it_database),
          auth: %SurrealDB.Auth.Root{user: "root", pass: "root"}
        )

      ref = Process.monitor(db)
      assert {:ok, _} = SurrealDB.version(db)

      task = Task.async(fn -> SurrealDB.query(db, "SLEEP 2s; RETURN 1") end)
      Process.sleep(100)
      TCPProxy.cut(proxy)

      assert {:error, %SurrealDB.Error{kind: :connection}} = Task.await(task, 1_000)
      assert_receive {:DOWN, ^ref, :process, ^db, {:shutdown, {:connection_lost, _}}}, 1_000
    end

    test "HTTP reconnects after the keep-alive connection closes" do
      {proxy, url} = proxied("http")
      db = connect(url: url)
      assert {:ok, _} = SurrealDB.version(db)

      TCPProxy.cut(proxy)
      Process.sleep(50)

      assert {:ok, [2]} = SurrealDB.query(db, "RETURN 1 + 1")
      assert {:ok, [3]} = SurrealDB.query(db, "RETURN 1 + 2")
      assert TCPProxy.accepted(proxy) == 2
    end
  end

  test "works over the HTTP engine too" do
    http_url = String.replace(@url, ~r/^ws/, "http")
    db = connect(url: http_url)
    assert {:ok, version} = SurrealDB.version(db)
    assert is_binary(version)
  end
end
