defmodule SurrealDB.ConnectionTest do
  use ExUnit.Case, async: true

  alias SurrealDB.{Codec, RecordId, Table, Uuid}

  @live_uuid "0190d6f2-8b1e-7000-8000-000000000001"

  defp responder do
    fn
      "use", _params -> {:ok, nil}
      "signin", _params -> {:ok, "jwt.token"}
      "create", [thing, data] -> {:ok, [Map.put(data, "thing", inspect(thing))]}
      "select", [thing] -> {:ok, %{"selected" => inspect(thing)}}
      "query", _params -> {:ok, [%{"status" => "OK", "time" => "1ms", "result" => [1, 2, 3]}]}
      "query_err", _params -> {:ok, [%{"status" => "ERR", "result" => "bad query"}]}
      "live", [_thing, _diff] -> {:ok, Uuid.new(@live_uuid)}
      "kill", _params -> {:ok, nil}
      "boom", _params -> {:error, %{"code" => -32_000, "message" => "explosion"}}
      "hang", _params -> :noreply
      method, params -> {:ok, %{"method" => method, "params" => params}}
    end
  end

  defp start(opts \\ []) do
    base = [url: "ws://mock/rpc", engine: SurrealDB.Engine.Mock, responder: responder()]
    start_supervised!({SurrealDB.Connection, Keyword.merge(base, opts)})
  end

  test "connects and authenticates on setup" do
    db =
      start(
        namespace: "test",
        database: "app",
        auth: %SurrealDB.Auth.Root{user: "root", pass: "root"}
      )

    info = SurrealDB.info_session(db)

    assert info.namespace == "test"
    assert info.database == "app"
    assert info.authenticated
    assert info.supports_live?
  end

  describe "use/2" do
    defp recording_start(opts) do
      parent = self()

      responder = fn
        "use", params ->
          send(parent, {:use, params})
          {:ok, nil}

        _method, _params ->
          {:ok, nil}
      end

      start(Keyword.merge([responder: responder], opts))
    end

    test "sends NONE for a missing or nil selection so the server keeps it" do
      db = recording_start(namespace: "test", database: "app")
      assert_receive {:use, ["test", "app"]}

      assert {:ok, _} = SurrealDB.use(db, database: "other")
      assert_receive {:use, [%SurrealDB.None{}, "other"]}

      assert {:ok, _} = SurrealDB.use(db, namespace: "test2", database: nil)
      assert_receive {:use, ["test2", %SurrealDB.None{}]}

      info = SurrealDB.info_session(db)
      assert {info.namespace, info.database} == {"test2", "other"}
    end

    test "sends NULL for :unset so the server clears it" do
      db = recording_start(namespace: "test", database: "app")
      assert_receive {:use, _}

      assert {:ok, _} = SurrealDB.use(db, database: :unset)
      assert_receive {:use, [%SurrealDB.None{}, nil]}
      assert SurrealDB.info_session(db).database == nil
      assert SurrealDB.info_session(db).namespace == "test"

      assert {:ok, _} = SurrealDB.use(db, database: "app")
      assert {:ok, _} = SurrealDB.use(db, namespace: :unset)

      assert {SurrealDB.info_session(db).namespace, SurrealDB.info_session(db).database} ==
               {nil, nil}
    end

    test "selecting only a database keeps it when no namespace is set" do
      db = recording_start([])
      assert {:ok, _} = SurrealDB.use(db, database: "app")
      assert_receive {:use, [%SurrealDB.None{}, "app"]}
      assert SurrealDB.info_session(db).database == "app"
    end

    test "over JSON, an unchanged selection is sent as its current value, not NULL" do
      db = recording_start(codec: :json, namespace: "test", database: "app")
      assert_receive {:use, ["test", "app"]}

      assert {:ok, _} = SurrealDB.use(db, database: "other")
      assert_receive {:use, ["test", "other"]}

      assert {:ok, _} = SurrealDB.use(db, database: :unset)
      assert_receive {:use, ["test", nil]}
      assert SurrealDB.info_session(db).namespace == "test"
    end

    test "connecting with only a namespace leaves the database unchanged" do
      _db = recording_start(namespace: "test")
      assert_receive {:use, ["test", %SurrealDB.None{}]}
    end
  end

  test "keeps the access token from a signin that returns a token pair" do
    responder = fn
      "signin", _params -> {:ok, %{"access" => "access.jwt", "refresh" => "refresh.jwt"}}
      _method, _params -> {:ok, nil}
    end

    db = start(responder: responder)
    auth = %SurrealDB.Auth.Record{namespace: "n", database: "d", access: "account"}

    assert {:ok, %{"access" => "access.jwt"}} = SurrealDB.signin(db, auth)
    assert SurrealDB.info_session(db).token == "access.jwt"
    assert :sys.get_state(db).refresh_token == "refresh.jwt"
  end

  test "create normalises a bare table name to a Table" do
    db = start()
    {:ok, [record]} = SurrealDB.create(db, "person", %{"name" => "Tobie"})
    assert record["thing"] == inspect(Table.new("person"))
  end

  test "select treats a string with a colon as a table name, never a record id" do
    db = start()
    {:ok, result} = SurrealDB.select(db, "person:tobie")
    assert result["selected"] == inspect(Table.new("person:tobie"))
  end

  test "select passes record ids through" do
    db = start()
    {:ok, result} = SurrealDB.select(db, RecordId.parse!("person:tobie"))
    assert result["selected"] == inspect(RecordId.new("person", "tobie"))
  end

  test "relate rejects strings for the endpoints" do
    db = start()

    assert_raise ArgumentError, ~r/never treated as record ids/, fn ->
      SurrealDB.relate(db, "person:tobie", "wrote", RecordId.new("article", 1))
    end
  end

  test "query returns one result per statement" do
    db = start()
    assert SurrealDB.query(db, "SELECT 1") == {:ok, [[1, 2, 3]]}
  end

  test "query_raw returns the raw statement objects" do
    db = start()

    assert {:ok, [%{"status" => "OK", "result" => [1, 2, 3]}]} =
             SurrealDB.query_raw(db, "SELECT 1")
  end

  test "rpc errors are surfaced as SurrealDB.Error" do
    db = start()

    assert {:error, %SurrealDB.Error{kind: :rpc, message: "explosion", code: -32_000}} =
             SurrealDB.Connection.call(db, "boom", [])
  end

  test "let and unset update parameters" do
    db = start()
    assert {:ok, _} = SurrealDB.let(db, "x", 1)
    assert {:ok, _} = SurrealDB.unset(db, "x")
  end

  describe "connection loss" do
    @tag :capture_log
    test "a closed transport fails pending calls and stops the process" do
      Process.flag(:trap_exit, true)

      {:ok, db} =
        SurrealDB.Connection.start_link(
          url: "ws://mock/rpc",
          engine: SurrealDB.Engine.Mock,
          responder: responder()
        )

      ref = Process.monitor(db)
      task = Task.async(fn -> SurrealDB.Connection.call(db, "hang", []) end)
      wait_for_pending(db)

      send(db, :mock_closed)

      assert {:error, %SurrealDB.Error{kind: :connection}} = Task.await(task)
      assert_receive {:DOWN, ^ref, :process, ^db, {:shutdown, {:connection_lost, _}}}
    end

    test "a reset fails the calls in flight and keeps the process" do
      db = start()
      task = Task.async(fn -> SurrealDB.Connection.call(db, "hang", []) end)
      wait_for_pending(db)

      send(db, {:mock_reset, :closed})

      assert {:error, %SurrealDB.Error{kind: :connection}} = Task.await(task)
      assert Process.alive?(db)
      assert {:ok, _} = SurrealDB.version(db)
    end
  end

  defp wait_for_pending(db) do
    if map_size(:sys.get_state(db).pending) == 0 do
      Process.sleep(5)
      wait_for_pending(db)
    end
  end

  describe "live queries" do
    test "delivers notifications to the subscriber and stops on kill" do
      db = start()
      {:ok, live_id} = SurrealDB.live(db, "person")
      assert %Uuid{value: @live_uuid} = live_id

      notification = %{
        "id" => Uuid.new(@live_uuid),
        "action" => "CREATE",
        "result" => %{"id" => "person:new", "name" => "New"}
      }

      send(db, {:mock_message, Codec.CBOR.encode(notification)})

      assert_receive {:surrealdb, :live, _id, %{action: :create, result: result}}, 1_000
      assert result == %{"id" => "person:new", "name" => "New"}

      assert {:ok, _} = SurrealDB.kill(db, live_id)
    end

    test "routes notifications to a custom subscriber pid" do
      db = start()
      parent = self()
      subscriber = spawn(fn -> relay(parent) end)
      {:ok, _live_id} = SurrealDB.live(db, "person", to: subscriber)

      notification = %{"id" => Uuid.new(@live_uuid), "action" => "UPDATE", "result" => %{}}
      send(db, {:mock_message, Codec.CBOR.encode(notification)})

      assert_receive {:relayed, {:surrealdb, :live, _id, %{action: :update}}}, 1_000
    end
  end

  defp relay(parent) do
    receive do
      message -> send(parent, {:relayed, message})
    end
  end
end
