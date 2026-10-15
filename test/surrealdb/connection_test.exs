defmodule SurrealDB.ConnectionTest do
  use ExUnit.Case, async: true

  alias SurrealDB.{Codec, RecordId, Table, Uuid}

  defmodule Wrapper do
    @moduledoc false
    defstruct [:range]
  end

  @live_uuid "0190d6f2-8b1e-7000-8000-000000000001"

  defp responder do
    fn
      "use", _params -> {:ok, nil}
      "version", _params -> {:ok, "surrealdb-3.0.0"}
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
    opts = Keyword.merge(base, opts)
    start_supervised!({SurrealDB.Connection, Keyword.update!(opts, :responder, &with_version/1)})
  end

  # Most responders here answer methods they do not care about with nil; give the version
  # check a version to accept.
  defp with_version(responder) do
    fn
      "version", params ->
        case responder.("version", params) do
          {:ok, nil} -> {:ok, "surrealdb-3.0.0"}
          other -> other
        end

      method, params ->
        responder.(method, params)
    end
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
    assert SurrealDB.info_session(db).refresh_token == "refresh.jwt"
  end

  test "over JSON, a table name with a colon is refused rather than read as a record id" do
    db = start(codec: :json)

    assert {:error, %SurrealDB.Error{kind: :unsupported}} = SurrealDB.select(db, "user:1234")
    assert {:ok, _} = SurrealDB.select(db, "user")
  end

  test "over JSON, a record id range is refused rather than sent as a plain object" do
    db = start(codec: :json)
    range = SurrealDB.RecordIdRange.new("t", {:incl, 1}, {:incl, 5})

    assert {:error, %SurrealDB.Error{kind: :unsupported}} = SurrealDB.select(db, range)

    assert {:error, %SurrealDB.Error{kind: :unsupported}} =
             SurrealDB.query(db, "SELECT * FROM $r", %{"r" => range})

    assert {:error, %SurrealDB.Error{kind: :unsupported}} =
             SurrealDB.query(db, "SELECT * FROM $r", %{"r" => %Wrapper{range: range}})

    assert {:ok, _} = SurrealDB.select(db, "t")
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

  describe "version check" do
    # Sends replies to a request nobody made until the connection process exits.
    defp chatter(conn) do
      if Process.alive?(conn) do
        reply = Codec.CBOR.encode(%{"id" => 999_999, "result" => nil})
        send(conn, {:mock_message, reply})
        Process.sleep(20)
        chatter(conn)
      end
    end

    defp version_responder(version) do
      fn
        "version", _params -> {:ok, version}
        _method, _params -> {:ok, nil}
      end
    end

    test "refuses to start against an unsupported server" do
      Process.flag(:trap_exit, true)

      assert {:error, %SurrealDB.Error{kind: :unsupported}} =
               SurrealDB.Connection.start_link(
                 url: "ws://mock/rpc",
                 engine: SurrealDB.Engine.Mock,
                 responder: version_responder("surrealdb-5.0.0")
               )
    end

    test "refuses a version reply that is not a string" do
      Process.flag(:trap_exit, true)

      for reply <- [nil, %{"v" => 1}] do
        assert {:error, %SurrealDB.Error{kind: :unsupported}} =
                 SurrealDB.Connection.start_link(
                   url: "ws://mock/rpc",
                   engine: SurrealDB.Engine.Mock,
                   responder: version_responder(reply)
                 )
      end

      db = start(responder: version_responder(%{"v" => 1}), version_check: false)
      assert SurrealDB.info_session(db).version == nil
    end

    test "keeps the transport options for one-off HTTP requests" do
      db = start(transport_opts: [verify: :verify_none])
      assert SurrealDB.info_session(db).transport_opts == [verify: :verify_none]
    end

    test "unrelated transport traffic does not push back the setup deadline" do
      Process.flag(:trap_exit, true)

      responder = fn
        "version", _params ->
          conn = self()
          spawn(fn -> chatter(conn) end)
          :noreply

        _method, _params ->
          {:ok, nil}
      end

      {micros, result} =
        :timer.tc(fn ->
          SurrealDB.Connection.start_link(
            url: "ws://mock/rpc",
            engine: SurrealDB.Engine.Mock,
            responder: responder,
            connect_timeout: 200
          )
        end)

      assert {:error, %SurrealDB.Error{kind: :timeout}} = result
      assert micros < 1_000_000
    end

    test "a transport that closes during the version call fails startup, check or not" do
      Process.flag(:trap_exit, true)

      responder = fn
        "version", _params ->
          send(self(), :mock_closed)
          :noreply

        _method, _params ->
          {:ok, nil}
      end

      assert {:error, %SurrealDB.Error{kind: :connection}} =
               SurrealDB.Connection.start_link(
                 url: "ws://mock/rpc",
                 engine: SurrealDB.Engine.Mock,
                 responder: responder,
                 version_check: false
               )
    end

    test "starts against an unsupported server when the check is off" do
      db = start(responder: version_responder("surrealdb-5.0.0"), version_check: false)
      assert SurrealDB.info_session(db).version == "surrealdb-5.0.0"
    end

    test "records the server version" do
      db = start(responder: version_responder("surrealdb-3.3.4"))
      assert SurrealDB.info_session(db).version == "surrealdb-3.3.4"
    end
  end

  describe "timeouts" do
    test "a request times out after the given timeout and the connection carries on" do
      db = start()

      {micros, result} =
        :timer.tc(fn -> SurrealDB.Connection.call(db, "hang", [], timeout: 50) end)

      assert {:error, %SurrealDB.Error{kind: :timeout}} = result
      assert micros < 1_000_000
      assert :sys.get_state(db).pending == %{}
      assert {:ok, _} = SurrealDB.Connection.call(db, "ping", [])
    end

    test "a late reply to a timed-out request is dropped, not read as a notification" do
      responder = fn
        "select", _params -> :noreply
        "live", _params -> {:ok, Uuid.new(@live_uuid)}
        "version", _params -> {:ok, "surrealdb-3.3.4"}
        _method, params -> {:ok, params}
      end

      db = start(responder: responder, timeout: 50)
      {:ok, live_id} = SurrealDB.live(db, "person")

      assert {:error, %SurrealDB.Error{kind: :timeout}} = SurrealDB.select(db, "audit")

      id = :sys.get_state(db).next_id - 1
      row = %{"id" => Uuid.new(@live_uuid), "action" => "CREATE", "result" => %{}}
      send(db, {:mock_message, Codec.CBOR.encode(%{"id" => id, "result" => row})})

      refute_receive {:surrealdb, :live, ^live_id, _}, 100
      assert :sys.get_state(db).pending == %{}
    end

    test "a message whose id nobody is waiting for is not a notification" do
      responder = fn
        "live", _params -> {:ok, Uuid.new(@live_uuid)}
        "version", _params -> {:ok, "surrealdb-3.3.4"}
        _method, params -> {:ok, params}
      end

      db = start(responder: responder)
      {:ok, live_id} = SurrealDB.live(db, "person")

      row = %{"id" => Uuid.new(@live_uuid), "action" => "CREATE", "result" => %{}}
      send(db, {:mock_message, Codec.CBOR.encode(%{"id" => 999_999, "result" => row})})

      refute_receive {:surrealdb, :live, ^live_id, _}, 100
    end

    test "a timed-out use still updates the session when its reply arrives late" do
      responder = fn
        "use", _params -> :noreply
        "version", _params -> {:ok, "surrealdb-3.3.4"}
        _method, params -> {:ok, params}
      end

      db = start(responder: responder, timeout: 50)

      assert {:error, %SurrealDB.Error{kind: :timeout}} =
               SurrealDB.use(db, namespace: "ns", database: "db")

      [id] = Map.keys(:sys.get_state(db).pending)
      send(db, {:mock_message, Codec.CBOR.encode(%{"id" => id, "result" => nil})})

      info = SurrealDB.info_session(db)
      assert {info.namespace, info.database} == {"ns", "db"}
      assert :sys.get_state(db).pending == %{}
    end

    test "a live query that starts after its call timed out is killed" do
      parent = self()

      responder = fn
        "live", _params -> :noreply
        "kill", params -> tap({:ok, nil}, fn _ -> send(parent, {:kill, params}) end)
        "version", _params -> {:ok, "surrealdb-3.3.4"}
        _method, params -> {:ok, params}
      end

      db = start(responder: responder, timeout: 50)
      assert {:error, %SurrealDB.Error{kind: :timeout}} = SurrealDB.live(db, "person")

      [id] = Map.keys(:sys.get_state(db).pending)
      late = %{"id" => id, "result" => Uuid.new(@live_uuid)}
      send(db, {:mock_message, Codec.CBOR.encode(late)})

      assert_receive {:kill, [%Uuid{value: @live_uuid}]}
      assert :sys.get_state(db).live == %{}
    end

    test "a call that runs out of time in the mailbox is not sent" do
      parent = self()

      responder = fn
        "create", params -> tap({:ok, params}, fn _ -> send(parent, :created) end)
        "version", _params -> {:ok, "surrealdb-3.3.4"}
        _method, params -> {:ok, params}
      end

      db = start(responder: responder)
      :ok = :sys.suspend(db)

      task =
        Task.async(fn -> SurrealDB.Connection.call(db, "create", ["person"], timeout: 50) end)

      Process.sleep(150)
      :ok = :sys.resume(db)

      assert {:error, %SurrealDB.Error{kind: :timeout}} = Task.await(task)
      refute_receive :created, 100
    end

    test "the connection-wide timeout applies by default" do
      db = start(timeout: 50)

      assert {:error, %SurrealDB.Error{kind: :timeout}} =
               SurrealDB.Connection.call(db, "hang", [])
    end
  end

  test "reset clears the session state" do
    db =
      start(
        namespace: "test",
        database: "app",
        auth: %SurrealDB.Auth.Root{user: "root", pass: "root"}
      )

    assert {:ok, _} = SurrealDB.reset(db)

    info = SurrealDB.info_session(db)
    assert {info.namespace, info.database, info.token} == {nil, nil, nil}
  end

  describe "connection loss" do
    @tag :capture_log
    test "a closed transport fails pending calls and stops the process" do
      Process.flag(:trap_exit, true)

      {:ok, db} =
        SurrealDB.Connection.start_link(
          url: "ws://mock/rpc",
          engine: SurrealDB.Engine.Mock,
          responder: responder(),
          reconnect: false
        )

      ref = Process.monitor(db)
      task = Task.async(fn -> SurrealDB.Connection.call(db, "hang", []) end)
      wait_for_pending(db)

      send(db, :mock_closed)

      assert {:error, %SurrealDB.Error{kind: :connection}} = Task.await(task)
      assert_receive {:DOWN, ^ref, :process, ^db, {:shutdown, {:connection_lost, _}}}
    end

    @tag :capture_log
    test "an internal kill still in flight does not stop the callers being told" do
      Process.flag(:trap_exit, true)
      base = responder()

      responder = fn
        "kill", _params -> :noreply
        method, params -> base.(method, params)
      end

      {:ok, db} =
        SurrealDB.Connection.start_link(
          url: "ws://mock/rpc",
          engine: SurrealDB.Engine.Mock,
          responder: responder,
          reconnect: false
        )

      subscriber = spawn(fn -> Process.sleep(:infinity) end)
      {:ok, _live_id} = SurrealDB.live(db, "person", to: subscriber)

      # The last subscriber exits, so the connection sends a kill nobody waits for.
      Process.exit(subscriber, :kill)
      wait_for_pending(db)

      task = Task.async(fn -> SurrealDB.Connection.call(db, "hang", []) end)
      wait_for_pending(db, 2)
      ref = Process.monitor(db)
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

  defp wait_for_pending(db, count \\ 1) do
    if map_size(:sys.get_state(db).pending) < count do
      Process.sleep(5)
      wait_for_pending(db, count)
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

    test "over JSON, live returns the same Uuid its notifications carry" do
      responder = fn
        "live", _params -> {:ok, String.upcase(@live_uuid)}
        "version", _params -> {:ok, "surrealdb-3.3.4"}
        _method, params -> {:ok, params}
      end

      db = start(codec: :json, responder: responder)
      {:ok, live_id} = SurrealDB.live(db, "person")
      assert live_id == Uuid.new(@live_uuid)

      notification = %{"result" => %{"id" => @live_uuid, "action" => "CREATE", "result" => %{}}}
      send(db, {:mock_message, Codec.JSON.encode(notification)})

      assert_receive {:surrealdb, :live, ^live_id, %{action: :create}}, 1_000
    end
  end

  describe "live subscribers" do
    defp notify(db, action, result \\ %{}) do
      payload = %{
        "result" => %{"id" => Uuid.new(@live_uuid), "action" => action, "result" => result}
      }

      send(db, {:mock_message, Codec.CBOR.encode(payload)})
    end

    defp spawn_relay do
      parent = self()
      spawn(fn -> relay_forever(parent) end)
    end

    defp relay_forever(parent) do
      receive do
        :stop ->
          :ok

        message ->
          send(parent, {:relayed, self(), message})
          relay_forever(parent)
      end
    end

    test "subscribe_live adds a subscriber instead of replacing the existing one" do
      db = start()
      {:ok, live_id} = SurrealDB.live(db, "person")
      other = spawn_relay()
      :ok = SurrealDB.subscribe_live(db, live_id, other)

      notify(db, "CREATE")

      assert_receive {:surrealdb, :live, _, %{action: :create}}
      assert_receive {:relayed, ^other, {:surrealdb, :live, _, %{action: :create}}}
    end

    test "unsubscribe_live stops delivery to that pid only" do
      db = start()
      {:ok, live_id} = SurrealDB.live(db, "person")
      other = spawn_relay()
      :ok = SurrealDB.subscribe_live(db, live_id, other)
      :ok = SurrealDB.unsubscribe_live(db, live_id)

      notify(db, "UPDATE")

      assert_receive {:relayed, ^other, {:surrealdb, :live, _, %{action: :update}}}
      refute_receive {:surrealdb, :live, _, _}, 50
    end

    test "kills the live query when its last subscriber exits" do
      parent = self()

      responder = fn
        "live", _params ->
          {:ok, Uuid.new(@live_uuid)}

        "kill", params ->
          send(parent, {:killed, params})
          {:ok, nil}

        _method, _params ->
          {:ok, nil}
      end

      db = start(responder: responder)
      first = spawn_relay()
      second = spawn_relay()
      {:ok, live_id} = SurrealDB.live(db, "person", to: first)
      :ok = SurrealDB.subscribe_live(db, live_id, second)

      send(first, :stop)
      refute_receive {:killed, _}, 50
      notify(db, "CREATE")
      assert_receive {:relayed, ^second, {:surrealdb, :live, _, %{action: :create}}}

      send(second, :stop)
      assert_receive {:killed, [%Uuid{value: @live_uuid}]}, 1_000
      assert :sys.get_state(db).live == %{}
      assert :sys.get_state(db).monitors == %{}
    end

    test "a KILLED notification is delivered and then the live query is forgotten" do
      db = start()
      {:ok, _live_id} = SurrealDB.live(db, "person")

      notify(db, "KILLED", nil)
      assert_receive {:surrealdb, :live, _, %{action: :killed}}

      notify(db, "CREATE")
      refute_receive {:surrealdb, :live, _, _}, 50
      assert :sys.get_state(db).live == %{}
      assert :sys.get_state(db).monitors == %{}
    end

    test "a reply whose result has an action field is still a reply" do
      responder = fn
        "select", _params -> {:ok, %{"id" => "log:1", "action" => "login"}}
        _method, _params -> {:ok, nil}
      end

      db = start(responder: responder)
      assert {:ok, %{"action" => "login"}} = SurrealDB.select(db, RecordId.new("log", 1))
    end
  end

  defp relay(parent) do
    receive do
      message -> send(parent, {:relayed, message})
    end
  end
end
