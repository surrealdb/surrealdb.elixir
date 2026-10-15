defmodule SurrealDB.ReconnectTest do
  use ExUnit.Case, async: true

  @moduletag :capture_log

  alias SurrealDB.{Codec, Connection, Uuid}

  @first_live "0190d6f2-8b1e-7000-8000-000000000001"
  @second_live "0190d6f2-8b1e-7000-8000-000000000002"

  # Records every RPC the connection sends, hands out a new live query id each time `live`
  # is called, and refuses connections while the process flag `:refuse` is set. It runs in
  # the connection process, so it keeps its counters in that process's dictionary.
  defp responder(parent) do
    fn
      "__connect__", _params ->
        if Process.get(:refuse), do: {:error, :econnrefused}, else: :ok

      method, params ->
        send(parent, {:rpc, method, params})

        case method do
          "signin" ->
            {:ok, "token.#{Process.put(:signins, (Process.get(:signins) || 0) + 1) || 0}"}

          "live" ->
            n = Process.put(:lives, (Process.get(:lives) || 0) + 1) || 0
            {:ok, Uuid.new(Enum.at([@first_live, @second_live], n, @second_live))}

          "version" ->
            {:ok, "surrealdb-3.3.4"}

          "hang" ->
            :noreply

          _other ->
            {:ok, nil}
        end
    end
  end

  defp start(opts \\ []) do
    base = [
      url: "ws://mock/rpc",
      engine: SurrealDB.Engine.Mock,
      responder: responder(self()),
      reconnect: [delay: 10, max_delay: 50]
    ]

    start_supervised!({Connection, Keyword.merge(base, opts)})
  end

  defp drain do
    receive do
      {:rpc, _, _} -> drain()
    after
      0 -> :ok
    end
  end

  defp notify(db, live_id) do
    payload = %{"result" => %{"id" => Uuid.new(live_id), "action" => "CREATE", "result" => %{}}}
    send(db, {:mock_message, Codec.CBOR.encode(payload)})
  end

  test "restores auth, namespace, database, parameters, and live queries" do
    db =
      start(
        namespace: "test",
        database: "app",
        auth: %SurrealDB.Auth.Root{user: "root", pass: "root"}
      )

    {:ok, _} = SurrealDB.let(db, "x", 1)
    {:ok, _} = SurrealDB.let(db, "y", 2)
    {:ok, _} = SurrealDB.unset(db, "y")
    {:ok, live_id} = SurrealDB.live(db, "person")
    :ok = SurrealDB.subscribe_status(db)
    drain()

    send(db, :mock_closed)

    assert_receive {:surrealdb, :status, :reconnecting, %{attempt: 1}}
    assert_receive {:surrealdb, :status, :connected, %{attempt: 1}}

    assert_received {:rpc, "version", []}
    assert_received {:rpc, "authenticate", ["token.0"]}
    assert_received {:rpc, "use", ["test", "app"]}
    assert_received {:rpc, "let", ["x", 1]}
    refute_received {:rpc, "let", ["y", _]}
    assert_received {:rpc, "live", [%SurrealDB.Table{name: "person"}, false]}

    # Notifications for the restarted live query arrive under the id the caller has.
    notify(db, @second_live)
    assert_receive {:surrealdb, :live, ^live_id, %{action: :create}}

    # Killing it by that id kills the new server-side live query.
    assert {:ok, _} = SurrealDB.kill(db, live_id)
    assert_receive {:rpc, "kill", [%Uuid{value: @second_live}]}
  end

  test "signs in again with the credentials when the token is refused" do
    parent = self()
    base = responder(parent)

    responder = fn
      "authenticate", params ->
        send(parent, {:rpc, "authenticate", params})
        {:error, %{"code" => -32_002, "message" => "Token expired", "kind" => "NotAllowed"}}

      method, params ->
        base.(method, params)
    end

    db = start(responder: responder, auth: %SurrealDB.Auth.Root{user: "root", pass: "root"})
    :ok = SurrealDB.subscribe_status(db)
    drain()

    send(db, :mock_closed)
    assert_receive {:surrealdb, :status, :connected, _}
    assert_received {:rpc, "authenticate", ["token.0"]}
    assert_received {:rpc, "signin", [%{"user" => "root", "pass" => "root"}]}
    assert SurrealDB.info_session(db).token == "token.1"
  end

  test "fails requests in flight instead of sending them again" do
    db = start()
    task = Task.async(fn -> Connection.call(db, "hang", []) end)
    assert_receive {:rpc, "hang", []}

    send(db, :mock_closed)

    assert {:error, %SurrealDB.Error{kind: :connection}} = Task.await(task)
    refute_receive {:rpc, "hang", []}, 100
  end

  test "holds requests made while reconnecting and sends them once connected" do
    db = start(reconnect: [delay: 200])
    :ok = SurrealDB.subscribe_status(db)
    send(db, :mock_closed)
    assert_receive {:surrealdb, :status, :reconnecting, _}

    task = Task.async(fn -> SurrealDB.query(db, "RETURN 1") end)
    refute_receive {:rpc, "query", _}, 50

    assert_receive {:surrealdb, :status, :connected, _}, 1_000
    assert_receive {:rpc, "query", ["RETURN 1", %{}]}
    assert {:ok, _} = Task.await(task)
  end

  test "a held request still times out" do
    db = start(reconnect: [delay: 500])
    send(db, :mock_closed)
    Process.sleep(10)

    assert {:error, %SurrealDB.Error{kind: :timeout}} =
             Connection.call(db, "ping", [], timeout: 50)
  end

  test "stops after the last attempt fails" do
    Process.flag(:trap_exit, true)
    parent = self()
    base = responder(parent)

    responder = fn
      "__connect__", _params ->
        if Process.get(:connected),
          do: {:error, :econnrefused},
          else: Process.put(:connected, true)

      method, params ->
        base.(method, params)
    end

    {:ok, db} =
      Connection.start_link(
        url: "ws://mock/rpc",
        engine: SurrealDB.Engine.Mock,
        responder: responder,
        reconnect: [attempts: 3, delay: 5, max_delay: 10]
      )

    ref = Process.monitor(db)
    :ok = SurrealDB.subscribe_status(db)
    send(db, :mock_closed)

    assert_receive {:surrealdb, :status, :reconnecting, %{attempt: 1}}
    assert_receive {:surrealdb, :status, :reconnecting, %{attempt: 2}}
    assert_receive {:surrealdb, :status, :reconnecting, %{attempt: 3}}
    assert_receive {:surrealdb, :status, :disconnected, %{error: %SurrealDB.Error{}}}
    assert_receive {:DOWN, ^ref, :process, ^db, {:shutdown, {:connection_lost, _}}}
  end

  test "reconnect can be turned off" do
    Process.flag(:trap_exit, true)

    {:ok, db} =
      Connection.start_link(url: "ws://mock/rpc", engine: SurrealDB.Engine.Mock, reconnect: false)

    ref = Process.monitor(db)
    send(db, :mock_closed)
    assert_receive {:DOWN, ^ref, :process, ^db, {:shutdown, {:connection_lost, _}}}
  end

  # Answers `method` with `fun.(n, params)`, where `n` counts its earlier calls, so a test
  # can change how the session restore goes. `fun` returns a responder result, or
  # `:default` for the usual one.
  defp on_reconnect(parent, method, fun) do
    base = responder(parent)

    fn
      ^method, params ->
        n = Process.put({:calls, method}, (Process.get({:calls, method}) || 0) + 1) || 0

        case fun.(n, params) do
          :default -> base.(method, params)
          result -> tap(result, fn _ -> send(parent, {:rpc, method, params}) end)
        end

      other, params ->
        base.(other, params)
    end
  end

  test "a call made while the session is being restored still times out" do
    responder =
      on_reconnect(self(), "version", fn
        0, _params -> :default
        _n, _params -> {:delay, 800, "surrealdb-3.3.4"}
      end)

    db = start(responder: responder, reconnect: [delay: 10])
    :ok = SurrealDB.subscribe_status(db)
    drain()
    send(db, :mock_closed)
    assert_receive {:rpc, "version", []}, 1_000

    started = System.monotonic_time(:millisecond)

    assert {:error, %SurrealDB.Error{kind: :timeout}} =
             Connection.call(db, "ping", [], timeout: 50)

    assert System.monotonic_time(:millisecond) - started < 600
    assert_receive {:surrealdb, :status, :connected, _}, 2_000
    refute_received {:rpc, "ping", _}
  end

  test "a held call whose timeout passes during the restore is not sent" do
    responder =
      on_reconnect(self(), "version", fn
        0, _params -> :default
        _n, _params -> {:delay, 800, "surrealdb-3.3.4"}
      end)

    db = start(responder: responder, reconnect: [delay: 50])
    :ok = SurrealDB.subscribe_status(db)
    send(db, :mock_closed)
    assert_receive {:surrealdb, :status, :reconnecting, _}

    task = Task.async(fn -> Connection.call(db, "ping", [], timeout: 200) end)
    assert {:error, %SurrealDB.Error{kind: :timeout}} = Task.await(task)
    assert_receive {:surrealdb, :status, :connected, _}, 2_000
    refute_receive {:rpc, "ping", _}, 50
  end

  test "a parameter that cannot be restored fails the reconnect attempt" do
    error = %{"code" => -32_000, "message" => "Not allowed", "kind" => "NotAllowed"}

    responder =
      on_reconnect(self(), "let", fn
        1, _params -> {:error, error}
        _n, _params -> :default
      end)

    db = start(responder: responder)
    {:ok, _} = SurrealDB.let(db, "x", 1)
    :ok = SurrealDB.subscribe_status(db)
    send(db, :mock_closed)

    assert_receive {:surrealdb, :status, :reconnecting, %{attempt: 2, error: %{kind: :rpc}}}
    assert_receive {:surrealdb, :status, :connected, %{attempt: 2}}
  end

  test "a notification in the same batch as the restarted live reply reaches subscribers" do
    notification = %{
      "result" => %{"id" => Uuid.new(@second_live), "action" => "CREATE", "result" => %{}}
    }

    responder =
      on_reconnect(self(), "live", fn
        0, _params -> {:ok, Uuid.new(@first_live)}
        _n, _params -> {:batch, Uuid.new(@second_live), [notification]}
      end)

    db = start(responder: responder)
    {:ok, live_id} = SurrealDB.live(db, "person")
    :ok = SurrealDB.subscribe_status(db)
    send(db, :mock_closed)

    assert_receive {:surrealdb, :status, :connected, _}
    assert_receive {:surrealdb, :live, ^live_id, %{action: :create}}
  end

  test "over JSON, a colon table name made while reconnecting is refused, not queued" do
    db = start(codec: :json, reconnect: [delay: 500])
    :ok = SurrealDB.subscribe_status(db)
    send(db, :mock_closed)
    assert_receive {:surrealdb, :status, :reconnecting, _}

    assert {:error, %SurrealDB.Error{kind: :unsupported}} = SurrealDB.delete(db, "user:1234")
    assert :sys.get_state(db).queue == []
  end

  test "a failed sign-in while restoring fails the reconnect attempt" do
    parent = self()
    base = responder(parent)
    refused = %{"code" => -32_002, "message" => "Not allowed", "kind" => "NotAllowed"}

    responder = fn
      "authenticate", params ->
        send(parent, {:rpc, "authenticate", params})
        {:error, refused}

      "signin", params ->
        n = Process.put(:signin_calls, (Process.get(:signin_calls) || 0) + 1) || 0

        if n == 1,
          do: tap({:error, refused}, fn _ -> send(parent, {:rpc, "signin", params}) end),
          else: base.("signin", params)

      method, params ->
        base.(method, params)
    end

    db = start(responder: responder, auth: %SurrealDB.Auth.Root{user: "root", pass: "root"})
    :ok = SurrealDB.subscribe_status(db)
    send(db, :mock_closed)

    assert_receive {:surrealdb, :status, :reconnecting, %{attempt: 2, error: %{kind: :rpc}}}
    assert_receive {:surrealdb, :status, :connected, %{attempt: 2}}
    assert :sys.get_state(db).sessions[nil].credentials
  end

  test "a call's timeout runs from when it was made, even during a slow handshake" do
    parent = self()
    base = responder(parent)

    responder = fn
      "__connect__", params ->
        n = Process.put(:connects, (Process.get(:connects) || 0) + 1) || 0

        if n > 0 do
          send(parent, :handshaking)
          Process.sleep(1_000)
        end

        base.("__connect__", params)

      "version", params ->
        n = Process.put(:versions, (Process.get(:versions) || 0) + 1) || 0
        if n > 0, do: {:delay, 2_000, "surrealdb-3.3.4"}, else: base.("version", params)

      method, params ->
        base.(method, params)
    end

    db = start(responder: responder, reconnect: [delay: 10])
    send(db, :mock_closed)
    assert_receive :handshaking, 1_000

    started = System.monotonic_time(:millisecond)

    assert {:error, %SurrealDB.Error{kind: :timeout}} =
             Connection.call(db, "ping", [], timeout: 800)

    assert System.monotonic_time(:millisecond) - started < 1_500
  end

  test "a restarted live query that times out retries the reconnect instead of ending" do
    responder =
      on_reconnect(self(), "live", fn
        1, _params -> :noreply
        _n, _params -> :default
      end)

    db = start(responder: responder, connect_timeout: 200)
    {:ok, live_id} = SurrealDB.live(db, "person")
    :ok = SurrealDB.subscribe_status(db)
    send(db, :mock_closed)

    assert_receive {:surrealdb, :status, :reconnecting, %{attempt: 2, error: %{kind: :timeout}}},
                   3_000

    assert_receive {:surrealdb, :status, :connected, %{attempt: 2}}, 3_000
    refute_received {:surrealdb, :live, ^live_id, %{action: :killed}}
    assert Map.has_key?(:sys.get_state(db).live_specs, live_id.value)
  end

  test "a socket that closes during a restore leaves no stale sync result behind" do
    responder =
      on_reconnect(self(), "version", fn
        n, _params when n in [1, 2] ->
          send(self(), :mock_closed)
          :noreply

        _n, _params ->
          :default
      end)

    db = start(responder: responder)
    :ok = SurrealDB.subscribe_status(db)
    send(db, :mock_closed)

    assert_receive {:surrealdb, :status, :connected, %{attempt: 3}}, 3_000
    assert :sys.get_state(db).sync == %{}
  end

  test "backoff stops growing at the cap, however many attempts" do
    config = [delay: 1_000, max_delay: 60_000, multiplier: 2.0, jitter: 0.0]
    assert Connection.backoff(config, 5_000) == 60_000
    assert Connection.backoff(Keyword.put(config, :jitter, 0.1), 1_000_000) in 54_000..60_000
    assert Connection.backoff(Keyword.put(config, :multiplier, 1.0), 1_000_000) == 1_000
  end

  test "backoff grows by the multiplier up to the cap" do
    config = [delay: 100, max_delay: 1_000, multiplier: 2.0, jitter: 0.0]
    assert Enum.map(1..6, &Connection.backoff(config, &1)) == [100, 200, 400, 800, 1_000, 1_000]

    jittered = Connection.backoff(Keyword.put(config, :jitter, 0.1), 1)
    assert jittered in 90..110
  end
end
