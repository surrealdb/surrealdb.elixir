defmodule SurrealDB.SessionTest do
  use ExUnit.Case, async: true

  @moduletag :capture_log

  alias SurrealDB.{Connection, Error, Session, Transaction, Uuid}

  @txn "0190d6f2-8b1e-7000-8000-0000000000aa"

  # Reports every RPC with its session and txn envelope fields, as strings or nil.
  defp responder(parent, overrides \\ %{}) do
    fn
      "__connect__", _params, _request ->
        :ok

      method, params, request ->
        session = request["session"] && to_string(request["session"])
        txn = request["txn"] && to_string(request["txn"])
        send(parent, {:rpc, method, params, session, txn})

        case Map.fetch(overrides, method) do
          {:ok, reply} ->
            reply

          :error ->
            case method do
              "version" -> {:ok, "surrealdb-3.3.4"}
              "begin" -> {:ok, Uuid.new(@txn)}
              "signin" -> {:ok, "token"}
              _ -> {:ok, nil}
            end
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
      {:rpc, _, _, _, _} -> drain()
    after
      0 -> :ok
    end
  end

  describe "sessions" do
    test "new_session attaches a fresh session id and calls on it carry the id" do
      db = start()
      drain()

      assert {:ok, %Session{conn: ^db, id: %Uuid{value: id}} = session} =
               SurrealDB.new_session(db)

      assert_received {:rpc, "attach", [], ^id, nil}

      {:ok, _} = SurrealDB.select(session, "person")
      assert_received {:rpc, "select", _, ^id, nil}

      {:ok, _} = SurrealDB.select(db, "person")
      assert_received {:rpc, "select", _, nil, nil}
    end

    test "each session keeps its own namespace, database, and parameters" do
      db = start(namespace: "ns", database: "main")
      {:ok, session} = SurrealDB.new_session(db)

      {:ok, _} = SurrealDB.use(session, namespace: "ns", database: "tenant")
      {:ok, _} = SurrealDB.let(session, "x", 1)

      assert %{database: "tenant", vars: %{"x" => 1}} = SurrealDB.info_session(session)
      assert %{database: "main", vars: vars} = SurrealDB.info_session(db)
      assert vars == %{}
    end

    test "fork_session copies the namespace, database, token, and parameters" do
      db = start(namespace: "ns", database: "main")
      {:ok, _} = SurrealDB.signin(db, %SurrealDB.Auth.Root{user: "root", pass: "root"})
      {:ok, _} = SurrealDB.let(db, "x", 1)
      drain()

      assert {:ok, %Session{id: %Uuid{value: id}} = fork} = SurrealDB.fork_session(db)

      assert_received {:rpc, "attach", [], ^id, nil}
      assert_received {:rpc, "authenticate", ["token"], ^id, nil}
      assert_received {:rpc, "use", ["ns", "main"], ^id, nil}
      assert_received {:rpc, "let", ["x", 1], ^id, nil}

      assert %{namespace: "ns", database: "main", token: "token", vars: %{"x" => 1}} =
               SurrealDB.info_session(fork)
    end

    test "close_session detaches the session and forgets it" do
      db = start()
      {:ok, %Session{id: %Uuid{value: id}} = session} = SurrealDB.new_session(db)

      assert {:ok, _} = SurrealDB.close_session(session)
      assert_received {:rpc, "detach", [], ^id, nil}
      assert :sys.get_state(db).sessions |> Map.keys() == [nil]
    end

    test "sessions lists the server's sessions on the connection" do
      ids = [Uuid.new(@txn)]
      db = start(responder: responder(self(), %{"sessions" => {:ok, ids}}))
      {:ok, session} = SurrealDB.new_session(db)

      assert {:ok, ^ids} = SurrealDB.sessions(session)
      assert_received {:rpc, "sessions", [], nil, nil}
    end

    test "a reconnect attaches each session again and restores its state" do
      db = start(namespace: "ns", database: "main")
      {:ok, %Session{id: %Uuid{value: id}} = session} = SurrealDB.new_session(db)
      {:ok, _} = SurrealDB.use(session, namespace: "ns", database: "tenant")
      {:ok, _} = SurrealDB.let(session, "x", 1)
      {:ok, _live} = SurrealDB.live(session, "person")
      :ok = SurrealDB.subscribe_status(db)
      drain()

      send(db, :mock_closed)
      assert_receive {:surrealdb, :status, :connected, _}, 5_000

      assert_received {:rpc, "use", ["ns", "main"], nil, nil}
      assert_received {:rpc, "attach", [], ^id, nil}
      assert_received {:rpc, "use", ["ns", "tenant"], ^id, nil}
      assert_received {:rpc, "let", ["x", 1], ^id, nil}
      assert_received {:rpc, "live", _, ^id, nil}
    end

    test "a session that cannot be attached again is dropped, and the rest restored" do
      parent = self()
      db = start()
      {:ok, %Session{id: %Uuid{value: id}}} = SurrealDB.new_session(db)
      {:ok, _} = SurrealDB.let(db, "x", 1)
      :ok = SurrealDB.subscribe_status(db)

      # From now on the server refuses to attach.
      error = %{"code" => -32_000, "message" => "no", "kind" => "Internal"}

      :sys.replace_state(db, fn state ->
        responder = responder(parent, %{"attach" => {:error, error}})
        %{state | engine_opts: Keyword.put(state.engine_opts, :responder, responder)}
      end)

      drain()
      send(db, :mock_closed)
      assert_receive {:surrealdb, :status, :connected, _}, 5_000

      assert_received {:rpc, "attach", [], ^id, nil}
      assert :sys.get_state(db).sessions |> Map.keys() == [nil]
    end

    test "sessions returns UUIDs over JSON too" do
      id = "0190d6f2-8b1e-7000-8000-0000000000cc"

      db =
        start(codec: :json, responder: responder(self(), %{"sessions" => {:ok, [Uuid.new(id)]}}))

      assert {:ok, [%Uuid{value: ^id}]} = SurrealDB.sessions(db)
    end

    test "forking a closed session fails without attaching a new one" do
      db = start()
      {:ok, session} = SurrealDB.new_session(db)
      {:ok, _} = SurrealDB.close_session(session)
      drain()

      assert {:error, %Error{kind: :connection}} = SurrealDB.fork_session(session)
      refute_received {:rpc, "attach", _, _, _}
    end

    test "a fork interrupted by a reconnect can still sign in again on its own" do
      parent = self()
      base = responder(parent)
      expired = %{"code" => -32_002, "message" => "Token expired", "kind" => "NotAllowed"}

      responder = fn
        # The fork's `use`, just after it authenticated: drop the connection.
        "use", params, %{"session" => sid} = request when not is_nil(sid) ->
          if Process.put(:dropped, true) do
            base.("use", params, request)
          else
            send(self(), :mock_closed)
            :noreply
          end

        "authenticate", params, request ->
          if Process.get(:dropped),
            do: tap({:error, expired}, fn _ -> base.("authenticate", params, request) end),
            else: base.("authenticate", params, request)

        method, params, request ->
          base.(method, params, request)
      end

      auth = %SurrealDB.Auth.Root{user: "root", pass: "root"}
      db = start(responder: responder, auth: auth, namespace: "ns", database: "db")
      drain()

      assert {:error, _} = SurrealDB.fork_session(db)
      assert_receive {:rpc, "attach", [], fork, nil}, 5_000
      assert_receive {:rpc, "signin", [_credentials], ^fork, nil}, 5_000
    end

    test "a session whose restore times out is kept, and the reconnect tried again" do
      parent = self()
      base = responder(parent)

      responder = fn
        # The session's `use` while restoring: answer it only on the second attempt.
        "use", params, %{"session" => sid} = request when not is_nil(sid) ->
          n = Process.put(:session_uses, (Process.get(:session_uses) || 0) + 1) || 0
          if n == 1, do: :noreply, else: base.("use", params, request)

        method, params, request ->
          base.(method, params, request)
      end

      db = start(responder: responder, connect_timeout: 200)
      {:ok, %Session{id: %Uuid{value: id}} = session} = SurrealDB.new_session(db)
      {:ok, _} = SurrealDB.use(session, namespace: "ns", database: "db")
      :ok = SurrealDB.subscribe_status(db)
      send(db, :mock_closed)

      assert_receive {:surrealdb, :status, :reconnecting,
                      %{attempt: 2, error: %{kind: :timeout}}},
                     5_000

      assert_receive {:surrealdb, :status, :connected, %{attempt: 2}}, 5_000
      assert Map.has_key?(:sys.get_state(db).sessions, id)
    end

    test "a session attached again but not restored is detached" do
      parent = self()
      db = start()
      {:ok, %Session{id: %Uuid{value: id}} = session} = SurrealDB.new_session(db)
      {:ok, _} = SurrealDB.use(session, namespace: "ns", database: "gone")
      :ok = SurrealDB.subscribe_status(db)

      # From now on the server refuses the session's database.
      error = %{"code" => -32_000, "message" => "no database", "kind" => "NotFound"}

      :sys.replace_state(db, fn state ->
        responder = responder(parent, %{"use" => {:error, error}})
        %{state | engine_opts: Keyword.put(state.engine_opts, :responder, responder)}
      end)

      drain()
      send(db, :mock_closed)
      assert_receive {:surrealdb, :status, :connected, _}, 5_000

      assert_received {:rpc, "attach", [], ^id, nil}
      assert_receive {:rpc, "detach", [], ^id, nil}
      assert :sys.get_state(db).sessions |> Map.keys() == [nil]
    end

    test "a live query whose subscribers have all exited is killed in its session" do
      live = Uuid.new("0190d6f2-8b1e-7000-8000-0000000000bb")
      db = start(responder: responder(self(), %{"live" => {:ok, live}}))
      {:ok, %Session{id: %Uuid{value: id}} = session} = SurrealDB.new_session(db)
      drain()

      {:ok, ^live} = Task.async(fn -> SurrealDB.live(session, "person") end) |> Task.await()
      assert_received {:rpc, "live", _, ^id, nil}
      assert_receive {:rpc, "kill", [^live], ^id, nil}
    end
  end

  # Answers the one request still pending, as a server would after its caller timed out.
  defp late_reply(db, method, result) do
    [id] = for {id, {nil, ^method, _, _}} <- :sys.get_state(db).pending, do: id
    payload = SurrealDB.Codec.CBOR.encode(%{"id" => id, "result" => result})
    send(db, {:mock_message, payload})
  end

  describe "late replies" do
    test "a session that attaches after its call timed out is detached again" do
      db = start(responder: responder(self(), %{"attach" => :noreply}), timeout: 50)
      drain()

      assert {:error, %Error{kind: :timeout}} = SurrealDB.new_session(db)
      assert_received {:rpc, "attach", [], sid, nil}

      late_reply(db, "attach", nil)

      assert_receive {:rpc, "detach", [], ^sid, nil}
      assert Map.keys(:sys.get_state(db).sessions) == [nil]
    end

    test "a transaction that begins after its call timed out is cancelled" do
      db = start(responder: responder(self(), %{"begin" => :noreply}), timeout: 50)
      drain()

      assert {:error, %Error{kind: :timeout}} = SurrealDB.begin(db)
      late_reply(db, "begin", Uuid.new(@txn))

      assert_receive {:rpc, "cancel", [%Uuid{value: @txn}], nil, nil}
    end
  end

  describe "transactions" do
    test "calls in a transaction carry its id; commit names it in the params" do
      db = start()
      drain()

      assert {:ok, %Transaction{id: %Uuid{value: @txn}, session: nil} = txn} =
               SurrealDB.begin(db)

      assert_received {:rpc, "begin", [], nil, nil}

      {:ok, _} = SurrealDB.create(txn, "person", %{})
      assert_received {:rpc, "create", _, nil, @txn}

      {:ok, _} = SurrealDB.commit(txn)
      assert_received {:rpc, "commit", [%Uuid{value: @txn}], nil, nil}
    end

    test "a transaction begun on a session runs in that session" do
      db = start()
      {:ok, %Session{id: %Uuid{value: id}} = session} = SurrealDB.new_session(db)
      drain()

      {:ok, txn} = SurrealDB.begin(session)
      assert_received {:rpc, "begin", [], ^id, nil}

      {:ok, _} = SurrealDB.select(txn, "person")
      assert_received {:rpc, "select", _, ^id, @txn}

      {:ok, _} = SurrealDB.cancel(txn)
      assert_received {:rpc, "cancel", [%Uuid{value: @txn}], ^id, nil}
    end

    test "transaction/2 commits when the function succeeds" do
      db = start()

      assert {:ok, :done} = SurrealDB.transaction(db, fn _txn -> :done end)
      assert_received {:rpc, "commit", _, _, _}

      assert {:ok, 1} = SurrealDB.transaction(db, fn _txn -> {:ok, 1} end)
    end

    test "transaction/2 cancels when the function returns an error" do
      db = start()

      assert {:error, :nope} = SurrealDB.transaction(db, fn _txn -> {:error, :nope} end)
      assert_received {:rpc, "cancel", _, _, _}
      refute_received {:rpc, "commit", _, _, _}
    end

    test "transaction/2 returns the function's error when the cancel cannot be sent" do
      db = start()

      result =
        SurrealDB.transaction(db, fn _txn ->
          stop_supervised!(Connection)
          {:error, :nope}
        end)

      assert result == {:error, :nope}
    end

    test "transaction/2 cancels and re-raises when the function raises" do
      db = start()

      assert_raise RuntimeError, "boom", fn ->
        SurrealDB.transaction(db, fn _txn -> raise "boom" end)
      end

      assert_received {:rpc, "cancel", _, _, _}
    end

    test "transaction/2 returns the error of a failed commit" do
      error = %{"code" => -32_009, "message" => "conflict", "kind" => "Query"}
      db = start(responder: responder(self(), %{"commit" => {:error, error}}))

      assert {:error, %Error{} = error} = SurrealDB.transaction(db, fn _txn -> :ok end)
      assert Error.transaction_conflict?(error)
    end

    test "a transaction refuses calls it cannot run" do
      db = start()
      {:ok, txn} = SurrealDB.begin(db)
      drain()

      for call <- [
            fn -> SurrealDB.use(txn, namespace: "ns", database: "db") end,
            fn -> SurrealDB.signin(txn, %SurrealDB.Auth.Root{user: "root", pass: "root"}) end,
            fn -> SurrealDB.let(txn, "x", 1) end,
            fn -> SurrealDB.live(txn, "person") end,
            fn -> SurrealDB.info(txn) end,
            fn -> SurrealDB.version(txn) end,
            fn -> SurrealDB.export(txn) end,
            fn -> SurrealDB.import(txn, "CREATE person") end
          ] do
        assert {:error, %Error{kind: :unsupported}} = call.()
      end

      refute_received {:rpc, _, _, _, _}

      {:ok, _} = SurrealDB.query(txn, "RETURN 1")
      assert_received {:rpc, "query", _, nil, @txn}
    end

    test "transaction/2 re-raises the callback's error even if cancelling fails" do
      Process.flag(:trap_exit, true)
      db = start()

      assert_raise RuntimeError, "boom", fn ->
        SurrealDB.transaction(db, fn _txn ->
          GenServer.stop(db)
          raise "boom"
        end)
      end
    end

    test "a session cannot be started or forked from a transaction" do
      db = start()
      {:ok, txn} = SurrealDB.begin(db)
      drain()

      assert {:error, %Error{kind: :unsupported}} = SurrealDB.new_session(txn)
      assert {:error, %Error{kind: :unsupported}} = SurrealDB.fork_session(txn)
      refute_received {:rpc, "attach", _, _, _}
    end

    test "transactions cannot be nested" do
      db = start()
      {:ok, txn} = SurrealDB.begin(db)
      assert {:error, %Error{kind: :unsupported}} = SurrealDB.begin(txn)
    end
  end
end
