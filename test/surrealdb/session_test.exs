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
      assert_receive {:surrealdb, :status, :connected, _}, 1_000

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
      assert_receive {:surrealdb, :status, :connected, _}, 1_000

      assert_received {:rpc, "attach", [], ^id, nil}
      assert :sys.get_state(db).sessions |> Map.keys() == [nil]
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

    test "transactions cannot be nested" do
      db = start()
      {:ok, txn} = SurrealDB.begin(db)
      assert {:error, %Error{kind: :unsupported}} = SurrealDB.begin(txn)
    end
  end
end
