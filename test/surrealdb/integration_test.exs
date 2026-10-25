defmodule SurrealDB.IntegrationTest do
  @moduledoc """
  End-to-end tests against a real SurrealDB server. Excluded by default.

  Start a server and run them with:

      docker run --rm -p 8000:8000 surrealdb/surrealdb:v3 \
        start --user root --pass root memory
      mix test --include integration

  CI runs them against the latest 3.x release and the nightly build.

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

    defp proxied_ws(proxy_url, opts) do
      base = [
        url: proxy_url,
        namespace: "test",
        database: Process.get(:it_database),
        auth: %SurrealDB.Auth.Root{user: "root", pass: "root"},
        reconnect: [delay: 50, max_delay: 200]
      ]

      start_supervised!({SurrealDB.Connection, Keyword.merge(base, opts)}, id: :proxied)
    end

    @tag :capture_log
    test "a dropped WebSocket stops the connection when reconnect is off" do
      Process.flag(:trap_exit, true)
      {proxy, url} = proxied("ws")

      {:ok, db} =
        SurrealDB.Connection.start_link(
          url: url,
          namespace: "test",
          database: Process.get(:it_database),
          auth: %SurrealDB.Auth.Root{user: "root", pass: "root"},
          reconnect: false
        )

      ref = Process.monitor(db)
      assert {:ok, _} = SurrealDB.version(db)

      task = Task.async(fn -> SurrealDB.query(db, "SLEEP 2s; RETURN 1") end)
      Process.sleep(100)
      TCPProxy.cut(proxy)

      assert {:error, %SurrealDB.Error{kind: :connection}} = Task.await(task, 1_000)
      assert_receive {:DOWN, ^ref, :process, ^db, {:shutdown, {:connection_lost, _}}}, 1_000
    end

    @tag :capture_log
    test "a dropped WebSocket reconnects and restores the session and live queries" do
      {proxy, url} = proxied("ws")
      db = proxied_ws(url, [])

      {:ok, _} = SurrealDB.query(db, "DEFINE TABLE watched")
      {:ok, _} = SurrealDB.let(db, "greeting", "hello")
      {:ok, live_id} = SurrealDB.live(db, "watched")
      :ok = SurrealDB.subscribe_status(db)

      TCPProxy.cut(proxy)
      assert_receive {:surrealdb, :status, :reconnecting, _}, 1_000
      assert_receive {:surrealdb, :status, :connected, _}, 5_000

      # Namespace, database, auth, and parameters are back.
      assert {:ok, ["hello"]} = SurrealDB.query(db, "RETURN $greeting")
      assert {:ok, [[]]} = SurrealDB.query(db, "SELECT * FROM watched")

      # The live query was started again and still reports under the original id.
      {:ok, _} = SurrealDB.create(db, "watched", %{"value" => 1})
      assert_receive {:surrealdb, :live, ^live_id, %{action: :create}}, 2_000

      assert {:ok, _} = SurrealDB.kill(db, live_id)
      assert TCPProxy.accepted(proxy) == 2
    end

    @tag :capture_log
    test "a reconnect restores attached sessions with their state" do
      {proxy, url} = proxied("ws")
      db = proxied_ws(url, [])
      other = "#{Process.get(:it_database)}_s"

      {:ok, session} = SurrealDB.new_session(db)
      {:ok, _} = SurrealDB.signin(session, %SurrealDB.Auth.Root{user: "root", pass: "root"})
      {:ok, _} = SurrealDB.use(session, namespace: "test", database: other)
      {:ok, _} = SurrealDB.let(session, "who", "session")
      :ok = SurrealDB.subscribe_status(db)

      TCPProxy.cut(proxy)
      assert_receive {:surrealdb, :status, :connected, _}, 5_000

      assert {:ok, [["session", ^other]]} =
               SurrealDB.query(session, "RETURN [$who, session::db()]")

      assert {:ok, ids} = SurrealDB.sessions(db)
      assert session.id in ids
    end

    @tag :capture_log
    test "requests made during an outage are sent after reconnecting" do
      {proxy, url} = proxied("ws")
      db = proxied_ws(url, [])
      :ok = SurrealDB.subscribe_status(db)

      TCPProxy.pause(proxy)
      assert_receive {:surrealdb, :status, :reconnecting, _}, 1_000
      task = Task.async(fn -> SurrealDB.query(db, "RETURN 42") end)
      Process.sleep(200)
      TCPProxy.resume(proxy)

      assert {:ok, [42]} = Task.await(task, 5_000)
      assert_received {:surrealdb, :status, :connected, _}
    end

    @tag :capture_log
    test "gives up and stops after the last attempt" do
      Process.flag(:trap_exit, true)
      {proxy, url} = proxied("ws")
      db = proxied_ws(url, reconnect: [attempts: 2, delay: 20, max_delay: 40])
      ref = Process.monitor(db)
      :ok = SurrealDB.subscribe_status(db)

      TCPProxy.pause(proxy)
      assert_receive {:surrealdb, :status, :disconnected, _}, 2_000
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

  test "an HTTP error status fails the call at once instead of timing out" do
    http_url = @url |> String.replace(~r/^ws/, "http") |> String.replace(~r{/rpc$}, "/missing")
    db = start_supervised!({SurrealDB.Connection, url: http_url, version_check: false})

    {micros, result} = :timer.tc(fn -> SurrealDB.version(db) end)
    assert {:error, %SurrealDB.Error{kind: :http, code: 404}} = result
    assert micros < 5_000_000
  end

  describe "record access with refresh tokens" do
    defp define_record_access(token_duration \\ "1h") do
      db = connect()

      {:ok, _} =
        SurrealDB.query(db, """
        DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select WHERE id = $auth.id;
        DEFINE ACCESS account ON DATABASE TYPE RECORD
          SIGNUP (CREATE user SET email = $email, pass = crypto::argon2::generate($pass))
          SIGNIN (SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass))
          WITH REFRESH
          DURATION FOR TOKEN #{token_duration}, FOR SESSION 1d;
        """)

      %SurrealDB.Auth.Record{
        namespace: "test",
        database: Process.get(:it_database),
        access: "account",
        variables: %{email: "a@example.com", pass: "secret"}
      }
    end

    for scheme <- ["ws", "http"] do
      test "signup and signin keep the access token over #{scheme}" do
        auth = define_record_access()
        url = String.replace(@url, ~r/^ws/, unquote(scheme))

        db = start_supervised!({SurrealDB.Connection, url: url}, id: :record_user)

        assert {:ok, %{"access" => access, "refresh" => refresh}} = SurrealDB.signup(db, auth)
        assert is_binary(access) and is_binary(refresh)
        assert SurrealDB.info_session(db).token == access
        assert {:ok, %{"email" => "a@example.com"}} = SurrealDB.info(db)

        assert {:ok, %{"access" => access}} = SurrealDB.signin(db, auth)
        assert SurrealDB.info_session(db).token == access
        assert {:ok, %{"email" => "a@example.com"}} = SurrealDB.info(db)
      end
    end

    test "refresh swaps the pair, and the old refresh token is spent" do
      auth = define_record_access()
      db = start_supervised!({SurrealDB.Connection, url: @url}, id: :record_user)

      {:ok, old_pair} = SurrealDB.signup(db, auth)
      assert {:ok, %{"access" => access, "refresh" => refresh} = new_pair} = SurrealDB.refresh(db)
      refute new_pair == old_pair
      assert %{token: ^access, refresh_token: ^refresh} = SurrealDB.info_session(db)
      assert {:ok, %{"email" => "a@example.com"}} = SurrealDB.info(db)

      assert {:error, %SurrealDB.Error{}} = SurrealDB.refresh(db, old_pair)
    end

    test "a token pair can authenticate another connection, and revoke ends refreshing" do
      auth = define_record_access()
      db = start_supervised!({SurrealDB.Connection, url: @url}, id: :record_user)
      {:ok, pair} = SurrealDB.signup(db, auth)

      other = start_supervised!({SurrealDB.Connection, url: @url}, id: :other_user)
      assert {:ok, _} = SurrealDB.authenticate(other, pair)
      assert {:ok, %{"email" => "a@example.com"}} = SurrealDB.info(other)

      assert {:ok, _} = SurrealDB.revoke(other)
      assert SurrealDB.info_session(other).refresh_token == nil
      assert {:error, %SurrealDB.Error{}} = SurrealDB.refresh(db)
    end

    @tag :capture_log
    test "the access token is renewed before it expires" do
      auth = define_record_access("4s")
      db = start_supervised!({SurrealDB.Connection, url: @url}, id: :record_user)

      {:ok, %{"access" => first}} = SurrealDB.signup(db, auth)

      # Renewed halfway through the 4 second lifetime, so still signed in after it.
      Process.sleep(5_000)
      refute SurrealDB.info_session(db).token == first
      assert {:ok, %{"email" => "a@example.com"}} = SurrealDB.info(db)
    end
  end

  describe "value types" do
    test "a record id range selects the records in it, and decodes from the server" do
      db = connect()
      {:ok, _} = SurrealDB.query(db, "CREATE t:1, t:2, t:3, t:4")

      range = SurrealDB.RecordIdRange.new("t", {:incl, 2}, {:excl, 4})
      assert {:ok, rows} = SurrealDB.select(db, range)
      assert Enum.map(rows, & &1["id"].id) == [2, 3]

      assert {:ok, [%SurrealDB.RecordIdRange{table: "t", begin: {:incl, 1}, end: {:incl, 5}}]} =
               SurrealDB.query(db, "RETURN t:1..=5")

      assert {:ok, _} = SurrealDB.delete(db, SurrealDB.RecordIdRange.new("t", nil, {:incl, 2}))

      assert {:ok, [[%{"id" => %SurrealDB.RecordId{id: 3}}, _]]} =
               SurrealDB.query(db, "SELECT id FROM t")
    end

    test "sets round-trip as MapSets" do
      db = connect()
      set = MapSet.new([3, 1, "a"])

      assert {:ok, [^set, "set", deduped]} =
               SurrealDB.query(db, "RETURN $s; RETURN type::of($s); RETURN <set> [2, 1, 1]", %{
                 s: set
               })

      assert deduped == MapSet.new([1, 2])
    end

    test "file references round-trip" do
      db = connect()
      file = SurrealDB.FileRef.new("avatars", "/tobie.png")

      assert {:ok, [^file, "file"]} =
               SurrealDB.query(db, "RETURN $f; RETURN type::of($f)", %{f: file})
    end

    test "generated UUIDs are accepted as UUIDs" do
      db = connect()

      for uuid <- [SurrealDB.Uuid.v4(), SurrealDB.Uuid.v7()] do
        assert {:ok, [^uuid, "uuid", true]} =
                 SurrealDB.query(
                   db,
                   "RETURN $u; RETURN type::of($u); RETURN $u = <uuid> $text",
                   %{u: uuid, text: uuid.value}
                 )
      end
    end
  end

  describe "export and import" do
    test "export picks what to include, and import restores it elsewhere" do
      db = connect()

      {:ok, _} =
        SurrealDB.query(
          db,
          "CREATE person:tobie SET name = 'Tobie'; CREATE secret:1; DEFINE PARAM $p VALUE 1"
        )

      assert {:ok, script} = SurrealDB.export(db, tables: ["person"], params: false)
      assert script =~ "OPTION IMPORT"
      assert script =~ "person:tobie"
      refute script =~ "secret"
      refute script =~ "DEFINE PARAM"

      assert {:ok, schema_only} = SurrealDB.export(db, records: false)
      refute schema_only =~ "person:tobie"

      other = "#{Process.get(:it_database)}_import"
      {:ok, _} = SurrealDB.query(db, "DEFINE DATABASE #{other}")

      auth = %SurrealDB.Auth.Root{user: "root", pass: "root"}
      opts = [url: @url, namespace: "test", database: other, auth: auth]
      target = start_supervised!({SurrealDB.Connection, opts}, id: :target)

      assert {:ok, :imported} = SurrealDB.import(target, script)

      assert {:ok, %{"name" => "Tobie"}} =
               SurrealDB.select(target, SurrealDB.RecordId.new("person", "tobie"))
    end

    test "an import reports the statements that failed and applies the rest" do
      db = connect()
      {:ok, _} = SurrealDB.query(db, "CREATE a:5")

      script = "OPTION IMPORT; CREATE a:1; THROW 'bad'; CREATE a:5; CREATE a:2;"

      assert {:error, %SurrealDB.Error{kind: :query, details: %{failures: failures}}} =
               SurrealDB.import(db, script)

      assert [
               %SurrealDB.Error{server_kind: :thrown},
               %SurrealDB.Error{server_kind: :already_exists}
             ] =
               failures

      assert {:ok, [[%{"id" => _}, %{"id" => _}, %{"id" => _}]]} =
               SurrealDB.query(db, "SELECT id FROM a")
    end

    test "an import without OPTION IMPORT says why it was refused" do
      db = connect()

      assert {:error, %SurrealDB.Error{kind: :http, code: 400, message: message}} =
               SurrealDB.import(db, "CREATE a:1")

      assert message =~ "OPTION IMPORT"
    end
  end

  describe "streaming queries, gql, and api" do
    test "query_stream delivers rows in batches and a failed statement's error" do
      db = connect()
      {:ok, _} = SurrealDB.query(db, "FOR $i IN 0..1000 { CREATE big SET n = $i }")

      frames =
        db
        |> SurrealDB.query_stream("RETURN 1; THROW 'x'; SELECT n FROM big ORDER BY n")
        |> Enum.to_list()

      assert {:begin, %{statements: 3}} = hd(frames)
      assert {:end, %{results: 3, error: nil}} = List.last(frames)
      assert {:value, 0, 1} in frames

      assert Enum.any?(frames, &match?({:finished, 1, %{error: %SurrealDB.Error{}}}, &1))

      rows = for {:rows, 2, rows} <- frames, row <- rows, do: row["n"]
      assert rows == Enum.to_list(0..999)
      assert Enum.count(frames, &match?({:rows, 2, _}, &1)) > 1
    end

    test "stopping a stream early cancels it and leaves the connection usable" do
      db = connect()
      {:ok, _} = SurrealDB.query(db, "FOR $i IN 0..2000 { CREATE big SET n = $i }")

      assert [{:begin, _}, {:rows, 0, _}] =
               db |> SurrealDB.query_stream("SELECT * FROM big") |> Enum.take(2)

      {:ok, ref} = SurrealDB.stream_query(db, "SLEEP 2s; RETURN 1")
      assert_receive {:surrealdb, :stream, ^ref, {:begin, _}}, 1_000
      assert {:ok, _} = SurrealDB.cancel_query(db, ref)
      refute_receive {:surrealdb, :stream, ^ref, _}, 300

      assert {:ok, [2]} = SurrealDB.query(db, "RETURN 2")
    end

    test "gql runs graph queries" do
      db = connect()

      {:ok, _} =
        SurrealDB.query(db, """
        CREATE person:a SET name = 'A'; CREATE person:b SET name = 'B';
        RELATE person:a->knows->person:b
        """)

      assert {:ok, [[%{"b.name" => "B"}]]} =
               SurrealDB.gql(db, "MATCH (a:person)-[:knows]->(b:person) RETURN b.name")
    end

    test "api calls a defined endpoint" do
      db = connect()

      {:ok, _} =
        SurrealDB.query(db, """
        DEFINE API "/echo" FOR any THEN {
          RETURN { status: 201, body: { got: $request.body, q: $request.query, m: $request.method } }
        }
        """)

      assert {:ok, %{status: 201, body: body}} =
               SurrealDB.api(db, "/echo", method: :post, body: %{x: 1}, query: %{"a" => "b"})

      assert body == %{"got" => %{"x" => 1}, "m" => "post", "q" => %{"a" => "b"}}
      assert {:ok, %{status: 404}} = SurrealDB.api(db, "/missing")
    end
  end

  test "strings are never turned into record ids" do
    db = connect()
    {:ok, _} = SurrealDB.create(db, SurrealDB.RecordId.new("user", 1234), %{"n" => 1})

    # "user:1234" is a table name here, and no such table exists.
    assert {:error, %SurrealDB.Error{message: "The table 'user:1234' does not exist"}} =
             SurrealDB.select(db, "user:1234")

    assert {:ok, %{"n" => 1}} = SurrealDB.select(db, SurrealDB.RecordId.parse!("user:1234"))
    assert {:ok, %{"n" => 1}} = SurrealDB.select(db, SurrealDB.StringRecordId.new("user:1234"))

    # A string value stays a string.
    assert {:ok, [created]} = SurrealDB.create(db, "note", %{"ref" => "user:1234"})
    assert created["ref"] == "user:1234"
  end

  test "a failed statement does not hide the results of the others" do
    db = connect()
    sql = "RETURN 1; THROW 'boom'; RETURN 3"

    assert {:error, %SurrealDB.Error{kind: :query, details: %{results: results}}} =
             SurrealDB.query(db, sql)

    assert [{:ok, 1}, {:error, %SurrealDB.Error{message: message}}, {:ok, 3}] = results
    assert message =~ "boom"

    assert {:ok, [{:ok, 1}, {:error, _}, {:ok, 3}]} = SurrealDB.query_results(db, sql)
    assert {:ok, [1, 3]} = SurrealDB.query(db, "RETURN 1; RETURN 3")
  end

  describe "live subscribers" do
    test "every subscriber gets notifications, and KILLED ends the live query" do
      db = connect()
      {:ok, _} = SurrealDB.query(db, "DEFINE TABLE watched")
      {:ok, live_id} = SurrealDB.live(db, "watched")

      parent = self()

      other =
        spawn(fn ->
          receive do
            message -> send(parent, {:other, message})
          end
        end)

      :ok = SurrealDB.subscribe_live(db, live_id, other)
      {:ok, _} = SurrealDB.create(db, "watched", %{"value" => 1})

      assert_receive {:surrealdb, :live, _, %{action: :create, record: %SurrealDB.RecordId{}}},
                     2_000

      assert_receive {:other, {:surrealdb, :live, _, %{action: :create}}}, 2_000

      {:ok, _} = SurrealDB.query(db, "REMOVE TABLE watched")
      assert_receive {:surrealdb, :live, _, %{action: :killed}}, 2_000
      assert :sys.get_state(db).live == %{}
    end

    test "selecting a record with an action field returns it" do
      db = connect()
      id = SurrealDB.RecordId.new("log", 1)
      {:ok, _} = SurrealDB.create(db, id, %{"action" => "login"})
      assert {:ok, %{"action" => "login"}} = SurrealDB.select(db, id)
    end
  end

  describe "structured errors" do
    test "RPC errors carry the server's kind and details" do
      db = connect()

      assert {:error, %SurrealDB.Error{kind: :rpc, server_kind: :not_found} = error} =
               SurrealDB.select(db, "missing_table")

      assert SurrealDB.Error.detail_kinds(error) == ["Table"]
      assert SurrealDB.Error.detail_fields(error) == %{"name" => "missing_table"}
    end

    test "statement errors carry the server's kind" do
      db = connect()

      assert {:ok, [{:error, thrown}, {:error, missing}]} =
               SurrealDB.query_results(db, "THROW 'x'; SELECT * FROM missing_table")

      assert thrown.server_kind == :thrown
      assert missing.server_kind == :not_found
      assert SurrealDB.Error.detail_kinds(missing) == ["Table"]
    end

    test "a failed signin is not allowed" do
      db = start_supervised!({SurrealDB.Connection, url: @url})

      assert {:error, %SurrealDB.Error{server_kind: :not_allowed}} =
               SurrealDB.signin(db, %SurrealDB.Auth.Root{user: "root", pass: "wrong"})
    end
  end

  describe "version, health, reset, and timeouts" do
    test "the version check accepts the server and records its version" do
      db = connect()
      assert "surrealdb-" <> _ = SurrealDB.info_session(db).version
    end

    test "health is ok over both engines" do
      assert :ok = SurrealDB.health(connect())

      http = String.replace(@url, ~r/^ws/, "http")

      assert :ok =
               SurrealDB.health(start_supervised!({SurrealDB.Connection, url: http}, id: :http))
    end

    test "reset clears the namespace, database, and auth on the server" do
      db = connect()
      assert {:ok, _} = SurrealDB.reset(db)

      # Signed out: the server refuses anonymous queries.
      assert {:error, %SurrealDB.Error{server_kind: :not_allowed}} =
               SurrealDB.query(db, "RETURN 1")

      # Signed back in, the namespace and database are gone too.
      {:ok, _} = SurrealDB.signin(db, %SurrealDB.Auth.Root{user: "root", pass: "root"})
      assert {:ok, [[ns, db_name]]} = SurrealDB.query(db, "RETURN [session::ns(), session::db()]")
      assert ns in [nil, %SurrealDB.None{}]
      assert db_name in [nil, %SurrealDB.None{}]
    end

    test "a query times out without breaking the connection" do
      db = connect()

      assert {:error, %SurrealDB.Error{kind: :timeout}} =
               SurrealDB.query(db, "SLEEP 500ms; RETURN 1", %{}, timeout: 50)

      assert {:ok, [2]} = SurrealDB.query(db, "RETURN 2")
      Process.sleep(600)
      assert {:ok, [3]} = SurrealDB.query(db, "RETURN 3")
    end
  end

  describe "sessions" do
    test "a new session has its own namespace, database, auth, and parameters" do
      db = connect()
      {:ok, _} = SurrealDB.let(db, "who", "default")

      assert {:ok, session} = SurrealDB.new_session(db)

      # A new session starts signed out with nothing selected.
      assert {:error, %SurrealDB.Error{}} = SurrealDB.query(session, "SELECT * FROM person")

      {:ok, _} = SurrealDB.signin(session, %SurrealDB.Auth.Root{user: "root", pass: "root"})
      other = "#{Process.get(:it_database)}_other"
      {:ok, _} = SurrealDB.use(session, namespace: "test", database: other)
      {:ok, _} = SurrealDB.let(session, "who", "session")

      assert {:ok, [["session", ^other]]} =
               SurrealDB.query(session, "RETURN [$who, session::db()]")

      database = Process.get(:it_database)

      assert {:ok, [["default", ^database]]} =
               SurrealDB.query(db, "RETURN [$who, session::db()]")

      assert {:ok, ids} = SurrealDB.sessions(db)
      assert session.id in ids

      assert {:ok, _} = SurrealDB.close_session(session)
      assert {:ok, ids} = SurrealDB.sessions(db)
      refute session.id in ids
    end

    test "a forked session starts with a copy of the source session" do
      db = connect()
      {:ok, _} = SurrealDB.let(db, "who", "default")

      assert {:ok, fork} = SurrealDB.fork_session(db)
      database = Process.get(:it_database)

      assert {:ok, [["default", ^database]]} =
               SurrealDB.query(fork, "RETURN [$who, session::db()]")

      {:ok, _} = SurrealDB.let(fork, "who", "fork")
      assert {:ok, ["default"]} = SurrealDB.query(db, "RETURN $who")
    end

    test "live queries started in a session are delivered" do
      db = connect()
      {:ok, _} = SurrealDB.query(db, "DEFINE TABLE watched")
      {:ok, session} = SurrealDB.fork_session(db)

      {:ok, live_id} = SurrealDB.live(session, "watched")
      {:ok, _} = SurrealDB.create(db, "watched", %{"value" => 1})
      assert_receive {:surrealdb, :live, ^live_id, %{action: :create}}, 2_000

      assert {:ok, _} = SurrealDB.kill(session, live_id)
    end
  end

  describe "transactions" do
    test "writes are visible only after commit" do
      db = connect()
      {:ok, _} = SurrealDB.query(db, "DEFINE TABLE account")

      {:ok, txn} = SurrealDB.begin(db)
      {:ok, _} = SurrealDB.create(txn, SurrealDB.RecordId.new("account", "a"), %{balance: 1})

      assert {:ok, [[%{"balance" => 1}]]} = SurrealDB.query(txn, "SELECT balance FROM account")
      assert {:ok, [[]]} = SurrealDB.query(db, "SELECT * FROM account")

      assert {:ok, _} = SurrealDB.commit(txn)
      assert {:ok, [[%{"balance" => 1}]]} = SurrealDB.query(db, "SELECT balance FROM account")
    end

    test "cancel discards the writes" do
      db = connect()
      {:ok, _} = SurrealDB.query(db, "DEFINE TABLE account")

      {:ok, txn} = SurrealDB.begin(db)
      {:ok, _} = SurrealDB.create(txn, "account", %{balance: 1})
      assert {:ok, _} = SurrealDB.cancel(txn)

      assert {:ok, [[]]} = SurrealDB.query(db, "SELECT * FROM account")
      assert {:error, %SurrealDB.Error{}} = SurrealDB.commit(txn)
    end

    test "transaction/2 commits on success and cancels on error" do
      db = connect()
      {:ok, _} = SurrealDB.query(db, "DEFINE TABLE account")

      assert {:ok, :done} =
               SurrealDB.transaction(db, fn txn ->
                 {:ok, _} = SurrealDB.create(txn, SurrealDB.RecordId.new("account", "a"), %{})
                 :done
               end)

      assert {:error, :nope} =
               SurrealDB.transaction(db, fn txn ->
                 {:ok, _} = SurrealDB.create(txn, SurrealDB.RecordId.new("account", "b"), %{})
                 {:error, :nope}
               end)

      assert {:ok, [[%{"id" => %SurrealDB.RecordId{id: "a"}}]]} =
               SurrealDB.query(db, "SELECT id FROM account")
    end

    test "a transaction in a session runs with that session's selection" do
      db = connect()
      {:ok, session} = SurrealDB.fork_session(db)
      other = "#{Process.get(:it_database)}_txn"
      {:ok, _} = SurrealDB.use(session, database: other)

      assert {:ok, [^other]} =
               SurrealDB.transaction(session, fn txn ->
                 SurrealDB.query(txn, "RETURN session::db()")
               end)
    end
  end

  test "infinities and NaN round-trip through the server" do
    db = connect()

    assert {:ok, [[:infinity, :neg_infinity, :nan]]} =
             SurrealDB.query(db, "RETURN [math::inf, -math::inf, NaN]")

    assert {:ok, [[true, true, true]]} =
             SurrealDB.query(
               db,
               "RETURN [$a = math::inf, $b = -math::inf, type::is_float($c)]",
               %{a: :infinity, b: :neg_infinity, c: :nan}
             )
  end

  test "datetimes before 1970 round-trip through the server" do
    db = connect()
    {:ok, dt, _} = DateTime.from_iso8601("1969-12-31T23:59:59.250000Z")
    datetime = SurrealDB.Datetime.from_datetime(dt)

    assert {:ok, [true]} =
             SurrealDB.query(db, "RETURN $d = d'1969-12-31T23:59:59.25Z'", %{d: datetime})

    assert {:ok, [^datetime]} = SurrealDB.query(db, "RETURN d'1969-12-31T23:59:59.25Z'")
  end

  test "RecordId.to_string matches the server's rendering and parses back" do
    db = connect()

    ids = [
      SurrealDB.RecordId.new("person", "tobie"),
      SurrealDB.RecordId.new("person", "complex id"),
      SurrealDB.RecordId.new("person", "123"),
      SurrealDB.RecordId.new("person", "a`b\\c"),
      SurrealDB.RecordId.new("select", 42),
      SurrealDB.RecordId.new("temp", ["london", 2024, 1.5]),
      SurrealDB.RecordId.new("temp", %{"city" => "london", "at" => Decimal.new("1.50")}),
      SurrealDB.RecordId.new("temp", SurrealDB.Uuid.new("0190d6f2-8b1e-7000-8000-000000000000"))
    ]

    for id <- ids do
      string = SurrealDB.RecordId.to_string(id)
      assert {:ok, [^string]} = SurrealDB.query(db, "RETURN <string> $id", %{id: id})
      assert {:ok, [true]} = SurrealDB.query(db, "RETURN #{string} = $id", %{id: id})
    end
  end

  test "works over the HTTP engine too" do
    http_url = String.replace(@url, ~r/^ws/, "http")
    db = connect(url: http_url)
    assert {:ok, version} = SurrealDB.version(db)
    assert is_binary(version)
  end
end
