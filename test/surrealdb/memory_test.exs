defmodule SurrealDB.MemoryTest do
  use ExUnit.Case, async: true

  import Plug.Conn

  alias SurrealDB.Memory

  setup do
    bypass = Bypass.open()

    client =
      Memory.new(
        endpoint: "http://localhost:#{bypass.port}",
        context: "acme",
        api_key: "secret",
        retry: false
      )

    {:ok, bypass: bypass, client: client}
  end

  defp read_json(conn) do
    {:ok, body, conn} = read_body(conn)
    {Jason.decode!(body), conn}
  end

  test "remember posts text with auth and idempotency headers", %{bypass: bypass, client: client} do
    Bypass.expect_once(bypass, "POST", "/contexts/acme/remember", fn conn ->
      assert get_req_header(conn, "authorization") == ["Bearer secret"]
      assert [_key] = get_req_header(conn, "idempotency-key")
      {body, conn} = read_json(conn)
      assert body == %{"text" => "I got promoted", "scopes" => "user/tobie"}
      resp(conn, 200, Jason.encode!(%{"facts" => 1}))
    end)

    assert {:ok, %{"facts" => 1}} =
             Memory.remember(client, "I got promoted", scopes: "user/tobie")
  end

  test "recall posts the query and options", %{bypass: bypass, client: client} do
    Bypass.expect_once(bypass, "POST", "/contexts/acme/recall", fn conn ->
      {body, conn} = read_json(conn)
      assert body == %{"query" => "role?", "k" => 10}
      resp(conn, 200, Jason.encode!(%{"hits" => []}))
    end)

    assert {:ok, %{"hits" => []}} = Memory.recall(client, "role?", k: 10)
  end

  test "audit sends a GET with query params and no idempotency key", %{
    bypass: bypass,
    client: client
  } do
    Bypass.expect_once(bypass, "GET", "/contexts/acme/audit", fn conn ->
      conn = fetch_query_params(conn)
      assert conn.query_params["limit"] == "50"
      assert get_req_header(conn, "idempotency-key") == []
      resp(conn, 200, Jason.encode!(%{"entries" => []}))
    end)

    assert {:ok, %{"entries" => []}} = Memory.audit(client, limit: 50)
  end

  test "whoami is not context scoped", %{bypass: bypass, client: client} do
    Bypass.expect_once(bypass, "GET", "/whoami", fn conn ->
      resp(conn, 200, Jason.encode!(%{"principal" => "principal:me"}))
    end)

    assert {:ok, %{"principal" => "principal:me"}} = Memory.whoami(client)
  end

  test "on_behalf_of adds the delegation header", %{bypass: bypass, client: client} do
    Bypass.expect_once(bypass, "POST", "/contexts/acme/remember", fn conn ->
      assert get_req_header(conn, "x-spectron-on-behalf-of") == ["principal:alex"]
      resp(conn, 200, Jason.encode!(%{}))
    end)

    delegated = Memory.on_behalf_of(client, "principal:alex")
    assert {:ok, _} = Memory.remember(delegated, "reviewed the plan")
    assert client.on_behalf_of == nil
  end

  describe "error mapping" do
    test "404 maps to not_found", %{bypass: bypass, client: client} do
      Bypass.expect_once(bypass, "GET", "/contexts/acme/state", fn conn ->
        conn
        |> put_resp_header("x-spectron-trace-id", "trace-123")
        |> resp(404, Jason.encode!(%{"message" => "missing"}))
      end)

      assert {:error, error} = Memory.state(client)
      assert error.kind == :not_found
      assert error.status == 404
      assert error.message == "missing"
      assert error.trace_id == "trace-123"
    end

    test "429 maps to rate_limit", %{bypass: bypass, client: client} do
      Bypass.expect(bypass, "POST", "/contexts/acme/reflect", fn conn ->
        resp(conn, 429, Jason.encode!(%{"error" => "slow down"}))
      end)

      assert {:error, %{kind: :rate_limit, status: 429}} =
               Memory.reflect(client, "what changed?")
    end

    test "401 maps to auth", %{bypass: bypass, client: client} do
      Bypass.expect_once(bypass, "GET", "/contexts/acme/profile", fn conn ->
        resp(conn, 401, Jason.encode!(%{"message" => "bad key"}))
      end)

      assert {:error, %{kind: :auth}} = Memory.profile(client)
    end
  end

  describe "namespaces" do
    test "Documents.list", %{bypass: bypass, client: client} do
      Bypass.expect_once(bypass, "GET", "/contexts/acme/documents", fn conn ->
        resp(conn, 200, Jason.encode!(%{"documents" => []}))
      end)

      assert {:ok, %{"documents" => []}} = Memory.Documents.list(client)
    end

    test "Keys.create", %{bypass: bypass, client: client} do
      Bypass.expect_once(bypass, "POST", "/contexts/acme/keys", fn conn ->
        {body, conn} = read_json(conn)
        assert body == %{"name" => "ci", "ttl_seconds" => 3600}
        resp(conn, 200, Jason.encode!(%{"secret" => "sk-..."}))
      end)

      assert {:ok, %{"secret" => "sk-..."}} =
               Memory.Keys.create(client, name: "ci", ttl_seconds: 3600)
    end

    test "Sessions.create returns a Session handle", %{bypass: bypass, client: client} do
      Bypass.expect_once(bypass, "POST", "/contexts/acme/sessions", fn conn ->
        resp(conn, 200, Jason.encode!(%{"id" => "session:1"}))
      end)

      assert {:ok, %Memory.Session{id: "session:1"}} = Memory.Sessions.create(client)
    end

    test "Session.turns posts to the session path", %{bypass: bypass, client: client} do
      session = %Memory.Session{client: client, id: "session:1"}

      Bypass.expect_once(bypass, "POST", "/contexts/acme/sessions/session%3A1/turns", fn conn ->
        {body, conn} = read_json(conn)
        assert body == %{"turns" => [%{"role" => "user", "content" => "hi"}]}
        resp(conn, 200, Jason.encode!(%{"ok" => true}))
      end)

      assert {:ok, _} = Memory.Session.turns(session, [%{"role" => "user", "content" => "hi"}])
    end
  end
end
