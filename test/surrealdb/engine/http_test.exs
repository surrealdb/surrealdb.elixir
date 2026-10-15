defmodule SurrealDB.Engine.HTTPTest do
  use ExUnit.Case, async: true

  alias SurrealDB.Codec.CBOR

  setup do
    bypass = Bypass.open()
    {:ok, bypass: bypass, url: "http://localhost:#{bypass.port}/rpc"}
  end

  defp start(url), do: start_supervised!({SurrealDB.Connection, url: url, version_check: false})

  defp reply(conn, status, content_type, body) do
    conn
    |> Plug.Conn.put_resp_content_type(content_type, nil)
    |> Plug.Conn.resp(status, body)
  end

  defp rpc_id(conn) do
    {:ok, body, conn} = Plug.Conn.read_body(conn)
    {:ok, %{"id" => id}} = CBOR.decode(body)
    {id, conn}
  end

  defp timed(fun) do
    {micros, result} = :timer.tc(fun)
    assert micros < 5_000_000, "call took #{div(micros, 1000)}ms"
    result
  end

  test "a 401 with a text body fails the call at once", %{bypass: bypass, url: url} do
    Bypass.stub(bypass, "POST", "/rpc", fn conn ->
      reply(conn, 401, "text/plain", "There was a problem with authentication")
    end)

    db = start(url)

    assert {:error, %SurrealDB.Error{kind: :auth, code: 401, message: message}} =
             timed(fn -> SurrealDB.version(db) end)

    assert message =~ "There was a problem with authentication"
  end

  test "a 415 with a JSON body fails the call at once", %{bypass: bypass, url: url} do
    body = ~s({"code":415,"details":"Unsupported media type","description":"Bad type."})
    Bypass.stub(bypass, "POST", "/rpc", fn conn -> reply(conn, 415, "application/json", body) end)

    db = start(url)

    assert {:error, %SurrealDB.Error{kind: :http, code: 415, message: "HTTP 415: Bad type."}} =
             timed(fn -> SurrealDB.version(db) end)
  end

  test "a 500 with an empty body fails the call at once", %{bypass: bypass, url: url} do
    Bypass.stub(bypass, "POST", "/rpc", fn conn -> reply(conn, 500, "text/plain", "") end)
    db = start(url)

    assert {:error, %SurrealDB.Error{kind: :http, code: 500}} =
             timed(fn -> SurrealDB.version(db) end)
  end

  test "a 2xx body that is not CBOR fails the call at once", %{bypass: bypass, url: url} do
    Bypass.stub(bypass, "POST", "/rpc", fn conn ->
      reply(conn, 200, "application/cbor", <<0x82, 0x01>>)
    end)

    db = start(url)

    assert {:error, %SurrealDB.Error{kind: :decode}} = timed(fn -> SurrealDB.version(db) end)
  end

  test "an RPC error in a non-2xx CBOR body keeps its code and message", %{
    bypass: bypass,
    url: url
  } do
    Bypass.stub(bypass, "POST", "/rpc", fn conn ->
      {id, conn} = rpc_id(conn)
      error = %{"code" => -32_002, "message" => "Token expired"}
      reply(conn, 400, "application/cbor", CBOR.encode(%{"id" => id, "error" => error}))
    end)

    db = start(url)

    assert {:error, %SurrealDB.Error{code: -32_002, message: "Token expired"}} =
             timed(fn -> SurrealDB.version(db) end)
  end

  test "a 200 reply still works and the connection stays usable", %{bypass: bypass, url: url} do
    Bypass.stub(bypass, "POST", "/rpc", fn conn ->
      {id, conn} = rpc_id(conn)
      body = CBOR.encode(%{"id" => id, "result" => "surrealdb-3.0.0"})
      reply(conn, 200, "application/cbor", body)
    end)

    db = start(url)
    assert {:ok, "surrealdb-3.0.0"} = SurrealDB.version(db)
    assert {:ok, "surrealdb-3.0.0"} = SurrealDB.version(db)
  end
end
