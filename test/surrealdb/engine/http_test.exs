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

  test "a JSON error body with structured fields fails the call", %{bypass: bypass, url: url} do
    Bypass.stub(bypass, "POST", "/rpc", fn conn ->
      reply(conn, 403, "application/json", ~s({"details":{"reason":"denied"}}))
    end)

    db = start(url)
    assert {:error, %SurrealDB.Error{kind: :auth, code: 403}} = SurrealDB.version(db)
    assert Process.alive?(db)
  end

  test "a reply goes to the request it answers, whatever id its body has", %{
    bypass: bypass,
    url: url
  } do
    Bypass.stub(bypass, "POST", "/rpc", fn conn ->
      {_id, conn} = rpc_id(conn)
      body = CBOR.encode(%{"id" => 999_999, "result" => "surrealdb-3.0.0"})
      reply(conn, 200, "application/cbor", body)
    end)

    db = start(url)
    assert {:ok, "surrealdb-3.0.0"} = timed(fn -> SurrealDB.version(db) end)
  end

  # Bypass fails a test whose handler is killed by a client disconnect, which is what this
  # test causes, so it uses a bare TCP server: requests whose body mentions "hang" get no
  # answer, every other request is answered with its method name.
  defp hanging_server do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen)
    spawn_link(fn -> accept_loop(listen) end)
    "http://localhost:#{port}/rpc"
  end

  defp accept_loop(listen) do
    {:ok, socket} = :gen_tcp.accept(listen)
    pid = spawn(fn -> serve(socket) end)
    :gen_tcp.controlling_process(socket, pid)
    accept_loop(listen)
  end

  defp serve(socket) do
    with {:ok, data} <- :gen_tcp.recv(socket, 0) do
      [_head, body] = String.split(data, "\r\n\r\n", parts: 2)
      {:ok, %{"id" => id, "method" => method}} = CBOR.decode(body)

      unless method == "hang" do
        reply = CBOR.encode(%{"id" => id, "result" => method})

        :gen_tcp.send(socket, [
          "HTTP/1.1 200 OK\r\ncontent-type: application/cbor\r\ncontent-length: ",
          Integer.to_string(byte_size(reply)),
          "\r\n\r\n",
          reply
        ])
      end

      serve(socket)
    end
  end

  test "a timed-out request does not hold up the requests after it" do
    db = start(hanging_server())

    assert {:error, %SurrealDB.Error{kind: :timeout}} =
             SurrealDB.Connection.call(db, "hang", [], timeout: 100)

    assert {:ok, "version"} = timed(fn -> SurrealDB.version(db) end)
  end

  test "sessions and transactions are unsupported over HTTP", %{bypass: bypass, url: url} do
    Bypass.stub(bypass, "POST", "/rpc", fn conn ->
      {id, conn} = rpc_id(conn)
      body = CBOR.encode(%{"id" => id, "result" => "surrealdb-3.0.0"})
      reply(conn, 200, "application/cbor", body)
    end)

    db = start(url)
    assert {:error, %SurrealDB.Error{kind: :unsupported}} = SurrealDB.new_session(db)
    assert {:error, %SurrealDB.Error{kind: :unsupported}} = SurrealDB.begin(db)
    assert {:error, %SurrealDB.Error{kind: :unsupported}} = SurrealDB.sessions(db)
  end

  test "a request the server stops reading fails after the send timeout" do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen)

    spawn_link(fn ->
      {:ok, _socket} = :gen_tcp.accept(listen)
      Process.sleep(:infinity)
    end)

    uri = URI.parse("http://127.0.0.1:#{port}/rpc")
    {:ok, conn} = SurrealDB.Engine.HTTP.connect(uri, send_timeout: 200)

    # The runtime queues one large write without blocking; the next write then blocks
    # until the queue drains, which it never does.
    body = :binary.copy("x", 64 * 1024 * 1024)
    {:ok, conn} = SurrealDB.Engine.HTTP.request(conn, 1, body, %{})

    {micros, result} = :timer.tc(fn -> SurrealDB.Engine.HTTP.request(conn, 2, "{}", %{}) end)
    assert {:error, _reason} = result
    assert micros < 5_000_000
  end
end
