defmodule SurrealDB.Engine.WebSocketTest do
  use ExUnit.Case, async: true

  alias SurrealDB.Engine.WebSocket

  test "closes the socket when the server refuses the upgrade" do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen)
    parent = self()

    spawn_link(fn ->
      {:ok, socket} = :gen_tcp.accept(listen)
      {:ok, _request} = :gen_tcp.recv(socket, 0, 5_000)
      :ok = :gen_tcp.send(socket, "HTTP/1.1 403 Forbidden\r\ncontent-length: 0\r\n\r\n")
      send(parent, {:after_refusal, :gen_tcp.recv(socket, 0, 5_000)})
    end)

    uri = URI.parse("ws://127.0.0.1:#{port}/rpc")
    assert {:error, _reason} = WebSocket.connect(uri, [])
    assert_receive {:after_refusal, {:error, :closed}}, 5_000
  end

  describe "subprotocol negotiation" do
    # Accepts one upgrade, answering with `chosen` (or no subprotocol header), and reports
    # the subprotocols the client offered.
    defp upgrade_server(chosen) do
      {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
      {:ok, port} = :inet.port(listen)
      parent = self()

      spawn_link(fn ->
        {:ok, socket} = :gen_tcp.accept(listen)
        {:ok, request} = :gen_tcp.recv(socket, 0, 5_000)
        [_, key] = Regex.run(~r/sec-websocket-key: *([^\r]+)/i, request)
        [_, offered] = Regex.run(~r/sec-websocket-protocol: *([^\r]+)/i, request)
        send(parent, {:offered, offered})

        accept =
          :crypto.hash(:sha, key <> "258EAFA5-E914-47DA-95CA-C5AB0DC85B11") |> Base.encode64()

        protocol = if chosen, do: "sec-websocket-protocol: #{chosen}\r\n", else: ""

        :ok =
          :gen_tcp.send(
            socket,
            "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\nconnection: Upgrade\r\n" <>
              "sec-websocket-accept: #{accept}\r\n" <> protocol <> "\r\n"
          )

        Process.sleep(:infinity)
      end)

      URI.parse("ws://127.0.0.1:#{port}/rpc")
    end

    @codecs [SurrealDB.Codec.CBOR2, SurrealDB.Codec.CBOR]

    test "a 4.0 server picks cbor.2" do
      assert {:ok, conn} = WebSocket.connect(upgrade_server("cbor.2"), codecs: @codecs)
      assert_receive {:offered, "cbor.2, cbor"}
      assert WebSocket.codec(conn) == SurrealDB.Codec.CBOR2
      WebSocket.close(conn)
    end

    test "an older server picks cbor" do
      assert {:ok, conn} = WebSocket.connect(upgrade_server("cbor"), codecs: @codecs)
      assert WebSocket.codec(conn) == SurrealDB.Codec.CBOR
      WebSocket.close(conn)
    end

    test "a server that names no subprotocol gets the oldest one offered" do
      assert {:ok, conn} = WebSocket.connect(upgrade_server(nil), codecs: @codecs)
      assert WebSocket.codec(conn) == SurrealDB.Codec.CBOR
      WebSocket.close(conn)
    end
  end
end
