defmodule SurrealDB.Test.ProtocolProbe do
  @moduledoc false

  # Whether the server at `url` negotiates cbor.2 (SurrealDB 4.0 and later). Integration
  # tests that need it are skipped on older servers, and on no server at all.
  def cbor2?(url) do
    uri = URI.parse(url)
    codecs = [SurrealDB.Codec.CBOR2, SurrealDB.Codec.CBOR]
    opts = [codecs: codecs, transport_opts: [timeout: 2_000]]

    with {:ok, engine} <- SurrealDB.Engine.for_scheme(uri.scheme),
         {:ok, conn} <- engine.connect(uri, opts),
         {:ok, codec, conn} <- probe(engine, conn) do
      engine.close(conn)
      codec == SurrealDB.Codec.CBOR2
    else
      _ -> false
    end
  end

  defp probe(SurrealDB.Engine.WebSocket = engine, conn), do: {:ok, engine.codec(conn), conn}

  # HTTP learns the codec from the first reply.
  defp probe(SurrealDB.Engine.HTTP = engine, conn) do
    payload = SurrealDB.Codec.CBOR.encode(%{"id" => 1, "method" => "version", "params" => []})
    {:ok, conn} = engine.request(conn, 1, payload, %{})
    await(engine, conn, System.monotonic_time(:millisecond) + 2_000)
  end

  defp await(engine, conn, deadline) do
    receive do
      message ->
        case engine.handle_transport(conn, message) do
          {:ok, [{:reply, 1, _body} | _], conn} -> {:ok, engine.codec(conn), conn}
          {:ok, _events, conn} -> await(engine, conn, deadline)
          _ -> :error
        end
    after
      max(deadline - System.monotonic_time(:millisecond), 0) -> :error
    end
  end
end
