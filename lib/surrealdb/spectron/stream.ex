defmodule SurrealDB.Spectron.Stream do
  @moduledoc false
  # Server-sent events streaming for `SurrealDB.Spectron.chat/3` with `stream: true`.
  #
  # Returns a lazy `Stream` of decoded chunk maps. Each `data:` line is parsed as JSON; the
  # terminal `data: [DONE]` marker and the end of the HTTP response both stop the stream.

  alias SurrealDB.Spectron.Error

  @doc "Builds a lazy stream of chat chunks for the given request."
  @spec start(struct(), String.t(), iodata(), [{String.t(), String.t()}]) :: Enumerable.t()
  def start(client, path, body, headers) do
    Stream.resource(
      fn -> connect(client, path, body, headers) end,
      &next/1,
      &cleanup/1
    )
  end

  defp connect(client, path, body, headers) do
    %URI{} = uri = client.endpoint
    scheme = if uri.scheme == "https", do: :https, else: :http
    host = uri.host
    port = uri.port || if(scheme == :https, do: 443, else: 80)
    connect_opts = [protocols: [:http1], mode: :passive] ++ tls_opts(scheme)

    case Mint.HTTP.connect(scheme, host, port, connect_opts) do
      {:ok, conn} ->
        {:ok, conn, ref} = Mint.HTTP.request(conn, "POST", path, headers, body)
        %{conn: conn, ref: ref, buffer: "", done: false, timeout: client.timeout}

      {:error, reason} ->
        raise Error.connection(reason)
    end
  end

  defp next(%{done: true, buffer: ""} = state), do: {:halt, state}

  defp next(%{done: true, buffer: buffer} = state) do
    {events, _rest} = parse_events(buffer <> "\n\n")
    {events, %{state | buffer: ""}}
  end

  defp next(state) do
    case Mint.HTTP.recv(state.conn, 0, state.timeout) do
      {:ok, conn, responses} ->
        state = %{state | conn: conn}
        {data, done} = collect(responses, state.ref, "", false)
        {events, buffer} = parse_events(state.buffer <> data)

        case {events, done} do
          {[], false} -> {[], %{state | buffer: buffer}}
          {events, done} -> {events, %{state | buffer: buffer, done: done}}
        end

      {:error, _conn, _reason, _responses} ->
        {:halt, %{state | done: true}}
    end
  end

  defp collect([], _ref, data, done), do: {data, done}

  defp collect([{:status, ref, _status} | rest], ref, data, done),
    do: collect(rest, ref, data, done)

  defp collect([{:headers, ref, _h} | rest], ref, data, done), do: collect(rest, ref, data, done)

  defp collect([{:data, ref, chunk} | rest], ref, data, done),
    do: collect(rest, ref, data <> chunk, done)

  defp collect([{:done, ref} | rest], ref, data, _done), do: collect(rest, ref, data, true)
  defp collect([_other | rest], ref, data, done), do: collect(rest, ref, data, done)

  defp parse_events(buffer) do
    parts = String.split(buffer, "\n\n")
    {complete, remaining} = Enum.split(parts, -1)
    events = complete |> Enum.map(&parse_event/1) |> Enum.reject(&is_nil/1)
    {events, List.first(remaining) || ""}
  end

  defp parse_event(block) do
    data =
      block
      |> String.split("\n")
      |> Enum.filter(&String.starts_with?(&1, "data:"))
      |> Enum.map_join("\n", fn line ->
        line |> String.replace_prefix("data:", "") |> String.trim()
      end)

    cond do
      data == "" -> nil
      data == "[DONE]" -> nil
      true -> decode(data)
    end
  end

  defp decode(data) do
    case Jason.decode(data) do
      {:ok, decoded} -> decoded
      {:error, _} -> %{"delta" => data}
    end
  end

  defp cleanup(%{conn: conn}), do: Mint.HTTP.close(conn)
  defp cleanup(_state), do: :ok

  defp tls_opts(:https), do: [transport_opts: [cacerts: :public_key.cacerts_get()]]
  defp tls_opts(:http), do: []
end
