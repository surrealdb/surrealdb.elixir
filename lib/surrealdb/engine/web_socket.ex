defmodule SurrealDB.Engine.WebSocket do
  @moduledoc """
  WebSocket transport engine, backed by `Mint.WebSocket`.

  It holds a persistent connection to the SurrealDB `/rpc` endpoint and supports live
  queries. The codec's subprotocol is negotiated during the upgrade (`cbor` or `json`), and
  frames are sent as binary or text accordingly.

  The upgrade handshake is performed synchronously while the socket is in passive mode; the
  socket is then switched to active mode so the connection process receives frames as
  messages.
  """

  @behaviour SurrealDB.Engine

  require Logger

  defstruct [:mint, :websocket, :ref, :frame_type]

  @upgrade_timeout 10_000

  @impl true
  def supports_live?, do: true

  @impl true
  def connect(%URI{} = uri, opts) do
    {http_scheme, ws_scheme} =
      if uri.scheme == "wss", do: {:https, :wss}, else: {:http, :ws}

    host = uri.host || "localhost"
    port = uri.port || default_port(ws_scheme)
    path = rpc_path(uri.path)
    subprotocol = Keyword.get(opts, :subprotocol, "cbor")
    frame_type = Keyword.get(opts, :frame_type, :binary)

    connect_opts =
      [protocols: [:http1], mode: :passive]
      |> Keyword.merge(transport_opts(http_scheme, opts))

    upgrade_headers = [{"sec-websocket-protocol", subprotocol}]

    with {:ok, mint} <- Mint.HTTP.connect(http_scheme, host, port, connect_opts),
         {:ok, mint, ref} <- Mint.WebSocket.upgrade(ws_scheme, mint, path, upgrade_headers),
         {:ok, mint, websocket} <- finish_upgrade(mint, ref),
         {:ok, mint} <- Mint.HTTP.set_mode(mint, :active) do
      {:ok, %__MODULE__{mint: mint, websocket: websocket, ref: ref, frame_type: frame_type}}
    else
      {:error, reason} -> {:error, reason}
      {:error, _mint, reason} -> {:error, reason}
    end
  end

  @impl true
  def request(%__MODULE__{} = conn, payload, _session) do
    frame = {conn.frame_type, IO.iodata_to_binary(payload)}

    with {:ok, websocket, data} <- Mint.WebSocket.encode(conn.websocket, frame),
         {:ok, mint} <- Mint.WebSocket.stream_request_body(conn.mint, conn.ref, data) do
      {:ok, %{conn | mint: mint, websocket: websocket}}
    else
      {:error, websocket_or_mint, reason} ->
        {:error, reattach(conn, websocket_or_mint), reason} |> elem_error()
    end
  end

  defp elem_error({:error, _conn, reason}), do: {:error, reason}

  defp reattach(conn, %Mint.WebSocket{} = websocket), do: %{conn | websocket: websocket}
  defp reattach(conn, mint), do: %{conn | mint: mint}

  @impl true
  def handle_transport(%__MODULE__{} = conn, message) do
    case Mint.WebSocket.stream(conn.mint, message) do
      :unknown ->
        :unknown

      {:ok, mint, responses} ->
        conn = %{conn | mint: mint}
        decode_responses(responses, conn, [])

      {:error, mint, _reason, _responses} ->
        {:ok, [:closed], %{conn | mint: mint}}
    end
  end

  @impl true
  def close(%__MODULE__{} = conn) do
    with {:ok, _websocket, data} <- Mint.WebSocket.encode(conn.websocket, :close),
         {:ok, mint} <- Mint.WebSocket.stream_request_body(conn.mint, conn.ref, data) do
      Mint.HTTP.close(mint)
    else
      _ -> Mint.HTTP.close(conn.mint)
    end

    :ok
  end

  defp finish_upgrade(mint, ref) do
    case recv_upgrade(mint, ref, nil, nil) do
      {:ok, mint, status, headers} -> Mint.WebSocket.new(mint, ref, status, headers)
      other -> other
    end
  end

  defp recv_upgrade(mint, ref, status, headers) do
    case Mint.HTTP.recv(mint, 0, @upgrade_timeout) do
      {:ok, mint, responses} ->
        case scan_upgrade(responses, ref, status, headers) do
          {:done, status, headers} -> {:ok, mint, status, headers}
          {:cont, status, headers} -> recv_upgrade(mint, ref, status, headers)
          {:error, reason} -> {:error, reason}
        end

      {:error, mint, reason, _responses} ->
        Mint.HTTP.close(mint)
        {:error, reason}
    end
  end

  defp scan_upgrade([], _ref, status, headers), do: {:cont, status, headers}

  defp scan_upgrade([{:status, ref, status} | rest], ref, _status, headers),
    do: scan_upgrade(rest, ref, status, headers)

  defp scan_upgrade([{:headers, ref, headers} | rest], ref, status, _headers),
    do: scan_upgrade(rest, ref, status, headers)

  defp scan_upgrade([{:done, ref} | _rest], ref, status, headers), do: {:done, status, headers}
  defp scan_upgrade([{:error, ref, reason} | _rest], ref, _status, _headers), do: {:error, reason}

  defp scan_upgrade([_other | rest], ref, status, headers),
    do: scan_upgrade(rest, ref, status, headers)

  defp decode_responses([], conn, events), do: {:ok, Enum.reverse(events), conn}

  defp decode_responses([{:data, ref, data} | rest], %{ref: ref} = conn, events) do
    case Mint.WebSocket.decode(conn.websocket, data) do
      {:ok, websocket, frames} ->
        conn = %{conn | websocket: websocket}
        {conn, new_events} = handle_frames(frames, conn, [])
        decode_responses(rest, conn, new_events ++ events)

      {:error, websocket, reason} ->
        Logger.warning("SurrealDB websocket decode error: #{inspect(reason)}")
        decode_responses(rest, %{conn | websocket: websocket}, events)
    end
  end

  defp decode_responses([_other | rest], conn, events), do: decode_responses(rest, conn, events)

  defp handle_frames([], conn, events), do: {conn, events}

  defp handle_frames([{:binary, payload} | rest], conn, events),
    do: handle_frames(rest, conn, [{:message, payload} | events])

  defp handle_frames([{:text, payload} | rest], conn, events),
    do: handle_frames(rest, conn, [{:message, payload} | events])

  defp handle_frames([{:ping, payload} | rest], conn, events) do
    conn = send_frame(conn, {:pong, payload})
    handle_frames(rest, conn, events)
  end

  defp handle_frames([{:close, _code, _reason} | rest], conn, events),
    do: handle_frames(rest, conn, [:closed | events])

  defp handle_frames([_other | rest], conn, events), do: handle_frames(rest, conn, events)

  defp send_frame(conn, frame) do
    with {:ok, websocket, data} <- Mint.WebSocket.encode(conn.websocket, frame),
         {:ok, mint} <- Mint.WebSocket.stream_request_body(conn.mint, conn.ref, data) do
      %{conn | websocket: websocket, mint: mint}
    else
      _ -> conn
    end
  end

  defp default_port(:wss), do: 443
  defp default_port(:ws), do: 80

  defp rpc_path(nil), do: "/rpc"
  defp rpc_path(""), do: "/rpc"
  defp rpc_path(path), do: path

  defp transport_opts(:https, opts) do
    [transport_opts: Keyword.get(opts, :transport_opts, cacerts: :public_key.cacerts_get())]
  end

  defp transport_opts(:http, opts) do
    case Keyword.get(opts, :transport_opts) do
      nil -> []
      transport_opts -> [transport_opts: transport_opts]
    end
  end
end
