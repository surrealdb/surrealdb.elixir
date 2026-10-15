defmodule SurrealDB.Engine.HTTP do
  @moduledoc """
  HTTP transport engine, backed by `Mint`.

  Each RPC is a `POST` to the SurrealDB `/rpc` endpoint. Because HTTP is stateless, the
  current namespace, database, and auth token tracked by the connection are sent as
  `Surreal-NS`, `Surreal-DB`, and `Authorization` headers on every request.

  Live queries are not available on this engine. Use a `ws://` or `wss://` URL for those.

  The underlying HTTP/1.1 connection is kept alive between requests. When the server or
  the network closes it, the requests in flight fail and the next request opens a new
  connection.
  """

  @behaviour SurrealDB.Engine

  defstruct [:mint, :scheme, :host, :port, :path, :content_type, :connect_opts, requests: %{}]

  @impl true
  def supports_live?, do: false

  @impl true
  def connect(%URI{} = uri, opts) do
    scheme = if uri.scheme == "https", do: :https, else: :http
    host = uri.host || "localhost"
    port = uri.port || default_port(scheme)
    path = rpc_path(uri.path)
    content_type = Keyword.get(opts, :content_type, "application/cbor")

    connect_opts =
      [protocols: [:http1], mode: :active]
      |> Keyword.merge(transport_opts(scheme, opts))

    conn = %__MODULE__{
      scheme: scheme,
      host: host,
      port: port,
      path: path,
      content_type: content_type,
      connect_opts: connect_opts
    }

    open(conn)
  end

  defp open(conn) do
    case Mint.HTTP.connect(conn.scheme, conn.host, conn.port, conn.connect_opts) do
      {:ok, mint} -> {:ok, %{conn | mint: mint}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def request(%__MODULE__{} = conn, payload, session) do
    headers = build_headers(conn, session)
    body = IO.iodata_to_binary(payload)

    with {:ok, conn} <- ensure_open(conn) do
      case send_request(conn, headers, body) do
        {:ok, conn} ->
          {:ok, conn}

        # The server closed an idle keep-alive connection before we noticed. Nothing was
        # sent, so it is safe to open a new connection and try once more.
        {:error, %{requests: requests} = conn, _reason} when map_size(requests) == 0 ->
          with {:ok, conn} <- reopen(conn), {:ok, conn} <- send_request(conn, headers, body) do
            {:ok, conn}
          else
            {:error, _conn, reason} -> {:error, reason}
            {:error, reason} -> {:error, reason}
          end

        {:error, _conn, reason} ->
          {:error, reason}
      end
    end
  end

  defp send_request(conn, headers, body) do
    case Mint.HTTP.request(conn.mint, "POST", conn.path, headers, body) do
      {:ok, mint, ref} ->
        requests = Map.put(conn.requests, ref, %{status: nil, body: []})
        {:ok, %{conn | mint: mint, requests: requests}}

      {:error, mint, reason} ->
        {:error, %{conn | mint: mint}, reason}
    end
  end

  defp ensure_open(conn) do
    if Mint.HTTP.open?(conn.mint), do: {:ok, conn}, else: reopen(conn)
  end

  defp reopen(conn) do
    Mint.HTTP.close(conn.mint)
    open(%{conn | requests: %{}})
  end

  @impl true
  def handle_transport(%__MODULE__{} = conn, message) do
    case Mint.HTTP.stream(conn.mint, message) do
      :unknown ->
        :unknown

      {:ok, mint, responses} ->
        {events, requests} = process_responses(responses, conn.requests, [])
        {:ok, events, %{conn | mint: mint, requests: requests}}

      # The connection dropped. Whatever completed before it is still delivered; the rest
      # is lost, and the next request reconnects.
      {:error, mint, reason, responses} ->
        {events, _requests} = process_responses(responses, conn.requests, [])
        Mint.HTTP.close(mint)
        {:ok, events ++ [{:reset, reason}], %{conn | mint: mint, requests: %{}}}
    end
  end

  @impl true
  def close(%__MODULE__{mint: mint}) do
    Mint.HTTP.close(mint)
    :ok
  end

  defp process_responses([], requests, events), do: {Enum.reverse(events), requests}

  defp process_responses([{:status, ref, status} | rest], requests, events) do
    requests = update_in(requests[ref].status, fn _ -> status end)
    process_responses(rest, requests, events)
  end

  defp process_responses([{:headers, _ref, _headers} | rest], requests, events) do
    process_responses(rest, requests, events)
  end

  defp process_responses([{:data, ref, data} | rest], requests, events) do
    requests = update_in(requests[ref].body, &[&1, data])
    process_responses(rest, requests, events)
  end

  defp process_responses([{:done, ref} | rest], requests, events) do
    {request, requests} = Map.pop(requests, ref)
    body = IO.iodata_to_binary(request.body)
    process_responses(rest, requests, [{:message, body} | events])
  end

  defp process_responses([{:error, ref, reason} | rest], requests, events) do
    requests = Map.delete(requests, ref)
    process_responses(rest, requests, [{:reset, reason} | events])
  end

  defp process_responses([_other | rest], requests, events) do
    process_responses(rest, requests, events)
  end

  defp build_headers(conn, session) do
    [{"content-type", conn.content_type}, {"accept", conn.content_type}]
    |> maybe_header("surreal-ns", session[:namespace])
    |> maybe_header("surreal-db", session[:database])
    |> maybe_header("authorization", bearer(session[:token]))
  end

  defp maybe_header(headers, _name, nil), do: headers
  defp maybe_header(headers, name, value), do: [{name, value} | headers]

  defp bearer(nil), do: nil
  defp bearer(token), do: "Bearer #{token}"

  defp default_port(:https), do: 443
  defp default_port(:http), do: 80

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
