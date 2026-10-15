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

  alias SurrealDB.Error

  defstruct [
    :mint,
    :scheme,
    :host,
    :port,
    :path,
    :content_type,
    :connect_opts,
    :codec,
    requests: %{}
  ]

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
      connect_opts: connect_opts,
      codec: Keyword.get(opts, :codec, SurrealDB.Codec.CBOR)
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
  def request(%__MODULE__{} = conn, id, payload, session) do
    headers = build_headers(conn, session)
    body = IO.iodata_to_binary(payload)

    with {:ok, conn} <- ensure_open(conn) do
      case send_request(conn, id, headers, body) do
        {:ok, conn} ->
          {:ok, conn}

        # The server closed an idle keep-alive connection before we noticed, so writing the
        # request failed. This is the only error retried: it comes from the write itself,
        # so the server has at most part of this request, which it cannot run, and there is
        # no earlier request in flight that could be lost. Errors after a successful write
        # (while waiting for the response) are never retried, since the server may have
        # run the request.
        {:error, %{requests: requests} = conn, %Mint.TransportError{reason: reason}}
        when map_size(requests) == 0 and reason in [:closed, :epipe, :econnreset, :enotconn] ->
          with {:ok, conn} <- reopen(conn),
               {:ok, conn} <- send_request(conn, id, headers, body) do
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

  defp send_request(conn, id, headers, body) do
    case Mint.HTTP.request(conn.mint, "POST", conn.path, headers, body) do
      {:ok, mint, ref} ->
        requests = Map.put(conn.requests, ref, %{id: id, status: nil, headers: [], body: []})
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

  # HTTP/1.1 answers pipelined requests in order, so a request that timed out would hold up
  # every request after it. Close the connection (the next request opens a new one) and
  # fail the requests that were waiting behind it; they cannot be retried, since the server
  # may already have run them.
  @impl true
  def cancel(%__MODULE__{} = conn, id) do
    if Enum.any?(conn.requests, fn {_ref, request} -> request.id == id end) do
      {:ok, mint} = Mint.HTTP.close(conn.mint)

      error =
        Error.new(
          :connection,
          "the HTTP connection was closed because an earlier request on it (#{id}) timed out"
        )

      events =
        for {_ref, request} <- conn.requests, request.id != id, do: {:failed, request.id, error}

      {:ok, events, %{conn | mint: mint, requests: %{}}}
    else
      {:ok, [], conn}
    end
  end

  @impl true
  def handle_transport(%__MODULE__{} = conn, message) do
    case Mint.HTTP.stream(conn.mint, message) do
      :unknown ->
        :unknown

      {:ok, mint, responses} ->
        {events, requests} = process_responses(responses, conn, conn.requests, [])
        {:ok, events, %{conn | mint: mint, requests: requests}}

      # The connection dropped. Whatever completed before it is still delivered; the rest
      # is lost, and the next request reconnects.
      {:error, mint, reason, responses} ->
        {events, _requests} = process_responses(responses, conn, conn.requests, [])
        Mint.HTTP.close(mint)
        {:ok, events ++ [{:reset, reason}], %{conn | mint: mint, requests: %{}}}
    end
  end

  @impl true
  def close(%__MODULE__{mint: mint}) do
    Mint.HTTP.close(mint)
    :ok
  end

  defp process_responses([], _conn, requests, events), do: {Enum.reverse(events), requests}

  defp process_responses([{:status, ref, status} | rest], conn, requests, events) do
    requests = update_request(requests, ref, &%{&1 | status: status})
    process_responses(rest, conn, requests, events)
  end

  defp process_responses([{:headers, ref, headers} | rest], conn, requests, events) do
    requests = update_request(requests, ref, &%{&1 | headers: &1.headers ++ headers})
    process_responses(rest, conn, requests, events)
  end

  defp process_responses([{:data, ref, data} | rest], conn, requests, events) do
    requests = update_request(requests, ref, &%{&1 | body: [&1.body, data]})
    process_responses(rest, conn, requests, events)
  end

  defp process_responses([{:done, ref} | rest], conn, requests, events) do
    case Map.pop(requests, ref) do
      {nil, requests} ->
        process_responses(rest, conn, requests, events)

      {request, requests} ->
        event = response_event(conn, request, IO.iodata_to_binary(request.body))
        process_responses(rest, conn, requests, [event | events])
    end
  end

  defp process_responses([{:error, ref, reason} | rest], conn, requests, events) do
    case Map.pop(requests, ref) do
      {nil, requests} ->
        process_responses(rest, conn, requests, events)

      {request, requests} ->
        error = Error.new(:connection, inspect(reason), details: reason)
        process_responses(rest, conn, requests, [{:failed, request.id, error} | events])
    end
  end

  defp process_responses([_other | rest], conn, requests, events) do
    process_responses(rest, conn, requests, events)
  end

  defp update_request(requests, ref, fun) do
    case Map.fetch(requests, ref) do
      {:ok, request} -> Map.put(requests, ref, fun.(request))
      :error -> requests
    end
  end

  # RPC errors arrive with status 200, so a 2xx body is always an RPC message. Any other
  # status is a failure of the request itself (bad token, wrong content type, server
  # error), and its body is plain text or JSON rather than an RPC message.
  defp response_event(_conn, %{id: id, status: status}, body) when status in 200..299,
    do: {:reply, id, body}

  defp response_event(conn, %{id: id, status: status, headers: headers}, body) do
    {:failed, id, status_error(conn, status, headers, body)}
  end

  defp status_error(conn, status, headers, body) do
    content_type = header(headers, "content-type") || ""

    case decode_error_body(conn, content_type, body) do
      %{"error" => %{} = rpc_error} ->
        Error.from_rpc(rpc_error)

      %{} = json ->
        message =
          Enum.find_value(["information", "description", "details"], body, fn key ->
            if is_binary(json[key]), do: json[key]
          end)

        Error.new(status_kind(status), "HTTP #{status}: #{message}", code: status, details: json)

      nil ->
        message = if body == "", do: reason_phrase(status), else: String.trim(body)
        Error.new(status_kind(status), "HTTP #{status}: #{message}", code: status, details: body)
    end
  end

  defp decode_error_body(conn, content_type, body) do
    cond do
      String.starts_with?(content_type, "application/json") ->
        case Jason.decode(body) do
          {:ok, %{} = map} -> map
          _ -> nil
        end

      String.starts_with?(content_type, conn.codec.content_type()) ->
        case conn.codec.decode(body) do
          {:ok, %{} = map} -> map
          _ -> nil
        end

      true ->
        nil
    end
  end

  defp header(headers, name) do
    Enum.find_value(headers, fn {key, value} -> if String.downcase(key) == name, do: value end)
  end

  defp status_kind(status) when status in [401, 403], do: :auth
  defp status_kind(_status), do: :http

  defp reason_phrase(401), do: "Unauthorized"
  defp reason_phrase(403), do: "Forbidden"
  defp reason_phrase(404), do: "Not Found"
  defp reason_phrase(413), do: "Payload Too Large"
  defp reason_phrase(415), do: "Unsupported Media Type"
  defp reason_phrase(status) when status >= 500, do: "Server Error"
  defp reason_phrase(_status), do: "Request Failed"

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

  defp transport_opts(scheme, opts) do
    given =
      case {scheme, Keyword.get(opts, :transport_opts)} do
        {:https, nil} -> [cacerts: :public_key.cacerts_get()]
        {_scheme, nil} -> []
        {_scheme, given} -> given
      end

    case Keyword.merge(send_timeout_opts(opts), given) do
      [] -> []
      transport_opts -> [transport_opts: transport_opts]
    end
  end

  # A write to a peer that stopped reading fails after the request timeout, instead of
  # blocking the connection process inside `request/4` for good.
  defp send_timeout_opts(opts) do
    case Keyword.get(opts, :send_timeout) do
      timeout when is_integer(timeout) -> [send_timeout: timeout, send_timeout_close: true]
      _none -> []
    end
  end
end
