defmodule SurrealDB.Engine.HTTP do
  @moduledoc """
  HTTP transport engine, backed by `Mint`.

  Each RPC is a `POST` to the SurrealDB `/rpc` endpoint. Because HTTP is stateless, the
  current namespace, database, and auth token tracked by the connection are sent as
  `Surreal-NS`, `Surreal-DB`, and `Authorization` headers on every request.

  Live queries are not available on this engine. Use a `ws://` or `wss://` URL for those.
  """

  @behaviour SurrealDB.Engine

  defstruct [:mint, :path, :host, :content_type, requests: %{}]

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

    case Mint.HTTP.connect(scheme, host, port, connect_opts) do
      {:ok, mint} ->
        {:ok, %__MODULE__{mint: mint, path: path, host: host, content_type: content_type}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def request(%__MODULE__{} = conn, payload, session) do
    headers = build_headers(conn, session)
    body = IO.iodata_to_binary(payload)

    case Mint.HTTP.request(conn.mint, "POST", conn.path, headers, body) do
      {:ok, mint, ref} ->
        requests = Map.put(conn.requests, ref, %{status: nil, body: []})
        {:ok, %{conn | mint: mint, requests: requests}}

      {:error, mint, reason} ->
        {:error, %{conn | mint: mint}, reason} |> elem_error()
    end
  end

  defp elem_error({:error, _conn, reason}), do: {:error, reason}

  @impl true
  def handle_transport(%__MODULE__{} = conn, message) do
    case Mint.HTTP.stream(conn.mint, message) do
      :unknown ->
        :unknown

      {:ok, mint, responses} ->
        {events, requests} = process_responses(responses, conn.requests, [])
        {:ok, events, %{conn | mint: mint, requests: requests}}

      {:error, mint, _reason, _responses} ->
        {:ok, [:closed], %{conn | mint: mint}}
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

  defp process_responses([{:error, ref, _reason} | rest], requests, events) do
    requests = Map.delete(requests, ref)
    process_responses(rest, requests, [:closed | events])
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
