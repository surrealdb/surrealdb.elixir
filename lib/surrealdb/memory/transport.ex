defmodule SurrealDB.Memory.Transport do
  @moduledoc false
  # Synchronous JSON transport for the Agent Memory REST API, backed by Mint.
  #
  # It builds the shared headers (Bearer auth, optional on-behalf-of delegation, an
  # idempotency key for mutating requests), performs the request, extracts the trace id,
  # decodes the JSON body, retries retryable failures with backoff, and maps non-2xx
  # responses to `SurrealDB.Memory.Error`.

  alias SurrealDB.Memory.Error

  @trace_header "x-spectron-trace-id"

  @doc """
  Performs a JSON request. Options:

    * `:body` - a map encoded as the JSON request body.
    * `:query` - a keyword list or map of query parameters.
    * `:headers` - extra request headers.
  """
  @spec request(struct(), String.t(), String.t(), keyword()) ::
          {:ok, term()} | {:error, Error.t()}
  def request(client, method, path, opts \\ []) do
    body = encode_body(opts[:body])
    full_path = build_path(client, path, opts[:query])
    headers = build_headers(client, method, full_path, body, opts[:headers] || [])

    attempt(client, method, full_path, headers, body, 1)
  end

  defp attempt(client, method, path, headers, body, attempt_number) do
    case do_request(client, method, path, headers, body) do
      {:ok, status, _resp_headers, resp_body} when status in 200..299 ->
        {:ok, decode_body(resp_body)}

      {:ok, status, resp_headers, resp_body} ->
        if retry?(client, status, attempt_number) do
          backoff(client, attempt_number)
          attempt(client, method, path, headers, body, attempt_number + 1)
        else
          {:error, Error.from_response(status, decode_body(resp_body), trace_id(resp_headers))}
        end

      {:error, reason} ->
        if retry?(client, :connection, attempt_number) do
          backoff(client, attempt_number)
          attempt(client, method, path, headers, body, attempt_number + 1)
        else
          {:error, Error.connection(reason)}
        end
    end
  end

  defp do_request(client, method, path, headers, body) do
    %URI{} = uri = client.endpoint
    scheme = if uri.scheme == "https", do: :https, else: :http
    host = uri.host
    port = uri.port || default_port(scheme)
    timeout = client.timeout
    connect_opts = [protocols: [:http1], mode: :passive] ++ tls_opts(scheme, client)

    with {:ok, conn} <- Mint.HTTP.connect(scheme, host, port, connect_opts),
         {:ok, conn, ref} <- Mint.HTTP.request(conn, method, path, headers, body || "") do
      recv(conn, ref, timeout, nil, [], [])
    end
  end

  defp recv(conn, ref, timeout, status, resp_headers, body) do
    case Mint.HTTP.recv(conn, 0, timeout) do
      {:ok, conn, responses} ->
        case reduce(responses, ref, status, resp_headers, body) do
          {:done, status, headers, body} ->
            Mint.HTTP.close(conn)
            {:ok, status, headers, IO.iodata_to_binary(body)}

          {:cont, status, headers, body} ->
            recv(conn, ref, timeout, status, headers, body)

          {:error, reason} ->
            Mint.HTTP.close(conn)
            {:error, reason}
        end

      {:error, _conn, reason, _responses} ->
        {:error, reason}
    end
  end

  defp reduce([], _ref, status, headers, body), do: {:cont, status, headers, body}

  defp reduce([{:status, ref, status} | rest], ref, _s, headers, body),
    do: reduce(rest, ref, status, headers, body)

  defp reduce([{:headers, ref, hs} | rest], ref, status, headers, body),
    do: reduce(rest, ref, status, headers ++ hs, body)

  defp reduce([{:data, ref, data} | rest], ref, status, headers, body),
    do: reduce(rest, ref, status, headers, [body, data])

  defp reduce([{:done, ref} | _rest], ref, status, headers, body),
    do: {:done, status, headers, body}

  defp reduce([{:error, ref, reason} | _rest], ref, _s, _h, _b), do: {:error, reason}

  defp reduce([_other | rest], ref, status, headers, body),
    do: reduce(rest, ref, status, headers, body)

  defp build_headers(client, method, path, body, extra) do
    [
      {"authorization", "Bearer #{client.api_key}"},
      {"accept", "application/json"}
    ]
    |> maybe_content_type(body)
    |> maybe_on_behalf_of(client.on_behalf_of)
    |> maybe_idempotency(method, path, body)
    |> Kernel.++(extra)
  end

  defp maybe_content_type(headers, nil), do: headers
  defp maybe_content_type(headers, _body), do: [{"content-type", "application/json"} | headers]

  defp maybe_on_behalf_of(headers, nil), do: headers

  defp maybe_on_behalf_of(headers, principal),
    do: [{"x-spectron-on-behalf-of", principal} | headers]

  defp maybe_idempotency(headers, method, path, body)
       when method in ["POST", "PUT", "PATCH", "DELETE"] do
    digest =
      :crypto.hash(:sha256, [method, " ", path, " ", body || ""]) |> Base.encode16(case: :lower)

    [{"idempotency-key", digest} | headers]
  end

  defp maybe_idempotency(headers, _method, _path, _body), do: headers

  defp build_path(client, path, query) do
    base = String.trim_trailing(client.endpoint.path || "", "/")
    full = base <> path

    case encode_query(query) do
      "" -> full
      qs -> full <> "?" <> qs
    end
  end

  defp encode_query(nil), do: ""
  defp encode_query(query) when query == %{}, do: ""
  defp encode_query([]), do: ""

  defp encode_query(query) do
    query
    |> Enum.reject(fn {_k, v} -> is_nil(v) end)
    |> Enum.map(fn {k, v} -> {to_string(k), to_string(v)} end)
    |> URI.encode_query()
  end

  defp encode_body(nil), do: nil
  defp encode_body(body) when is_binary(body), do: body
  defp encode_body(body), do: Jason.encode!(body)

  defp decode_body(""), do: nil

  defp decode_body(body) do
    case Jason.decode(body) do
      {:ok, decoded} -> decoded
      {:error, _} -> body
    end
  end

  defp trace_id(headers) do
    Enum.find_value(headers, fn {name, value} ->
      if String.downcase(name) == @trace_header, do: value
    end)
  end

  defp retry?(%{retry: false}, _status, _attempt), do: false

  defp retry?(%{retry: retry}, status, attempt) when is_map(retry) do
    attempt < Map.get(retry, :max_attempts, 3) and retryable_status?(status)
  end

  defp retry?(_client, _status, _attempt), do: false

  defp retryable_status?(:connection), do: true
  defp retryable_status?(429), do: true
  defp retryable_status?(status) when is_integer(status) and status in 500..599, do: true
  defp retryable_status?(_status), do: false

  defp backoff(%{retry: retry}, attempt) when is_map(retry) do
    base = Map.get(retry, :base_ms, 200)
    Process.sleep(base * round(:math.pow(2, attempt - 1)))
  end

  defp default_port(:https), do: 443
  defp default_port(:http), do: 80

  defp tls_opts(:https, client) do
    [transport_opts: Map.get(client.transport_opts, :tls, cacerts: :public_key.cacerts_get())]
  end

  defp tls_opts(:http, _client), do: []
end
