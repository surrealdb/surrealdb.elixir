defmodule SurrealDB.Rest do
  @moduledoc false
  # One-off synchronous HTTP requests for the SurrealDB `/export` and `/import` endpoints,
  # which are served over HTTP regardless of the connection's engine. The base address is
  # derived from the connection URL (`ws`/`wss` are mapped to `http`/`https`).

  alias SurrealDB.Error

  @spec export(map(), keyword()) :: {:ok, String.t()} | {:error, Error.t()}
  def export(session, opts) do
    headers = [{"accept", "text/plain"}]
    timeout = Keyword.get(opts, :timeout, 60_000)

    case request(session, "GET", "/export", headers, "", timeout) do
      {:ok, status, body} when status in 200..299 ->
        {:ok, body}

      {:ok, status, body} ->
        {:error, Error.new(:rpc, "export failed", code: status, details: body)}

      {:error, error} ->
        {:error, error}
    end
  end

  @spec import(map(), String.t(), keyword()) :: {:ok, :imported} | {:error, Error.t()}
  def import(session, surql, opts) do
    headers = [{"content-type", "text/plain"}, {"accept", "application/json"}]
    timeout = Keyword.get(opts, :timeout, 60_000)

    case request(session, "POST", "/import", headers, surql, timeout) do
      {:ok, status, _body} when status in 200..299 ->
        {:ok, :imported}

      {:ok, status, body} ->
        {:error, Error.new(:rpc, "import failed", code: status, details: body)}

      {:error, error} ->
        {:error, error}
    end
  end

  defp request(session, method, path, headers, body, timeout) do
    %URI{} = uri = session.uri
    scheme = http_scheme(uri.scheme)
    host = uri.host || "localhost"
    port = uri.port || default_port(scheme)
    headers = headers ++ session_headers(session)

    connect_opts = [protocols: [:http1], mode: :passive] ++ tls_opts(scheme)

    with {:ok, conn} <- Mint.HTTP.connect(scheme, host, port, connect_opts),
         {:ok, conn, ref} <- Mint.HTTP.request(conn, method, path, headers, body),
         {:ok, status, response_body} <- recv(conn, ref, timeout, nil, []) do
      {:ok, status, response_body}
    else
      {:error, reason} -> {:error, connection_error(reason)}
      {:error, _conn, reason} -> {:error, connection_error(reason)}
    end
  end

  defp recv(conn, ref, timeout, status, body) do
    case Mint.HTTP.recv(conn, 0, timeout) do
      {:ok, conn, responses} ->
        case reduce(responses, ref, status, body) do
          {:done, status, body} ->
            Mint.HTTP.close(conn)
            {:ok, status, IO.iodata_to_binary(body)}

          {:cont, status, body} ->
            recv(conn, ref, timeout, status, body)

          {:error, reason} ->
            Mint.HTTP.close(conn)
            {:error, connection_error(reason)}
        end

      {:error, _conn, reason, _responses} ->
        {:error, connection_error(reason)}
    end
  end

  defp reduce([], _ref, status, body), do: {:cont, status, body}

  defp reduce([{:status, ref, status} | rest], ref, _status, body),
    do: reduce(rest, ref, status, body)

  defp reduce([{:headers, ref, _h} | rest], ref, status, body),
    do: reduce(rest, ref, status, body)

  defp reduce([{:data, ref, data} | rest], ref, status, body),
    do: reduce(rest, ref, status, [body, data])

  defp reduce([{:done, ref} | _rest], ref, status, body), do: {:done, status, body}
  defp reduce([{:error, ref, reason} | _rest], ref, _status, _body), do: {:error, reason}
  defp reduce([_other | rest], ref, status, body), do: reduce(rest, ref, status, body)

  defp session_headers(session) do
    []
    |> put_header("surreal-ns", session[:namespace])
    |> put_header("surreal-db", session[:database])
    |> put_header("authorization", bearer(session[:token]))
  end

  defp put_header(headers, _name, nil), do: headers
  defp put_header(headers, name, value), do: [{name, value} | headers]

  defp bearer(nil), do: nil
  defp bearer(token), do: "Bearer #{token}"

  defp http_scheme("wss"), do: :https
  defp http_scheme("https"), do: :https
  defp http_scheme(_), do: :http

  defp default_port(:https), do: 443
  defp default_port(:http), do: 80

  defp tls_opts(:https), do: [transport_opts: [cacerts: :public_key.cacerts_get()]]
  defp tls_opts(:http), do: []

  defp connection_error(reason), do: Error.new(:connection, inspect(reason), details: reason)
end
