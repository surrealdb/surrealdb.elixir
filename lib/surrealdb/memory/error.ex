defmodule SurrealDB.Memory.Error do
  @moduledoc """
  Error returned by the Agent Memory client.

  The `kind` mirrors the JavaScript SDK's error classes:

    * `:auth` - 401 Unauthorized.
    * `:validation` - 400 / 422 and other 4xx without a more specific kind.
    * `:not_found` - 404.
    * `:rate_limit` - 429.
    * `:scope` - 403 scope or authorisation failure.
    * `:server` - 5xx.
    * `:connection` - the request could not be completed.

  `trace_id` carries the `X-Spectron-Trace-Id` header when the server returned one.
  """

  @type kind :: :auth | :validation | :not_found | :rate_limit | :scope | :server | :connection
  @type t :: %__MODULE__{
          kind: kind(),
          status: pos_integer() | nil,
          message: String.t(),
          trace_id: String.t() | nil,
          details: term()
        }

  defexception [:status, :trace_id, :details, kind: :server, message: "Agent Memory error"]

  @doc "Builds a connection error."
  @spec connection(term()) :: t()
  def connection(reason) do
    %__MODULE__{kind: :connection, message: "request failed: #{inspect(reason)}", details: reason}
  end

  @doc "Maps an HTTP status, decoded body, and trace id to an error."
  @spec from_response(pos_integer(), term(), String.t() | nil) :: t()
  def from_response(status, body, trace_id) do
    %__MODULE__{
      kind: kind_for_status(status),
      status: status,
      message: message_for(body, status),
      trace_id: trace_id,
      details: body
    }
  end

  defp kind_for_status(401), do: :auth
  defp kind_for_status(403), do: :scope
  defp kind_for_status(404), do: :not_found
  defp kind_for_status(429), do: :rate_limit
  defp kind_for_status(status) when status in 400..499, do: :validation
  defp kind_for_status(status) when status in 500..599, do: :server
  defp kind_for_status(_status), do: :server

  defp message_for(%{"error" => message}, _status) when is_binary(message), do: message
  defp message_for(%{"message" => message}, _status) when is_binary(message), do: message
  defp message_for(_body, status), do: "Agent Memory request failed with status #{status}"

  @impl true
  def message(%__MODULE__{kind: kind, status: nil, message: message}), do: "[#{kind}] #{message}"

  def message(%__MODULE__{kind: kind, status: status, message: message}) do
    "[#{kind}] (#{status}) #{message}"
  end
end
