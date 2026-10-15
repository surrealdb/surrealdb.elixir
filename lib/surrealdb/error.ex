defmodule SurrealDB.Error do
  @moduledoc """
  Error returned by SurrealDB operations.

  The `kind` field says where the failure came from:

    * `:rpc` - the server returned an error, for an RPC call or for one statement of a
      query. `server_kind`, `code`, `details`, and `cause` describe it (see below).
    * `:query` - one or more statements of a `SurrealDB.query/3` failed. `details.results`
      holds every statement's outcome as `{:ok, value}` or `{:error, error}`.
    * `:connection` - the transport could not be established or was lost.
    * `:decode` - a response could not be decoded.
    * `:auth` - authentication or authorisation failed (including HTTP 401 and 403).
    * `:http` - the HTTP engine got a non-2xx status; `code` holds the status.
    * `:timeout` - the request did not complete in time.
    * `:unsupported` - the operation is not available on the active engine or server.

  ## Server errors

  For errors from the server, `server_kind` is the server's error kind as an atom:
  `:validation`, `:configuration`, `:thrown`, `:query`, `:serialization`, `:not_allowed`,
  `:not_found`, `:already_exists`, `:connection`, `:internal`, or `:context`. Servers that
  send only a JSON-RPC `code` (before 3.0) get the kind derived from the code.

  `details` holds the server's structured details, a nested `%{"kind" => _, "details" => _}`
  map, for example a token expiry is

      %{"kind" => "Auth", "details" => %{"kind" => "TokenExpired"}}

  `detail_kinds/1` flattens that to `["Auth", "TokenExpired"]`, and helpers such as
  `token_expired?/1` and `transaction_conflict?/1` cover the common checks. `cause` is the
  underlying error, when the server sends one.
  """

  @type kind :: :rpc | :query | :connection | :decode | :auth | :http | :timeout | :unsupported
  @type server_kind ::
          :validation
          | :configuration
          | :thrown
          | :query
          | :serialization
          | :not_allowed
          | :not_found
          | :already_exists
          | :connection
          | :internal
          | :context

  @type t :: %__MODULE__{
          kind: kind(),
          message: String.t(),
          code: integer() | nil,
          server_kind: server_kind() | nil,
          details: term(),
          cause: t() | nil
        }

  defexception [:message, :code, :server_kind, :details, :cause, kind: :rpc]

  @server_kinds %{
    "Validation" => :validation,
    "Configuration" => :configuration,
    "Thrown" => :thrown,
    "Query" => :query,
    "Serialization" => :serialization,
    "NotAllowed" => :not_allowed,
    "NotFound" => :not_found,
    "AlreadyExists" => :already_exists,
    "Connection" => :connection,
    "Internal" => :internal,
    "Context" => :context
  }

  # JSON-RPC codes, for servers that do not send a kind.
  @code_kinds %{
    -32_700 => :validation,
    -32_600 => :validation,
    -32_601 => :not_found,
    -32_602 => :not_allowed,
    -32_603 => :validation,
    -32_604 => :configuration,
    -32_605 => :configuration,
    -32_606 => :configuration,
    -32_607 => :configuration,
    -32_000 => :internal,
    -32_001 => :connection,
    -32_002 => :not_allowed,
    -32_003 => :query,
    -32_004 => :query,
    -32_005 => :query,
    -32_006 => :thrown,
    -32_007 => :serialization,
    -32_008 => :serialization,
    -32_009 => :query
  }

  @doc "Builds an error of the given kind."
  @spec new(kind(), String.t(), keyword()) :: t()
  def new(kind, message, opts \\ []) do
    %__MODULE__{
      kind: kind,
      message: message,
      code: opts[:code],
      server_kind: opts[:server_kind],
      details: opts[:details],
      cause: opts[:cause]
    }
  end

  @doc """
  Builds an `:rpc` error from a SurrealDB RPC error object:
  `%{"code" => _, "message" => _, "kind" => _, "details" => _, "cause" => _}`.
  """
  @spec from_rpc(map()) :: t()
  def from_rpc(%{"message" => message} = error) do
    code = error["code"]

    %__MODULE__{
      kind: :rpc,
      message: to_string(message),
      code: code,
      server_kind: server_kind(error["kind"], code),
      details: present(error["details"]),
      cause: cause(error["cause"])
    }
  end

  def from_rpc(other) do
    %__MODULE__{kind: :rpc, message: inspect(other), details: other}
  end

  @doc """
  Builds an `:rpc` error from a failed statement in a `query` result:
  `%{"status" => "ERR", "result" => message, "kind" => _, "details" => _}`.
  """
  @spec from_statement(map()) :: t()
  def from_statement(%{"result" => message} = statement) do
    kind = statement["kind"]
    details = unwrap_details(present(statement["details"]), kind)

    %__MODULE__{
      kind: :rpc,
      message: to_string(message),
      server_kind: server_kind(kind, nil),
      details: details
    }
  end

  @doc """
  The chain of detail kinds, outermost first, for example `["Auth", "TokenExpired"]`.
  Empty when the error has no structured details.
  """
  @spec detail_kinds(t()) :: [String.t()]
  def detail_kinds(%__MODULE__{details: details}), do: walk_kinds(details)

  @doc """
  The innermost detail fields, such as `%{"name" => "person"}` for a missing table, or
  `nil` when there are none.
  """
  @spec detail_fields(t()) :: map() | nil
  def detail_fields(%__MODULE__{details: details}), do: innermost(details)

  @doc "Whether the server rejected the request because the token expired."
  @spec token_expired?(t()) :: boolean()
  def token_expired?(%__MODULE__{} = error) do
    error.server_kind == :not_allowed and detail_kinds(error) == ["Auth", "TokenExpired"]
  end

  @doc "Whether the server rejected the request because the session expired."
  @spec session_expired?(t()) :: boolean()
  def session_expired?(%__MODULE__{} = error) do
    error.server_kind == :not_allowed and detail_kinds(error) == ["Auth", "SessionExpired"]
  end

  @doc "Whether a transaction failed because it conflicted with another and can be retried."
  @spec transaction_conflict?(t()) :: boolean()
  def transaction_conflict?(%__MODULE__{} = error) do
    error.code == -32_009 or
      (error.server_kind == :query and detail_kinds(error) == ["TransactionConflict"])
  end

  @doc "Whether the server timed the query out."
  @spec timed_out?(t()) :: boolean()
  def timed_out?(%__MODULE__{kind: :timeout}), do: true

  def timed_out?(%__MODULE__{} = error),
    do: error.server_kind == :query and List.first(detail_kinds(error)) == "TimedOut"

  @impl true
  def message(%__MODULE__{} = error) do
    label =
      case error.server_kind do
        nil -> "#{error.kind}"
        server_kind -> "#{error.kind}:#{server_kind}"
      end

    case error.code do
      nil -> "[#{label}] #{error.message}"
      code -> "[#{label}] (#{code}) #{error.message}"
    end
  end

  # ── Helpers ────────────────────────────────────────────────────────────────

  defp server_kind(kind, _code) when is_binary(kind), do: Map.get(@server_kinds, kind, :internal)
  defp server_kind(_kind, code) when is_integer(code), do: Map.get(@code_kinds, code, :internal)
  defp server_kind(_kind, _code), do: nil

  defp cause(%{"message" => _} = cause), do: from_rpc(cause)
  defp cause(_other), do: nil

  # NONE (decoded as a struct) and empty maps mean "no details".
  defp present(%{} = map) when map_size(map) == 0, do: nil
  defp present(%{__struct__: _}), do: nil
  defp present(%{} = map), do: map
  defp present(_other), do: nil

  # Some servers repeat the statement's kind inside its details; drop that wrapper so
  # statement and RPC errors have the same shape.
  defp unwrap_details(%{"kind" => kind, "details" => inner}, kind), do: present(inner)
  defp unwrap_details(%{"kind" => kind} = details, kind) when map_size(details) == 1, do: nil
  defp unwrap_details(details, _kind), do: details

  defp walk_kinds(%{"kind" => kind} = details) when is_binary(kind),
    do: [kind | walk_kinds(details["details"])]

  defp walk_kinds(_other), do: []

  defp innermost(%{"kind" => _kind, "details" => %{} = inner}) do
    case innermost(inner) do
      nil -> if Map.has_key?(inner, "kind"), do: nil, else: inner
      fields -> fields
    end
  end

  defp innermost(%{"kind" => _kind}), do: nil
  defp innermost(%{} = fields), do: fields
  defp innermost(_other), do: nil
end
