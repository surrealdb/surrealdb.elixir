defmodule SurrealDB.Error do
  @moduledoc """
  Error returned by SurrealDB operations.

  The `kind` field categorises the failure:

    * `:rpc` - the server returned an error for an RPC call (`code`/`message` set).
    * `:connection` - the transport could not be established or was lost.
    * `:decode` - a response could not be decoded.
    * `:auth` - authentication or authorisation failed.
    * `:timeout` - the request did not complete in time.
    * `:unsupported` - the operation is not available on the active engine.
  """

  @type kind :: :rpc | :connection | :decode | :auth | :timeout | :unsupported
  @type t :: %__MODULE__{
          kind: kind(),
          message: String.t(),
          code: integer() | nil,
          details: term()
        }

  defexception [:message, :code, :details, kind: :rpc]

  @doc "Builds an error of the given kind."
  @spec new(kind(), String.t(), keyword()) :: t()
  def new(kind, message, opts \\ []) do
    %__MODULE__{
      kind: kind,
      message: message,
      code: opts[:code],
      details: opts[:details]
    }
  end

  @doc "Builds an `:rpc` error from a SurrealDB RPC error map."
  @spec from_rpc(map()) :: t()
  def from_rpc(%{"message" => message} = error) do
    %__MODULE__{kind: :rpc, message: message, code: error["code"], details: error}
  end

  def from_rpc(other) do
    %__MODULE__{kind: :rpc, message: inspect(other), details: other}
  end

  @impl true
  def message(%__MODULE__{kind: kind, message: message, code: nil}) do
    "[#{kind}] #{message}"
  end

  def message(%__MODULE__{kind: kind, message: message, code: code}) do
    "[#{kind}] (#{code}) #{message}"
  end
end
