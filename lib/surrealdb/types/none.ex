defmodule SurrealDB.None do
  @moduledoc """
  The SurrealDB `NONE` value (CBOR tag 6).

  `NONE` is distinct from `NULL`: `NULL` is represented as the Elixir `nil`, while `NONE`
  means "no value at all". Use the `none/0` singleton when you need to send it.
  """

  defstruct []

  @type t :: %__MODULE__{}

  @doc "Returns the `NONE` singleton."
  @spec none() :: t()
  def none, do: %__MODULE__{}

  defimpl Inspect do
    def inspect(_none, _opts), do: "#SurrealDB.None<>"
  end
end
