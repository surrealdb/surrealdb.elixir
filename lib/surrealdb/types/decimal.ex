defmodule SurrealDB.Decimal do
  @moduledoc """
  Helpers for SurrealDB arbitrary precision decimals.

  Decimals are represented with the `Decimal` library's `t:Decimal.t/0` struct. The codec
  encodes them as a decimal string (CBOR tag 10) to preserve precision. This module simply
  re-exports a convenient constructor.

      iex> SurrealDB.Decimal.new("3.14159265358979")
      Decimal.new("3.14159265358979")
  """

  @doc "Builds a `Decimal` from a string, integer, or float."
  @spec new(String.t() | integer() | float() | Decimal.t()) :: Decimal.t()
  def new(value), do: Decimal.new(value)
end
