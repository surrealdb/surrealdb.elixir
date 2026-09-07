defmodule SurrealDB.Memory.Traces do
  @moduledoc """
  Trace operations for a `SurrealDB.Memory` client.
  """

  alias SurrealDB.Memory

  @doc "Lists traces."
  @spec list(Memory.t(), keyword()) :: Memory.result()
  def list(client, opts \\ []), do: Memory.get(client, "/traces", query: opts)

  @doc "Returns a single trace."
  @spec get(Memory.t(), String.t()) :: Memory.result()
  def get(client, trace_id), do: Memory.get(client, "/traces/#{seg(trace_id)}")

  @doc "Returns aggregate trace statistics."
  @spec stats(Memory.t(), keyword()) :: Memory.result()
  def stats(client, opts \\ []), do: Memory.get(client, "/traces/stats", query: opts)

  defp seg(value), do: URI.encode(to_string(value), &URI.char_unreserved?/1)
end
