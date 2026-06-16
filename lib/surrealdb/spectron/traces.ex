defmodule SurrealDB.Spectron.Traces do
  @moduledoc """
  Trace operations for a `SurrealDB.Spectron` client.
  """

  alias SurrealDB.Spectron

  @doc "Lists traces."
  @spec list(Spectron.t(), keyword()) :: Spectron.result()
  def list(client, opts \\ []), do: Spectron.get(client, "/traces", query: opts)

  @doc "Returns a single trace."
  @spec get(Spectron.t(), String.t()) :: Spectron.result()
  def get(client, trace_id), do: Spectron.get(client, "/traces/#{seg(trace_id)}")

  @doc "Returns aggregate trace statistics."
  @spec stats(Spectron.t(), keyword()) :: Spectron.result()
  def stats(client, opts \\ []), do: Spectron.get(client, "/traces/stats", query: opts)

  defp seg(value), do: URI.encode(to_string(value), &URI.char_unreserved?/1)
end
