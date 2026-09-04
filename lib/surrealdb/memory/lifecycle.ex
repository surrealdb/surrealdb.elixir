defmodule SurrealDB.Memory.Lifecycle do
  @moduledoc """
  Memory lifecycle operations for a `SurrealDB.Memory` client.
  """

  alias SurrealDB.Memory

  @doc "Expires memory according to the configured policy."
  @spec expire(Memory.t(), keyword()) :: Memory.result()
  def expire(client, opts \\ []),
    do: Memory.post(client, "/lifecycle/expire", Memory.body(opts))

  @doc "Applies decay to memory salience."
  @spec decay(Memory.t(), keyword()) :: Memory.result()
  def decay(client, opts \\ []),
    do: Memory.post(client, "/lifecycle/decay", Memory.body(opts))
end
