defmodule SurrealDB.Spectron.Lifecycle do
  @moduledoc """
  Memory lifecycle operations for a `SurrealDB.Spectron` client.
  """

  alias SurrealDB.Spectron

  @doc "Expires memory according to the configured policy."
  @spec expire(Spectron.t(), keyword()) :: Spectron.result()
  def expire(client, opts \\ []),
    do: Spectron.post(client, "/lifecycle/expire", Spectron.body(opts))

  @doc "Applies decay to memory salience."
  @spec decay(Spectron.t(), keyword()) :: Spectron.result()
  def decay(client, opts \\ []),
    do: Spectron.post(client, "/lifecycle/decay", Spectron.body(opts))
end
