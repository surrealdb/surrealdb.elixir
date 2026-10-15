defmodule SurrealDB.Version do
  @moduledoc """
  Server version parsing and the range of server versions this SDK supports.

  The SDK accepts any server from `2.1.0` up to, but not including, `5.0.0`.
  Pre-release and build metadata are ignored for the range check, so `3.4.0-nightly` counts
  as `3.4.0` and `5.0.0-alpha.1` is rejected.

      iex> SurrealDB.Version.parse("surrealdb-3.4.0-nightly+20261014.4f7fa16")
      {:ok, {3, 4, 0}}

      iex> SurrealDB.Version.supported?("surrealdb-3.3.4")
      true

      iex> SurrealDB.Version.supported?("surrealdb-5.0.0")
      false
  """

  @minimum {2, 1, 0}
  @maximum {5, 0, 0}

  @doc "The lowest supported server version (inclusive)."
  @spec minimum() :: String.t()
  def minimum, do: format(@minimum)

  @doc "The first unsupported server version (exclusive)."
  @spec maximum() :: String.t()
  def maximum, do: format(@maximum)

  @doc """
  Parses a version string as returned by the `version` RPC (`"surrealdb-3.3.4"`) or the
  `/version` endpoint, into `{major, minor, patch}`.
  """
  @spec parse(String.t()) ::
          {:ok, {non_neg_integer(), non_neg_integer(), non_neg_integer()}} | :error
  def parse(string) when is_binary(string) do
    case Regex.run(~r/(\d+)\.(\d+)\.(\d+)/, string) do
      [_, major, minor, patch] ->
        {:ok, {String.to_integer(major), String.to_integer(minor), String.to_integer(patch)}}

      nil ->
        :error
    end
  end

  @doc "Whether the server version is in the supported range."
  @spec supported?(String.t()) :: boolean()
  def supported?(string), do: check(string) == :ok

  @doc "Returns `:ok` for a supported version, or an `:unsupported` error explaining why not."
  @spec check(String.t()) :: :ok | {:error, SurrealDB.Error.t()}
  def check(string) when is_binary(string) do
    case parse(string) do
      {:ok, version} when version >= @minimum and version < @maximum ->
        :ok

      {:ok, _version} ->
        {:error,
         SurrealDB.Error.new(
           :unsupported,
           "server version #{string} is not supported; this SDK supports " <>
             ">= #{minimum()} and < #{maximum()}. Pass `version_check: false` to connect anyway.",
           details: string
         )}

      :error ->
        message = "could not parse the server version #{inspect(string)}"
        {:error, SurrealDB.Error.new(:unsupported, message, details: string)}
    end
  end

  @doc "Whether `version` (`{major, minor, patch}`) is at least `minimum`."
  @spec at_least?(term(), {integer(), integer(), integer()}) :: boolean()
  def at_least?({_, _, _} = version, minimum), do: version >= minimum
  def at_least?(_version, _minimum), do: false

  defp format({major, minor, patch}), do: "#{major}.#{minor}.#{patch}"
end
