defmodule SurrealDB.FileRef do
  @moduledoc """
  A reference to a file in a SurrealDB bucket, written `f"bucket:/key"` in SurrealQL.

  Over CBOR it is tag 55 holding `[bucket, key]`.

      iex> SurrealDB.FileRef.new("avatars", "/tobie.png")
      %SurrealDB.FileRef{bucket: "avatars", key: "/tobie.png"}

  Files are an experimental server feature: the server must run with
  `--allow-experimental files`.
  """

  @enforce_keys [:bucket, :key]
  defstruct [:bucket, :key]

  @type t :: %__MODULE__{bucket: String.t(), key: String.t()}

  @doc "Builds a file reference. A key without a leading `/` gets one."
  @spec new(String.t(), String.t()) :: t()
  def new(bucket, key) when is_binary(bucket) and is_binary(key) do
    key = if String.starts_with?(key, "/"), do: key, else: "/" <> key
    %__MODULE__{bucket: bucket, key: key}
  end

  @doc "The `bucket:/key` form of the reference."
  @spec to_string(t()) :: String.t()
  def to_string(%__MODULE__{bucket: bucket, key: key}), do: bucket <> ":" <> key

  defimpl String.Chars do
    def to_string(file), do: SurrealDB.FileRef.to_string(file)
  end

  defimpl Inspect do
    def inspect(file, _opts),
      do: "#SurrealDB.FileRef<" <> SurrealDB.FileRef.to_string(file) <> ">"
  end
end
