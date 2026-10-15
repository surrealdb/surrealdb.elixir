defmodule SurrealDB.RecordId do
  @moduledoc """
  A SurrealDB record identifier in the form `table:id`.

  The `id` part can be a string, integer, list, or map (object id). It is kept as a native
  Elixir term so it round-trips through both the CBOR and JSON protocols without loss.

      iex> SurrealDB.RecordId.new("person", "tobie")
      %SurrealDB.RecordId{table: "person", id: "tobie"}

      iex> SurrealDB.RecordId.parse!("person:tobie")
      %SurrealDB.RecordId{table: "person", id: "tobie"}
  """

  @enforce_keys [:table, :id]
  defstruct [:table, :id]

  @type id :: String.t() | integer() | list() | map()
  @type t :: %__MODULE__{table: String.t(), id: id()}

  @doc "Builds a record id from a table name and an id part."
  @spec new(String.t() | SurrealDB.Table.t(), id()) :: t()
  def new(%SurrealDB.Table{name: table}, id), do: %__MODULE__{table: table, id: id}
  def new(table, id) when is_binary(table), do: %__MODULE__{table: table, id: id}

  @doc """
  Parses a `table:id` string into a record id.

  Plain identifiers, integer keys, and table or key parts quoted with backticks or `⟨⟩`
  are supported, which covers every string and integer key the server prints. For array,
  object, or uuid keys build the struct with `new/2`, or send the string as a
  `SurrealDB.StringRecordId` and let the server parse it.

      iex> SurrealDB.RecordId.parse("person:tobie")
      {:ok, %SurrealDB.RecordId{table: "person", id: "tobie"}}

      iex> SurrealDB.RecordId.parse("person:42")
      {:ok, %SurrealDB.RecordId{table: "person", id: 42}}

      iex> SurrealDB.RecordId.parse("person:`123`")
      {:ok, %SurrealDB.RecordId{table: "person", id: "123"}}

      iex> SurrealDB.RecordId.parse("person")
      {:error, "expected `:` after the table name in \"person\""}
  """
  @spec parse(String.t()) :: {:ok, t()} | {:error, String.t()}
  def parse(string) when is_binary(string) do
    with {:ok, table, rest, _quoted} <- part(string),
         {:ok, rest} <- colon(rest, string),
         {:ok, key, "", quoted} <- part(rest) do
      {:ok, %__MODULE__{table: table, id: key_value(key, quoted)}}
    else
      {:ok, _key, _rest, _quoted} ->
        {:error, "unexpected characters after the key in #{inspect(string)}"}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc "Like `parse/1`, but raises `ArgumentError` when the string is not a record id."
  @spec parse!(String.t()) :: t()
  def parse!(string) do
    case parse(string) do
      {:ok, record_id} -> record_id
      {:error, reason} -> raise ArgumentError, "invalid record id: " <> reason
    end
  end

  defp colon(":" <> rest, _string), do: {:ok, rest}

  defp colon(_rest, string),
    do: {:error, "expected `:` after the table name in #{inspect(string)}"}

  defp part("`" <> rest), do: quoted(rest, "`", [])
  defp part("⟨" <> rest), do: quoted(rest, "⟩", [])

  defp part(string) do
    case Regex.run(~r/^-?[a-zA-Z0-9_]+/, string) do
      [ident] ->
        size = byte_size(ident)
        {:ok, ident, binary_part(string, size, byte_size(string) - size), false}

      nil ->
        {:error, "expected an identifier at #{inspect(string)}"}
    end
  end

  defp quoted("\\" <> <<char::utf8, rest::binary>>, close, acc),
    do: quoted(rest, close, [unescape(char) | acc])

  defp quoted(<<char::utf8, rest::binary>>, close, acc) do
    if <<char::utf8>> == close do
      {:ok, acc |> Enum.reverse() |> IO.iodata_to_binary(), rest, true}
    else
      quoted(rest, close, [<<char::utf8>> | acc])
    end
  end

  defp quoted("", close, _acc), do: {:error, "missing closing #{close}"}

  defp unescape(?n), do: "\n"
  defp unescape(?t), do: "\t"
  defp unescape(?r), do: "\r"
  defp unescape(?0), do: <<0>>
  defp unescape(?f), do: "\f"
  defp unescape(char), do: <<char::utf8>>

  defp key_value(key, true), do: key

  defp key_value(key, false) do
    if Regex.match?(~r/^-?\d+$/, key), do: String.to_integer(key), else: key
  end

  @doc "Returns the canonical `table:id` string representation."
  @spec to_string(t()) :: String.t()
  def to_string(%__MODULE__{table: table, id: id}) do
    "#{escape(table)}:#{format_id(id)}"
  end

  defp format_id(id) when is_binary(id), do: escape(id)
  defp format_id(id) when is_integer(id), do: Integer.to_string(id)
  defp format_id(id), do: inspect(id)

  # SurrealDB identifiers that are not simple alphanumerics are wrapped in backticks.
  defp escape(part) when is_binary(part) do
    if Regex.match?(~r/^[a-zA-Z_][a-zA-Z0-9_]*$/, part) do
      part
    else
      "`" <> String.replace(part, "`", "\\`") <> "`"
    end
  end

  defimpl String.Chars do
    def to_string(record_id), do: SurrealDB.RecordId.to_string(record_id)
  end

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(record_id, _opts) do
      concat(["#SurrealDB.RecordId<", SurrealDB.RecordId.to_string(record_id), ">"])
    end
  end
end
