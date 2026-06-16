defmodule SurrealDB.RPC do
  @moduledoc """
  Helpers for building SurrealDB RPC request frames and interpreting `query` results.

  An RPC request is a map with an `id`, a `method`, and a list of `params`. Responses carry
  the same `id` along with either a `result` or an `error`.
  """

  @doc "Builds an RPC request map."
  @spec request(term(), String.t(), list()) :: map()
  def request(id, method, params) when is_list(params) do
    %{"id" => id, "method" => method, "params" => params}
  end

  @doc """
  Interprets the result of a `query` RPC (a list of per-statement result objects).

  Returns `{:ok, results}` with one entry per statement, or `{:error, error}` for the first
  statement that failed.
  """
  @spec parse_query([map()]) :: {:ok, list()} | {:error, SurrealDB.Error.t()}
  def parse_query(statements) when is_list(statements) do
    Enum.reduce_while(statements, {:ok, []}, fn statement, {:ok, acc} ->
      case statement do
        %{"status" => "OK", "result" => result} ->
          {:cont, {:ok, [result | acc]}}

        %{"status" => "ERR", "result" => message} ->
          {:halt, {:error, SurrealDB.Error.new(:rpc, message, details: statement)}}

        other ->
          {:cont, {:ok, [other | acc]}}
      end
    end)
    |> case do
      {:ok, results} -> {:ok, Enum.reverse(results)}
      error -> error
    end
  end

  @doc "Normalises a live notification action string into a lowercase atom."
  @spec action(String.t() | atom()) :: atom()
  def action(action) when is_atom(action), do: action
  def action(action) when is_binary(action), do: action |> String.downcase() |> String.to_atom()
end
