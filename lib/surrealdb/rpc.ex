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

  Returns `{:ok, values}` with one value per statement when every statement succeeded.
  If any failed, returns `{:error, error}` with `kind: :query`; `error.details.results`
  then holds every statement's outcome, as `statement_results/1` returns them, so the
  results of the statements that did succeed are not lost.
  """
  @spec parse_query([map()]) :: {:ok, list()} | {:error, SurrealDB.Error.t()}
  def parse_query(statements) when is_list(statements) do
    results = statement_results(statements)

    case Enum.with_index(results, 1) |> Enum.filter(&match?({{:error, _}, _}, &1)) do
      [] ->
        {:ok, Enum.map(results, fn {:ok, value} -> value end)}

      [{{:error, first}, index} | _] = failed ->
        message =
          "#{length(failed)} of #{length(results)} statements failed; " <>
            "statement #{index}: #{first.message}"

        {:error, SurrealDB.Error.new(:query, message, details: %{results: results})}
    end
  end

  @doc """
  Turns each per-statement result object into `{:ok, value}` or `{:error, error}`, in
  statement order.
  """
  @spec statement_results([map()]) :: [{:ok, term()} | {:error, SurrealDB.Error.t()}]
  def statement_results(statements) when is_list(statements) do
    Enum.map(statements, &statement_result/1)
  end

  defp statement_result(%{"status" => "OK"} = statement), do: {:ok, statement["result"]}

  defp statement_result(%{"status" => "ERR", "result" => message} = statement) do
    {:error, SurrealDB.Error.new(:rpc, to_string(message), details: statement)}
  end

  defp statement_result(other), do: {:ok, other}

  @doc "Normalises a live notification action string into a lowercase atom."
  @spec action(String.t() | atom()) :: atom()
  def action(action) when is_atom(action), do: action
  def action(action) when is_binary(action), do: action |> String.downcase() |> String.to_atom()
end
