defmodule SurrealDB.RPC do
  @moduledoc """
  Helpers for building SurrealDB RPC request frames and interpreting `query` results.

  An RPC request is a map with an `id`, a `method`, and a list of `params`. Responses carry
  the same `id` along with either a `result` or an `error`.
  """

  @doc """
  Builds an RPC request map. A `session` or `txn` id, when given, runs the request in that
  session or transaction.
  """
  @spec request(term(), String.t(), list(), String.t() | nil, String.t() | nil) :: map()
  def request(id, method, params, session \\ nil, txn \\ nil) when is_list(params) do
    %{"id" => id, "method" => method, "params" => params}
    |> put_uuid("session", session)
    |> put_uuid("txn", txn)
  end

  defp put_uuid(request, _key, nil), do: request
  defp put_uuid(request, key, id), do: Map.put(request, key, SurrealDB.Uuid.new(id))

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
    {:error, SurrealDB.Error.from_statement(%{statement | "result" => message})}
  end

  defp statement_result(other), do: {:ok, other}

  @doc """
  Normalises a live notification action into a lowercase atom. An action the SDK does not
  know stays a lowercase string, so server input never creates atoms.
  """
  @spec action(String.t() | atom()) :: atom() | String.t()
  def action(action) when is_atom(action), do: action

  def action(action) when is_binary(action) do
    case String.downcase(action) do
      "create" -> :create
      "update" -> :update
      "delete" -> :delete
      "killed" -> :killed
      other -> other
    end
  end

  @doc """
  Translates a `query_stream` frame into the tuple `SurrealDB.stream_query/4` delivers, or
  `nil` for a frame kind this SDK does not know (which the protocol says to ignore).
  """
  @spec stream_frame(map()) :: tuple() | nil
  def stream_frame(%{"stream" => "begin"} = frame),
    do: {:begin, %{statements: frame["statements"], version: frame["version"] || 1}}

  def stream_frame(%{"stream" => "rows", "index" => index, "values" => values}),
    do: {:rows, index, values}

  def stream_frame(%{"stream" => "value", "index" => index} = frame),
    do: {:value, index, Map.get(frame, "value", SurrealDB.None.none())}

  def stream_frame(%{"stream" => "finished", "index" => index} = frame) do
    {:finished, index,
     %{
       time: frame["time"],
       type: frame["type"],
       single: frame["single"] == true,
       error: frame_error(frame["error"])
     }}
  end

  def stream_frame(%{"stream" => "end"} = frame),
    do:
      {:end,
       %{results: frame["results"], time: frame["time"], error: frame_error(frame["error"])}}

  def stream_frame(_frame), do: nil

  defp frame_error(%{} = error), do: SurrealDB.Error.from_rpc(error)
  defp frame_error(_none), do: nil
end
