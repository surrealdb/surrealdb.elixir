defmodule SurrealDB.Memory.Documents do
  @moduledoc """
  Document operations for a `SurrealDB.Memory` client.

  Every function takes the client as its first argument.

      {:ok, doc} = SurrealDB.Memory.Documents.upload(client, title: "Handbook", content: text)
      {:ok, chunks} = SurrealDB.Memory.Documents.chunks(client, doc["id"])
  """

  alias SurrealDB.Memory

  @doc """
  Uploads a document.

  Provide the content with either `:content` (a string) or `:file` (a path that is read and
  base64-encoded). Other options (for example `:title` and `:mime`) are sent alongside.
  """
  @spec upload(Memory.t(), keyword()) :: Memory.result()
  def upload(client, opts) do
    Memory.post(client, "/documents", upload_body(opts))
  end

  @doc "Re-runs ingestion for a document."
  @spec reprocess(Memory.t(), String.t(), keyword()) :: Memory.result()
  def reprocess(client, document_id, opts \\ []) do
    Memory.post(client, "/documents/#{seg(document_id)}/reprocess", Memory.body(opts))
  end

  @doc "Returns a document's metadata."
  @spec get(Memory.t(), String.t()) :: Memory.result()
  def get(client, document_id), do: Memory.get(client, "/documents/#{seg(document_id)}")

  @doc "Returns a document's raw original."
  @spec raw(Memory.t(), String.t()) :: Memory.result()
  def raw(client, document_id), do: Memory.get(client, "/documents/#{seg(document_id)}/raw")

  @doc "Returns a document's chunks."
  @spec chunks(Memory.t(), String.t(), keyword()) :: Memory.result()
  def chunks(client, document_id, opts \\ []) do
    Memory.get(client, "/documents/#{seg(document_id)}/chunks", query: opts)
  end

  @doc "Lists documents."
  @spec list(Memory.t(), keyword()) :: Memory.result()
  def list(client, opts \\ []), do: Memory.get(client, "/documents", query: opts)

  @doc "Deletes a document."
  @spec delete(Memory.t(), String.t()) :: Memory.result()
  def delete(client, document_id), do: Memory.delete(client, "/documents/#{seg(document_id)}")

  @doc "Queries documents."
  @spec query(Memory.t(), String.t(), keyword()) :: Memory.result()
  def query(client, query, opts \\ []) do
    Memory.post(client, "/documents/query", opts |> Memory.body() |> Map.put("query", query))
  end

  @doc "Recomputes the link graph for a document."
  @spec recompute_links(Memory.t(), String.t()) :: Memory.result()
  def recompute_links(client, document_id) do
    Memory.post(client, "/documents/#{seg(document_id)}/recompute-links", %{})
  end

  @doc "Lists the keywords for a document."
  @spec keywords_list(Memory.t(), String.t()) :: Memory.result()
  def keywords_list(client, document_id) do
    Memory.get(client, "/documents/#{seg(document_id)}/keywords")
  end

  @doc "Adds keywords to a document."
  @spec keywords_add(Memory.t(), String.t(), [String.t()]) :: Memory.result()
  def keywords_add(client, document_id, keywords) do
    Memory.post(client, "/documents/#{seg(document_id)}/keywords", %{"keywords" => keywords})
  end

  @doc "Removes a keyword from a document."
  @spec keywords_delete(Memory.t(), String.t(), String.t()) :: Memory.result()
  def keywords_delete(client, document_id, keyword) do
    Memory.delete(client, "/documents/#{seg(document_id)}/keywords/#{seg(keyword)}")
  end

  defp upload_body(opts) do
    {file, opts} = Keyword.pop(opts, :file)
    body = Memory.body(opts)

    case file do
      nil -> body
      path -> Map.put(body, "content_base64", path |> File.read!() |> Base.encode64())
    end
  end

  defp seg(value), do: URI.encode(to_string(value), &URI.char_unreserved?/1)
end
