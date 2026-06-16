defmodule SurrealDB.Spectron.Documents do
  @moduledoc """
  Document operations for a `SurrealDB.Spectron` client.

  Every function takes the client as its first argument.

      {:ok, doc} = SurrealDB.Spectron.Documents.upload(client, title: "Handbook", content: text)
      {:ok, chunks} = SurrealDB.Spectron.Documents.chunks(client, doc["id"])
  """

  alias SurrealDB.Spectron

  @doc """
  Uploads a document.

  Provide the content with either `:content` (a string) or `:file` (a path that is read and
  base64-encoded). Other options (for example `:title` and `:mime`) are sent alongside.
  """
  @spec upload(Spectron.t(), keyword()) :: Spectron.result()
  def upload(client, opts) do
    Spectron.post(client, "/documents", upload_body(opts))
  end

  @doc "Re-runs ingestion for a document."
  @spec reprocess(Spectron.t(), String.t(), keyword()) :: Spectron.result()
  def reprocess(client, document_id, opts \\ []) do
    Spectron.post(client, "/documents/#{seg(document_id)}/reprocess", Spectron.body(opts))
  end

  @doc "Returns a document's metadata."
  @spec get(Spectron.t(), String.t()) :: Spectron.result()
  def get(client, document_id), do: Spectron.get(client, "/documents/#{seg(document_id)}")

  @doc "Returns a document's raw original."
  @spec raw(Spectron.t(), String.t()) :: Spectron.result()
  def raw(client, document_id), do: Spectron.get(client, "/documents/#{seg(document_id)}/raw")

  @doc "Returns a document's chunks."
  @spec chunks(Spectron.t(), String.t(), keyword()) :: Spectron.result()
  def chunks(client, document_id, opts \\ []) do
    Spectron.get(client, "/documents/#{seg(document_id)}/chunks", query: opts)
  end

  @doc "Lists documents."
  @spec list(Spectron.t(), keyword()) :: Spectron.result()
  def list(client, opts \\ []), do: Spectron.get(client, "/documents", query: opts)

  @doc "Deletes a document."
  @spec delete(Spectron.t(), String.t()) :: Spectron.result()
  def delete(client, document_id), do: Spectron.delete(client, "/documents/#{seg(document_id)}")

  @doc "Queries documents."
  @spec query(Spectron.t(), String.t(), keyword()) :: Spectron.result()
  def query(client, query, opts \\ []) do
    Spectron.post(client, "/documents/query", opts |> Spectron.body() |> Map.put("query", query))
  end

  @doc "Recomputes the link graph for a document."
  @spec recompute_links(Spectron.t(), String.t()) :: Spectron.result()
  def recompute_links(client, document_id) do
    Spectron.post(client, "/documents/#{seg(document_id)}/recompute-links", %{})
  end

  @doc "Lists the keywords for a document."
  @spec keywords_list(Spectron.t(), String.t()) :: Spectron.result()
  def keywords_list(client, document_id) do
    Spectron.get(client, "/documents/#{seg(document_id)}/keywords")
  end

  @doc "Adds keywords to a document."
  @spec keywords_add(Spectron.t(), String.t(), [String.t()]) :: Spectron.result()
  def keywords_add(client, document_id, keywords) do
    Spectron.post(client, "/documents/#{seg(document_id)}/keywords", %{"keywords" => keywords})
  end

  @doc "Removes a keyword from a document."
  @spec keywords_delete(Spectron.t(), String.t(), String.t()) :: Spectron.result()
  def keywords_delete(client, document_id, keyword) do
    Spectron.delete(client, "/documents/#{seg(document_id)}/keywords/#{seg(keyword)}")
  end

  defp upload_body(opts) do
    {file, opts} = Keyword.pop(opts, :file)
    body = Spectron.body(opts)

    case file do
      nil -> body
      path -> Map.put(body, "content_base64", path |> File.read!() |> Base.encode64())
    end
  end

  defp seg(value), do: URI.encode(to_string(value), &URI.char_unreserved?/1)
end
