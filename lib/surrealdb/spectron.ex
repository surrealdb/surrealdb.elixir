defmodule SurrealDB.Spectron do
  @moduledoc """
  Client for the [Spectron](https://surrealdb.com/platform/spectron) memory API.

  Spectron is a typed REST service for agent memory. The client is a lightweight struct
  pinned to a single context; create one with `new/1` and pass it to every call:

      client =
        SurrealDB.Spectron.new(
          endpoint: System.fetch_env!("SPECTRON_ENDPOINT"),
          context: "acme-prod",
          api_key: System.fetch_env!("SPECTRON_API_KEY")
        )

      {:ok, _facts} = SurrealDB.Spectron.remember(client, "I just got promoted to CTO", scopes: "user/tobie")
      {:ok, hits} = SurrealDB.Spectron.recall(client, "What is Tobie's role?", k: 10)
      {:ok, %{"reply" => reply}} = SurrealDB.Spectron.chat(client, "What do you know about me?")

  ## Namespaces

  Grouped operations live in dedicated modules, each taking the client as the first
  argument: `SurrealDB.Spectron.Documents`, `SurrealDB.Spectron.Entities`,
  `SurrealDB.Spectron.Sessions`, `SurrealDB.Spectron.Lifecycle`,
  `SurrealDB.Spectron.Traces`, `SurrealDB.Spectron.Principals`,
  `SurrealDB.Spectron.Scopes`, and `SurrealDB.Spectron.Keys`.

  ## Delegation

  `on_behalf_of/2` returns a new client whose requests carry the
  `X-Spectron-On-Behalf-Of` header, so calls run with that principal's authorisation. The
  original client is left unchanged.
  """

  alias SurrealDB.Spectron.{Stream, Transport}

  @enforce_keys [:endpoint, :context, :api_key]
  defstruct [
    :endpoint,
    :context,
    :api_key,
    on_behalf_of: nil,
    timeout: 30_000,
    transport_opts: %{},
    retry: %{max_attempts: 3, base_ms: 200}
  ]

  @type t :: %__MODULE__{
          endpoint: URI.t(),
          context: String.t(),
          api_key: String.t(),
          on_behalf_of: String.t() | nil,
          timeout: timeout(),
          transport_opts: map(),
          retry: map() | false
        }

  @type result :: {:ok, term()} | {:error, SurrealDB.Spectron.Error.t()}

  @doc """
  Builds a client.

  ## Options

    * `:endpoint` (required) - base URL of the Spectron API.
    * `:context` (required) - the context this client is pinned to.
    * `:api_key` (required) - API key sent as a Bearer token.
    * `:timeout` - per-request timeout in milliseconds (default 30000).
    * `:retry` - `%{max_attempts: integer, base_ms: integer}` or `false` to disable.
    * `:transport_opts` - extra transport options (for example `%{tls: [...]}`).
  """
  @spec new(keyword()) :: t()
  def new(opts) do
    %__MODULE__{
      endpoint: opts |> Keyword.fetch!(:endpoint) |> URI.parse(),
      context: Keyword.fetch!(opts, :context),
      api_key: Keyword.fetch!(opts, :api_key),
      timeout: Keyword.get(opts, :timeout, 30_000),
      retry: Keyword.get(opts, :retry, %{max_attempts: 3, base_ms: 200}),
      transport_opts: Keyword.get(opts, :transport_opts, %{}) |> Map.new()
    }
  end

  @doc "Returns a new client that acts on behalf of the given principal."
  @spec on_behalf_of(t(), String.t()) :: t()
  def on_behalf_of(%__MODULE__{} = client, principal_id) do
    %{client | on_behalf_of: principal_id}
  end

  # ── Memory operations ────────────────────────────────────────────────────────

  @doc "Persists facts from free text and/or caller-supplied options. Idempotent."
  @spec remember(t(), String.t() | nil, keyword()) :: result()
  def remember(client, text \\ nil, opts \\ []) do
    body = opts |> body() |> put_unless_nil("text", text)
    post(client, "/remember", body)
  end

  @doc "Persists a batch of conversation messages (`[%{role: ..., content: ...}, ...]`)."
  @spec remember_many(t(), [map()], keyword()) :: result()
  def remember_many(client, messages, opts \\ []) do
    body = opts |> body() |> Map.put("messages", messages)
    post(client, "/remember/batch", body)
  end

  @doc "Recalls relevant facts for a query using hybrid search."
  @spec recall(t(), String.t(), keyword()) :: result()
  def recall(client, query, opts \\ []) do
    post(client, "/recall", query_body(query, opts))
  end

  @doc "Returns the working memory and active topics relevant to a query."
  @spec context(t(), String.t(), keyword()) :: result()
  def context(client, query, opts \\ []) do
    post(client, "/context", query_body(query, opts))
  end

  @doc "Synthesises new durable memory from retrieved context."
  @spec reflect(t(), String.t(), keyword()) :: result()
  def reflect(client, query, opts \\ []) do
    post(client, "/reflect", query_body(query, opts))
  end

  @doc "Removes facts matching a query, optionally hard-purging with `purge: true`."
  @spec forget(t(), String.t(), keyword()) :: result()
  def forget(client, query, opts \\ []) do
    post(client, "/forget", query_body(query, opts))
  end

  @doc """
  Chats with the server-driven memory loop.

  With `stream: true`, returns `{:ok, stream}` where `stream` is a lazy `Stream` of decoded
  chunk maps. Otherwise returns `{:ok, response}` with the full reply.
  """
  @spec chat(t(), String.t(), keyword()) :: result() | {:ok, Enumerable.t()}
  def chat(client, message, opts \\ []) do
    {stream?, opts} = Keyword.pop(opts, :stream, false)
    body = opts |> body() |> Map.put("message", message)

    if stream? do
      path = context_path(client, "/chat")
      headers = stream_headers(client, path, Jason.encode!(body))
      {:ok, Stream.start(client, path, Jason.encode!(body), headers)}
    else
      post(client, "/chat", body)
    end
  end

  @doc "Returns a snapshot of the substrate state."
  @spec state(t()) :: result()
  def state(client), do: get(client, "/state")

  @doc "Returns the memory profile for the context."
  @spec profile(t()) :: result()
  def profile(client), do: get(client, "/profile")

  @doc "Returns information about the authenticated principal."
  @spec whoami(t()) :: result()
  def whoami(client), do: request(client, "GET", "/whoami")

  @doc "Runs a consolidation pass. Pass `dry_run: true` to preview."
  @spec consolidate(t(), keyword()) :: result()
  def consolidate(client, opts \\ []), do: post(client, "/consolidate", body(opts))

  @doc "Elaborates on an entity. Pass `entity_ref: \"person:tobie\"`."
  @spec elaborate(t(), keyword()) :: result()
  def elaborate(client, opts \\ []), do: post(client, "/elaborate", body(opts))

  @doc "Runs a filesystem-check style integrity pass over the substrate."
  @spec fsck(t(), keyword()) :: result()
  def fsck(client, opts \\ []), do: post(client, "/fsck", body(opts))

  @doc "Inspects substrate state for a reference."
  @spec inspect(t(), String.t(), keyword()) :: result()
  def inspect(client, ref, opts \\ []) do
    post(client, "/inspect", opts |> body() |> Map.put("ref", ref))
  end

  @doc "Returns audit trail entries. Pass `limit: 50` to bound the result."
  @spec audit(t(), keyword()) :: result()
  def audit(client, opts \\ []), do: get(client, "/audit", query: opts)

  # ── Path and body helpers (shared with namespace modules) ────────────────────

  @doc false
  def context_path(%__MODULE__{context: context}, suffix) do
    "/contexts/" <> URI.encode(context) <> suffix
  end

  @doc false
  def get(client, suffix, opts \\ []) do
    request(client, "GET", context_path(client, suffix), opts)
  end

  @doc false
  def post(client, suffix, body, opts \\ []) do
    request(client, "POST", context_path(client, suffix), Keyword.put(opts, :body, body))
  end

  @doc false
  def delete(client, suffix, opts \\ []) do
    request(client, "DELETE", context_path(client, suffix), opts)
  end

  @doc false
  def request(client, method, path, opts \\ []) do
    Transport.request(client, method, path, opts)
  end

  @doc false
  def body(opts) when is_list(opts) or is_map(opts) do
    Map.new(opts, fn
      {key, value} when is_atom(key) -> {Atom.to_string(key), value}
      {key, value} -> {key, value}
    end)
  end

  defp query_body(query, opts), do: opts |> body() |> Map.put("query", query)

  defp put_unless_nil(map, _key, nil), do: map
  defp put_unless_nil(map, key, value), do: Map.put(map, key, value)

  defp stream_headers(client, path, body) do
    digest = :crypto.hash(:sha256, ["POST ", path, " ", body]) |> Base.encode16(case: :lower)

    [
      {"authorization", "Bearer #{client.api_key}"},
      {"accept", "text/event-stream"},
      {"content-type", "application/json"},
      {"idempotency-key", digest}
    ]
    |> then(fn headers ->
      case client.on_behalf_of do
        nil -> headers
        principal -> [{"x-spectron-on-behalf-of", principal} | headers]
      end
    end)
  end
end
