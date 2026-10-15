defmodule SurrealDB.Test.TCPProxy do
  @moduledoc """
  A TCP proxy for integration tests that can drop every connection through it on demand,
  to simulate a network failure between the SDK and a real server.

      {:ok, proxy} = TCPProxy.start_link(upstream: {~c"127.0.0.1", 8000})
      url = "ws://127.0.0.1:\#{TCPProxy.port(proxy)}/rpc"
      TCPProxy.cut(proxy)      # close every open connection
      TCPProxy.pause(proxy)    # also refuse new ones until resume/1
  """

  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @doc "The local port clients should connect to."
  def port(proxy), do: GenServer.call(proxy, :port)

  @doc "Closes every connection currently open through the proxy."
  def cut(proxy), do: GenServer.call(proxy, :cut)

  @doc "Closes every connection and rejects new ones until `resume/1`."
  def pause(proxy), do: GenServer.call(proxy, :pause)

  @doc "Accepts connections again after `pause/1`."
  def resume(proxy), do: GenServer.call(proxy, :resume)

  @doc "How many client connections the proxy has accepted."
  def accepted(proxy), do: GenServer.call(proxy, :accepted)

  @impl true
  def init(opts) do
    {host, port} = Keyword.fetch!(opts, :upstream)

    {:ok, listen} =
      :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, ip: {127, 0, 0, 1}])

    {:ok, local_port} = :inet.port(listen)
    proxy = self()
    acceptor = spawn_link(fn -> accept_loop(listen, proxy) end)

    {:ok,
     %{
       listen: listen,
       port: local_port,
       upstream: {host, port},
       acceptor: acceptor,
       pairs: [],
       paused: false,
       accepted: 0
     }}
  end

  @impl true
  def handle_call(:port, _from, state), do: {:reply, state.port, state}
  def handle_call(:accepted, _from, state), do: {:reply, state.accepted, state}

  def handle_call(:cut, _from, state), do: {:reply, :ok, close_all(state)}

  def handle_call(:pause, _from, state),
    do: {:reply, :ok, %{close_all(state) | paused: true}}

  def handle_call(:resume, _from, state), do: {:reply, :ok, %{state | paused: false}}

  def handle_call({:accepted, client}, _from, %{paused: true} = state) do
    :gen_tcp.close(client)
    {:reply, :closed, state}
  end

  def handle_call({:accepted, client}, _from, state) do
    {host, port} = state.upstream

    case :gen_tcp.connect(host, port, [:binary, active: false]) do
      {:ok, upstream} ->
        pump_a = spawn(fn -> pump(client, upstream) end)
        pump_b = spawn(fn -> pump(upstream, client) end)
        :ok = :gen_tcp.controlling_process(client, pump_a)
        :ok = :gen_tcp.controlling_process(upstream, pump_b)
        send(pump_a, :go)
        send(pump_b, :go)
        pair = {client, upstream}
        {:reply, :ok, %{state | pairs: [pair | state.pairs], accepted: state.accepted + 1}}

      {:error, _} ->
        :gen_tcp.close(client)
        {:reply, :closed, state}
    end
  end

  defp close_all(state) do
    Enum.each(state.pairs, fn {a, b} ->
      :gen_tcp.shutdown(a, :read_write)
      :gen_tcp.shutdown(b, :read_write)
      :gen_tcp.close(a)
      :gen_tcp.close(b)
    end)

    %{state | pairs: []}
  end

  defp accept_loop(listen, proxy) do
    case :gen_tcp.accept(listen) do
      {:ok, client} ->
        :ok = :gen_tcp.controlling_process(client, proxy)
        GenServer.call(proxy, {:accepted, client})
        accept_loop(listen, proxy)

      {:error, :closed} ->
        :ok
    end
  end

  defp pump(from, to) do
    receive do
      :go -> pump_loop(from, to)
    end
  end

  defp pump_loop(from, to) do
    case :gen_tcp.recv(from, 0) do
      {:ok, data} ->
        case :gen_tcp.send(to, data) do
          :ok -> pump_loop(from, to)
          {:error, _} -> :gen_tcp.close(from)
        end

      {:error, _} ->
        :gen_tcp.close(to)
    end
  end
end
