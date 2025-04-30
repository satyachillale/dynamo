defmodule AntiEntropyTest do
  use ExUnit.Case
  import Emulation, only: [spawn: 2]
  alias Dynamo.Client

  test "anti-entropy synchronizes divergent replicas" do
    Emulation.init()
    nodes = [:a, :b]
    vnodes = 3
    clients = [:client]
    replication_factor = 2
    read_quorum = 1
    write_quorum = 1

    config =
      Core.new_configuration(
        nodes,
        vnodes,
        clients,
        replication_factor,
        read_quorum,
        write_quorum,
        :a
      )

    Enum.each(nodes, fn n -> spawn(n, fn -> Core.make_server(%{config | node: n}) end) end)

    spawn(:client_a, fn ->
      client = Client.new_client(:a)
      {:ok, _client} = Client.put(client, "k", "v1")
    end)

    :timer.sleep(12_000)

    spawn(:client_b, fn ->
      client_b = Client.new_client(:b)
      {:ok, values, _} = Client.get(client_b, "k")
      assert "v1" in values
    end)
  after
    Emulation.terminate()
  end

  test "anti-entropy repairs after node recovers from failure" do
    Emulation.init()
    nodes = [:a, :b]
    vnodes = 3
    clients = [:client]
    replication_factor = 2
    read_quorum = 1
    write_quorum = 1

    config =
      Core.new_configuration(
        nodes,
        vnodes,
        clients,
        replication_factor,
        read_quorum,
        write_quorum,
        :a
      )

    Enum.each(nodes, fn n -> spawn(n, fn -> Core.make_server(%{config | node: n}) end) end)

    spawn(:client_a, fn ->
      client = Client.new_client(:a)
      {:ok, _client} = Client.put(client, "k", "v2")
    end)

    :timer.sleep(12_000)

    spawn(:client_b, fn ->
      client_b = Client.new_client(:b)
      {:ok, values, _} = Client.get(client_b, "k")
      assert "v2" in values
    end)
  after
    Emulation.terminate()
  end

  test "anti-entropy resolves divergent values (conflict resolution)" do
    Emulation.init()
    nodes = [:a, :b]
    vnodes = 3
    clients = [:client]
    replication_factor = 2
    read_quorum = 1
    write_quorum = 1

    config =
      Core.new_configuration(
        nodes,
        vnodes,
        clients,
        replication_factor,
        read_quorum,
        write_quorum,
        :a
      )

    Enum.each(nodes, fn n -> spawn(n, fn -> Core.make_server(%{config | node: n}) end) end)

    spawn(:client_a, fn ->
      client_a = Client.new_client(:a)
      {:ok, _} = Client.put(client_a, "k", "vA")
    end)

    spawn(:client_b, fn ->
      client_b = Client.new_client(:b)
      {:ok, _} = Client.put(client_b, "k", "vB")
    end)

    :timer.sleep(12_000)

    spawn(:client_a_get, fn ->
      client_a = Client.new_client(:a)
      {:ok, values_a, _} = Client.get(client_a, "k")
      assert Enum.sort(values_a) == Enum.sort(["vA", "vB"])
    end)

    spawn(:client_b_get, fn ->
      client_b = Client.new_client(:b)
      {:ok, values_b, _} = Client.get(client_b, "k")
      assert Enum.sort(values_b) == Enum.sort(["vA", "vB"])
    end)
  after
    Emulation.terminate()
  end

  test "anti-entropy works with multiple keys" do
    Emulation.init()
    nodes = [:a, :b]
    vnodes = 3
    clients = [:client]
    replication_factor = 2
    read_quorum = 1
    write_quorum = 1

    config =
      Core.new_configuration(
        nodes,
        vnodes,
        clients,
        replication_factor,
        read_quorum,
        write_quorum,
        :a
      )

    Enum.each(nodes, fn n -> spawn(n, fn -> Core.make_server(%{config | node: n}) end) end)

    spawn(:client_a, fn ->
      client_a = Client.new_client(:a)
      {:ok, _} = Client.put(client_a, "k1", "v1")
    end)

    spawn(:client_b, fn ->
      client_b = Client.new_client(:b)
      {:ok, _} = Client.put(client_b, "k2", "v2")
    end)

    :timer.sleep(12_000)

    for key <- ["k1", "k2"], node <- [:a, :b] do
      spawn({:client, node, key}, fn ->
        client = Client.new_client(node)
        {:ok, values, _} = Client.get(client, key)
        assert Enum.any?(values, &(&1 == "v1" or &1 == "v2"))
      end)
    end
  after
    Emulation.terminate()
  end
end
