defmodule BasicDynamoTest do
  use ExUnit.Case
  import Emulation, only: [spawn: 2]
  alias Dynamo.Client

  test "basic put and get request" do
    Emulation.init()
    nodes = [:a, :b, :c]
    vnodes = 3
    clients = [:client]
    replication_factor = 3
    read_quorum = 2
    write_quorum = 2

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

    client = Client.new_client(:a)
    {:ok, client} = Client.put(client, "k", "v")
    {:ok, values, _client} = Client.get(client, "k")
    assert "v" in values

    Emulation.terminate()
  end

  test "multiple keys and overwrites" do
    Emulation.init()
    nodes = [:a, :b, :c]
    vnodes = 3
    clients = [:client]
    replication_factor = 3
    read_quorum = 2
    write_quorum = 2

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

    client = Client.new_client(:a)
    # Put key1
    {:ok, _} = Client.put(client, "k1", "v1")
    # Put key2
    {:ok, _} = Client.put(client, "k2", "v2")
    # Overwrite key1
    {:ok, _} = Client.put(client, "k1", "v1b")
    # Get key1 (should see latest value)
    {:ok, values1, _} = Client.get(client, "k1")
    assert "v1b" in values1
    # Get key2
    {:ok, values2, _} = Client.get(client, "k2")
    assert "v2" in values2

    Emulation.terminate()
  end

  test "concurrent clients and concurrent puts" do
    Emulation.init()
    nodes = [:a, :b, :c]
    vnodes = 3
    clients = [:client1, :client2]
    replication_factor = 3
    read_quorum = 2
    write_quorum = 2

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

    client1 = Client.new_client(:a)
    client2 = Client.new_client(:b)
    t1 = Task.async(fn -> Client.put(client1, "k", "v1") end)
    t2 = Task.async(fn -> Client.put(client2, "k", "v2") end)
    Task.await(t1, 5_000)
    Task.await(t2, 5_000)

    # Now get the key and check for both versions (conflict)
    {:ok, values, _} = Client.get(client1, "k")
    assert Enum.any?(values, &(&1 == "v1")) or Enum.any?(values, &(&1 == "v2"))

    Emulation.terminate()
  end

  test "client put and get using Dynamo.Client" do
    Emulation.init()
    nodes = [:a, :b, :c]
    vnodes = 3
    clients = [:client]
    replication_factor = 3
    read_quorum = 2
    write_quorum = 2

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

    client = Client.new_client(:a)
    {:ok, client} = Client.put(client, "k1", "v1")
    {:ok, values, _client} = Client.get(client, "k1")
    assert "v1" in values

    Emulation.terminate()
  end
end
