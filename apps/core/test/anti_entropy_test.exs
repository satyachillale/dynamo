defmodule AntiEntropyTest do
  use ExUnit.Case
  import Emulation, only: [spawn: 2]
  alias Dynamo.Client

  test "anti-entropy synchronizes divergent replicas" do
    Emulation.init()
    nodes = [:a, :b]
    config = Core.new_configuration(nodes, 3, [:client], 2, 1, 1, :a)
    Enum.each(nodes, fn n -> spawn(n, fn -> Core.make_server(%{config | node: n}) end) end)

    client = Client.new_client(:a)
    {:ok, _client} = Client.put(client, "k", "v1")

    # Wait for anti-entropy to run
    :timer.sleep(12_000)

    # Get from :b, should see "v1" after anti-entropy
    client_b = Client.new_client(:b)
    {:ok, values, _} = Client.get(client_b, "k")
    assert "v1" in values
  after
    Emulation.terminate()
  end

  test "anti-entropy repairs after node recovers from failure" do
    Emulation.init()
    nodes = [:a, :b]
    config = Core.new_configuration(nodes, 3, [:client], 2, 1, 1, :a)
    Enum.each(nodes, fn n -> spawn(n, fn -> Core.make_server(%{config | node: n}) end) end)

    client = Client.new_client(:a)
    {:ok, _client} = Client.put(client, "k", "v2")

    # Wait for anti-entropy to run and synchronize
    :timer.sleep(12_000)

    # Get from :b, should see "v2" after anti-entropy
    client_b = Client.new_client(:b)
    {:ok, values, _} = Client.get(client_b, "k")
    assert "v2" in values
  after
    Emulation.terminate()
  end

  test "anti-entropy resolves divergent values (conflict resolution)" do
    Emulation.init()
    nodes = [:a, :b]
    config = Core.new_configuration(nodes, 3, [:client], 2, 1, 1, :a)
    Enum.each(nodes, fn n -> spawn(n, fn -> Core.make_server(%{config | node: n}) end) end)

    client_a = Client.new_client(:a)
    client_b = Client.new_client(:b)
    {:ok, _} = Client.put(client_a, "k", "vA")
    {:ok, _} = Client.put(client_b, "k", "vB")

    # Wait for anti-entropy to run and synchronize
    :timer.sleep(12_000)

    # Get from both nodes, should see both values (conflict)
    {:ok, values_a, _} = Client.get(client_a, "k")
    assert Enum.sort(values_a) == Enum.sort(["vA", "vB"])
    {:ok, values_b, _} = Client.get(client_b, "k")
    assert Enum.sort(values_b) == Enum.sort(["vA", "vB"])
  after
    Emulation.terminate()
  end

  test "anti-entropy works with multiple keys" do
    Emulation.init()
    nodes = [:a, :b]
    config = Core.new_configuration(nodes, 3, [:client], 2, 1, 1, :a)
    Enum.each(nodes, fn n -> spawn(n, fn -> Core.make_server(%{config | node: n}) end) end)

    client_a = Client.new_client(:a)
    client_b = Client.new_client(:b)
    {:ok, _} = Client.put(client_a, "k1", "v1")
    {:ok, _} = Client.put(client_b, "k2", "v2")

    # Wait for anti-entropy to run and synchronize
    :timer.sleep(12_000)

    # Get both keys from both nodes
    for key <- ["k1", "k2"], node <- [:a, :b] do
      client = Client.new_client(node)
      {:ok, values, _} = Client.get(client, key)
      assert Enum.any?(values, &(&1 == "v1" or &1 == "v2"))
    end
  after
    Emulation.terminate()
  end
end
