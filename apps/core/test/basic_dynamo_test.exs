defmodule BasicDynamoTest do
  use ExUnit.Case
  import Emulation, only: [spawn: 2]
  alias Dynamo.Client

  test "basic put and get request" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.drop(0.05), Fuzzers.delay(2.0)])

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

    client_proc =
      spawn(:client, fn ->
        client = Client.new_client(:a)
        {msg, client} = Client.put(client, "k", "v")
        assert msg == :ok
        {:ok, values} = Client.get(client, "k")
        assert Enum.any?(values, fn v -> v.value == "v" end)
      end)

    handle = Process.monitor(client_proc)

    receive do
      {:DOWN, ^handle, _, _, _} -> true
    after
      30_000 -> assert false
    end

    Emulation.terminate()
  end

  test "multiple keys and overwrites" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.drop(0.05), Fuzzers.delay(2.0)])
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

    client_proc =
      spawn(:client, fn ->
        client = Client.new_client(:a)
        # Put key1
        {msg, _} = Client.put(client, "k1", "v1")
        assert msg == :ok
        # Put key2
        {msg, _} = Client.put(client, "k2", "v2")
        assert msg == :ok
        # Overwrite key1 with correct context
        {:ok, values1} = Client.get(client, "k1")
        IO.inspect(values1, label: "values1")
        # Assume single version for simplicity
        assert Enum.any?(values1, fn v -> v.value == "v1" end)
        vc1 = Enum.at(values1, 0).vector_clock
        {msg, _} = Client.put(client, "k1", "v1b", vc1)
        assert msg == :ok
        # Get key1 (should see latest value)
        {:ok, values1b} = Client.get(client, "k1")
        assert Enum.any?(values1b, fn v -> v.value == "v1b" end)
        # Get key2
        {:ok, values2} = Client.get(client, "k2")
        assert Enum.any?(values2, fn v -> v.value == "v2" end)
      end)

    handle = Process.monitor(client_proc)

    receive do
      {:DOWN, ^handle, _, _, _} -> true
    after
      30_000 -> assert false
    end

    Emulation.terminate()
  end

  test "concurrent clients and concurrent puts" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.drop(0.05), Fuzzers.delay(2.0)])
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

    client1_proc =
      spawn(:client1, fn ->
        client1 = Client.new_client(:a)
        IO.puts("[DEBUG] client1 starting PUT for k=v1")
        {msg, _} = Client.put(client1, "k", "v1")
        IO.puts("[DEBUG] client1 finished PUT for k=v1 with msg=#{inspect(msg)}")
        assert msg == :ok
      end)

    client2_proc =
      spawn(:client2, fn ->
        client2 = Client.new_client(:b)
        IO.puts("[DEBUG] client2 starting PUT for k=v2")
        {msg, _} = Client.put(client2, "k", "v2")
        IO.puts("[DEBUG] client2 finished PUT for k=v2 with msg=#{inspect(msg)}")
        assert msg == :ok
      end)

    # Wait for both clients to finish
    handle1 = Process.monitor(client1_proc)
    handle2 = Process.monitor(client2_proc)

    for handle <- [handle1, handle2] do
      receive do
        {:DOWN, ^handle, _, _, _} -> true
      after
        30_000 -> assert false
      end
    end

    # Now get the key and check for both versions (conflict)
    client_proc =
      spawn(:client_get, fn ->
        client = Client.new_client(:a)
        {msg, values} = Client.get(client, "k")
        assert(msg == :ok)
        v1_present = Enum.any?(values, fn v -> v.value == "v1" end)
        v2_present = Enum.any?(values, fn v -> v.value == "v2" end)

        assert (v1_present and not v2_present) or
                 (not v1_present and v2_present) or
                 (v1_present and v2_present)
      end)

    handle = Process.monitor(client_proc)

    receive do
      {:DOWN, ^handle, _, _, _} -> true
    after
      30_000 -> assert false
    end

    Emulation.terminate()
  end
end
