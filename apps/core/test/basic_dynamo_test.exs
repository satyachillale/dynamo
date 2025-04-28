defmodule BasicDynamoTest do
  use ExUnit.Case
  import Emulation, only: [spawn: 2, send: 2]
  import Kernel, except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "basic put and get request" do
    Emulation.init()
    nodes = [:a, :b, :c]
    Enum.each(nodes, fn n -> spawn(n, fn -> Core.make_server(%{config | node: n}) end) end)

    client = Dynamo.Client.new_client(:a)
    {:ok, client} = Dynamo.Client.put(client, "k", "v")
    {:ok, values, _client} = Dynamo.Client.get(client, "k")
    assert "v" in values

    Emulation.terminate()
  end

  test "multiple keys and overwrites" do
    Emulation.init()
    nodes = [:a, :b, :c]
    clients = [:client]
    config = Core.new_configuration(nodes, 3, clients, 3, 2, 2, :a)
    Enum.each(nodes, fn n -> spawn(n, fn -> Core.make_server(%{config | node: n}) end) end)

    client =
      spawn(:client, fn ->
        # Put key1
        send(
          :a,
          {:client,
           %Messages.ClientPutRequest{key: "k1", value: "v1", client: :client, context: %{}}}
        )

        receive do
          %Messages.ClientPutResponse{key: "k1", status: :ok} -> :ok
        after
          2_000 -> flunk("Put k1 did not succeed")
        end

        # Put key2
        send(
          :b,
          {:client,
           %Messages.ClientPutRequest{key: "k2", value: "v2", client: :client, context: %{}}}
        )

        receive do
          %Messages.ClientPutResponse{key: "k2", status: :ok} -> :ok
        after
          2_000 -> flunk("Put k2 did not succeed")
        end

        # Overwrite key1
        send(
          :c,
          {:client,
           %Messages.ClientPutRequest{key: "k1", value: "v1b", client: :client, context: %{}}}
        )

        receive do
          %Messages.ClientPutResponse{key: "k1", status: :ok} -> :ok
        after
          2_000 -> flunk("Overwrite k1 did not succeed")
        end

        # Get key1 (should see latest value)
        send(:a, {:client, %Messages.ClientGetRequest{key: "k1", client: :client}})

        receive do
          %Messages.ClientGetResponse{key: "k1", values: values} ->
            assert "v1b" in values
        after
          2_000 -> flunk("Get k1 did not succeed")
        end

        # Get key2
        send(:b, {:client, %Messages.ClientGetRequest{key: "k2", client: :client}})

        receive do
          %Messages.ClientGetResponse{key: "k2", values: ["v2"]} -> :ok
        after
          2_000 -> flunk("Get k2 did not succeed")
        end
      end)

    handle = Process.monitor(client)

    receive do
      {:DOWN, ^handle, _, _, _} -> :ok
    after
      10_000 -> flunk("Client did not finish")
    end
  after
    Emulation.terminate()
  end

  test "concurrent clients and concurrent puts" do
    Emulation.init()
    nodes = [:a, :b, :c]
    clients = [:client1, :client2]
    config = Core.new_configuration(nodes, 3, clients, 3, 2, 2, :a)
    Enum.each(nodes, fn n -> spawn(n, fn -> Core.make_server(%{config | node: n}) end) end)

    client1 = Dynamo.Client.new_client(:a)
    client2 = Dynamo.Client.new_client(:b)
    Task.async(fn -> Dynamo.Client.put(client1, "k", "v1") end)
    Task.async(fn -> Dynamo.Client.put(client2, "k", "v2") end)

    # Now get the key and check for both versions (conflict)
    send(:a, {:client, %Messages.ClientGetRequest{key: "k", client: :main}})

    receive do
      %Messages.ClientGetResponse{key: "k", values: values} ->
        assert Enum.any?(values, &(&1 == "v1")) or Enum.any?(values, &(&1 == "v2"))
    after
      2_000 -> flunk("Get did not succeed")
    end
  after
    Emulation.terminate()
  end

  test "client put and get using Dynamo.Client" do
    client = Dynamo.Client.new_client(:a)
    {:ok, client} = Dynamo.Client.put(client, "k1", "v1")
    {:ok, values, _client} = Dynamo.Client.get(client, "k1")
    assert "v1" in values
  end
end
