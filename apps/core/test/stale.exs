defmodule StaleDataTest do
  use ExUnit.Case
  import Emulation, only: [spawn: 2]
  alias Dynamo.Client

  @num_clients 10
  @num_gets 30

  # test "percentage of stale reads with delay and drop fuzzers" do
  #   Emulation.init()
  #   Emulation.append_fuzzers([Fuzzers.drop(0.0), Fuzzers.delay(0.0)])

  #   # 4 nodes
  #   nodes = Enum.map(1..4, &:"n#{&1}")
  #   vnodes = 10
  #   clients = Enum.map(1..@num_clients, &:"client#{&1}")
  #   replication_factor = 4
  #   read_quorum = 1
  #   write_quorum = 1

  #   config =
  #     Core.new_configuration(
  #       nodes,
  #       vnodes,
  #       clients,
  #       replication_factor,
  #       read_quorum,
  #       write_quorum,
  #       :n1
  #     )

  #   Enum.each(nodes, fn n -> spawn(n, fn -> Core.make_server(%{config | node: n}) end) end)

  #   key = "k"
  #   values = Enum.map(1..@num_clients, &"v#{&1}")

  #   # Each client writes a different value to the same key
  #   Enum.zip(clients, values)
  #   |> Enum.each(fn {client_name, value} ->
  #     spawn(client_name, fn ->
  #       coordinator = Enum.random(nodes)
  #       client = Client.new_client(coordinator)
  #       Client.put(client, key, value)
  #     end)
  #   end)

  #   # Wait for 10 seconds before issuing GETs
  #   :timer.sleep(10_000)

  #   latest_value = List.last(values)
  #   parent = self()

  #   Enum.each(1..@num_gets, fn i ->
  #     spawn(:"getter#{i}", fn ->
  #       coordinator = Enum.random(nodes)
  #       client = Client.new_client(coordinator)
  #       {msg, results} = Client.get(client, key)

  #       seen_latest =
  #         case msg do
  #           :ok -> Enum.any?(results, fn v -> v.value == latest_value end)
  #           _ -> false
  #         end

  #       send(parent, {:get_result, seen_latest})
  #     end)
  #   end)

  #   results =
  #     Enum.map(1..@num_gets, fn _ ->
  #       receive do
  #         {:get_result, seen_latest} -> seen_latest
  #       after
  #         10_000 -> false
  #       end
  #     end)

  #   stale_reads = Enum.count(results, fn seen_latest -> not seen_latest end)
  #   percent_stale = stale_reads * 100 / @num_gets

  #   IO.puts("Stale reads: #{stale_reads}/#{@num_gets} (#{percent_stale}%)")

  #   assert percent_stale >= 0

  #   Emulation.terminate()
  # end

  test "percentage of stale reads with one put per server" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.drop(0.0), Fuzzers.delay(0.0)])

    nodes = Enum.map(1..4, &:"n#{&1}")
    vnodes = 10
    clients = Enum.map(1..@num_clients, &:"client#{&1}")
    replication_factor = 4
    read_quorum = 1
    write_quorum = 4

    config =
      Core.new_configuration(
        nodes,
        vnodes,
        clients,
        replication_factor,
        read_quorum,
        write_quorum,
        :n1
      )

    Enum.each(nodes, fn n -> spawn(n, fn -> Core.make_server(%{config | node: n}) end) end)

    key = "k"
    values = Enum.map(1..length(nodes), &"v#{&1}")
    latest_value = List.last(values)
    parent = self()

    # Send one put per server, concurrently
    Enum.zip(nodes, values)
    |> Enum.each(fn {node, value} ->
      spawn(:"putter_#{node}", fn ->
        client = Client.new_client(node)
        Client.put(client, key, value)
      end)
    end)

    # Wait for 10 seconds before issuing GETs
    :timer.sleep(10_000)

    # Send one get per server, concurrently
    Enum.each(nodes, fn node ->
      spawn(:"getter_#{node}", fn ->
        client = Client.new_client(node)
        {msg, results} = Client.get(client, key)

        seen_latest =
          case msg do
            :ok -> Enum.any?(results, fn v -> v.value == latest_value end)
            _ -> false
          end

        send(parent, {:get_result, node, seen_latest})
      end)
    end)

    results =
      Enum.map(nodes, fn node ->
        receive do
          {:get_result, ^node, seen_latest} -> seen_latest
        after
          10_000 -> false
        end
      end)

    stale_reads = Enum.count(results, fn seen_latest -> not seen_latest end)
    percent_stale = stale_reads * 100 / length(nodes)

    IO.puts("Stale reads: #{stale_reads}/#{length(nodes)} (#{percent_stale}%)")

    assert percent_stale >= 0

    Emulation.terminate()
  end
end
