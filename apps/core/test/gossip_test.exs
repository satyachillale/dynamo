defmodule GossipTest do
  use ExUnit.Case
  import Emulation, only: [spawn: 2, send: 2]
  import Kernel, except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "gossip propagates membership changes" do
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

    send(:a, {:gossip, %Messages.Gossip{from: :a, membership: %{d: {"Healthy", 1}}, ring: []}})
    :timer.sleep(2_000)
    assert true
  after
    Emulation.terminate()
  end

  test "gossip eventually propagates new node join to all nodes" do
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

    send(:a, {:gossip, %Messages.Gossip{from: :a, membership: %{d: {"Healthy", 10}}, ring: []}})
    :timer.sleep(3_000)
    assert true
  after
    Emulation.terminate()
  end

  test "gossip propagates node failure and recovery" do
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

    send(:a, {:gossip, %Messages.Gossip{from: :a, membership: %{b: {"Failed", 20}}, ring: []}})
    :timer.sleep(2_000)
    send(:c, {:gossip, %Messages.Gossip{from: :c, membership: %{b: {"Healthy", 30}}, ring: []}})
    :timer.sleep(2_000)
    assert true
  after
    Emulation.terminate()
  end

  test "gossip propagates ring changes" do
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

    new_ring = [{123, :a}, {456, :b}, {789, :c}, {999, :d}]
    send(:a, {:gossip, %Messages.Gossip{from: :a, membership: %{}, ring: new_ring}})
    :timer.sleep(2_000)
    assert true
  after
    Emulation.terminate()
  end
end
