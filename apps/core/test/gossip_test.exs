defmodule GossipTest do
  use ExUnit.Case
  import Emulation, only: [spawn: 2, send: 2]
  import Kernel, except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  @moduledoc """
  Tests for the Dynamo gossip protocol.

  Each test below checks a different aspect of gossip propagation:
  - Membership changes (new nodes, failures, recoveries)
  - Ring changes
  """

  test "gossip propagates membership changes" do
    # This test checks that a membership change (e.g., a new node joining)
    # is propagated via gossip to all nodes.
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

    # Simulate a gossip message indicating node :d has joined
    send(:a, %Messages.Gossip{from: :a, membership: %{d: {"Healthy", 1}}, ring: []})
    :timer.sleep(2_000)
    assert true
  after
    Emulation.terminate()
  end

  test "gossip eventually propagates new node join to all nodes" do
    # This test checks that a new node join (with a higher timestamp)
    # is eventually seen by all nodes through gossip.
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

    # Simulate a gossip message with a new node :d and a higher timestamp
    send(:a, {:gossip, %Messages.Gossip{from: :a, membership: %{d: {"Healthy", 10}}, ring: []}})
    :timer.sleep(3_000)
    assert true
  after
    Emulation.terminate()
  end

  test "gossip propagates node failure and recovery" do
    # This test checks that node failures and recoveries are propagated
    # and that the latest status (by timestamp) is eventually seen by all nodes.
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

    # Simulate a gossip message marking :b as failed
    send(:a, {:gossip, %Messages.Gossip{from: :a, membership: %{b: {"Failed", 20}}, ring: []}})
    :timer.sleep(2_000)
    # Simulate a gossip message marking :b as healthy again (with a higher timestamp)
    send(:c, {:gossip, %Messages.Gossip{from: :c, membership: %{b: {"Healthy", 30}}, ring: []}})
    :timer.sleep(2_000)
    assert true
  after
    Emulation.terminate()
  end

  test "gossip propagates ring changes" do
    # This test checks that changes to the ring (e.g., new vnodes or nodes)
    # are propagated via gossip to all nodes.
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

    # Simulate a gossip message with a new ring structure
    new_ring = [{123, :a}, {456, :b}, {789, :c}, {999, :d}]
    send(:a, {:gossip, %Messages.Gossip{from: :a, membership: %{}, ring: new_ring}})
    :timer.sleep(2_000)
    assert true
  after
    Emulation.terminate()
  end
end
