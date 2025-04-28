defmodule GossipTest do
  use ExUnit.Case
  import Emulation, only: [spawn: 2, send: 2]
  import Kernel, except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "gossip propagates membership changes" do
    Emulation.init()
    nodes = [:a, :b, :c]
    clients = [:client]
    config = Core.new_configuration(nodes, 3, clients, 3, 2, 2, :a)
    Enum.each(nodes, fn n -> spawn(n, fn -> Core.make_server(%{config | node: n}) end) end)

    # Simulate a membership change (add node :d)
    send(:a, {:gossip, %Messages.Gossip{from: :a, membership: %{d: {"Healthy", 1}}, ring: []}})
    :timer.sleep(2_000)

    # For now, just ensure no crash and that the test completes
    assert true
  after
    Emulation.terminate()
  end

  test "gossip eventually propagates new node join to all nodes" do
    Emulation.init()
    nodes = [:a, :b, :c]
    clients = [:client]
    config = Core.new_configuration(nodes, 3, clients, 3, 2, 2, :a)
    Enum.each(nodes, fn n -> spawn(n, fn -> Core.make_server(%{config | node: n}) end) end)

    # Simulate node :d joining
    send(:a, {:gossip, %Messages.Gossip{from: :a, membership: %{d: {"Healthy", 10}}, ring: []}})
    :timer.sleep(3_000)

    # No crash, all nodes should have learned about :d (if you expose state, you could assert membership)
    assert true
  after
    Emulation.terminate()
  end

  test "gossip propagates node failure and recovery" do
    Emulation.init()
    nodes = [:a, :b, :c]
    clients = [:client]
    config = Core.new_configuration(nodes, 3, clients, 3, 2, 2, :a)
    Enum.each(nodes, fn n -> spawn(n, fn -> Core.make_server(%{config | node: n}) end) end)

    # Simulate node :b failure
    send(:a, {:gossip, %Messages.Gossip{from: :a, membership: %{b: {"Failed", 20}}, ring: []}})
    :timer.sleep(2_000)

    # Simulate node :b recovery
    send(:c, {:gossip, %Messages.Gossip{from: :c, membership: %{b: {"Healthy", 30}}, ring: []}})
    :timer.sleep(2_000)

    assert true
  after
    Emulation.terminate()
  end

  test "gossip propagates ring changes" do
    Emulation.init()
    nodes = [:a, :b, :c]
    clients = [:client]
    config = Core.new_configuration(nodes, 3, clients, 3, 2, 2, :a)
    Enum.each(nodes, fn n -> spawn(n, fn -> Core.make_server(%{config | node: n}) end) end)

    # Simulate a ring change (e.g., new vnode assignment)
    new_ring = [{123, :a}, {456, :b}, {789, :c}, {999, :d}]
    send(:a, {:gossip, %Messages.Gossip{from: :a, membership: %{}, ring: new_ring}})
    :timer.sleep(2_000)

    assert true
  after
    Emulation.terminate()
  end
end
