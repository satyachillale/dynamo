defmodule Core do
  import Emulation, only: [send: 2, whoami: 0]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  @enforce_keys [
    :nodes,
    :vnodes,
    :clients,
    :read_quorum,
    :write_quorum,
    :replication_factor,
    :ring
  ]
  # State struct for each Dynamo node
  defstruct(
    # This node's name
    node: nil,
    # List of all nodes
    nodes: nil,
    # Number of virtual nodes per physical node
    vnodes: nil,
    # List of clients
    clients: nil,
    # N: number of replicas for each key
    replication_factor: nil,
    # R: read quorum
    read_quorum: nil,
    # W: write quorum
    write_quorum: nil,
    # Consistent hash ring (list of {hash, node})
    ring: nil,
    # Key-value store: key => [%{value, vector_clock}]
    kv_store: %{},
    # Sequence number (not always used)
    seq: 0,
    # key => number of responses received for current op
    response_count: %{},
    # key => list of responses for current op
    responses: %{},
    # vnode => keys (for partition transfer)
    vnodeToKeys: %{},
    # node => {status, timestamp}
    status_of_nodes: %{},
    # {node, key} => timer ref for request timeouts
    requestTimerMap: %{},
    # For client request timeouts (if needed)
    clientRequestTimerMap: %{},
    # node => [PutRequest] for hinted handoff
    hintedHandedOffMap: %{},
    # Is this node failed?
    inFailedState: false,
    # node => timestamp when marked failed
    failed_nodes: %{},
    # node => {status, timestamp}
    membership_history: %{},
    # key => value
    pending_puts: %{},
    # key => client
    client_map: %{},
    sent_nodes: %{}
  )

  @doc """
  Creates a new configuration/state struct for a Dynamo server.
  """
  @spec new_configuration(
          nodes :: list(),
          vnodes :: non_neg_integer(),
          clients :: list(),
          replication_factor :: non_neg_integer(),
          read_quorum :: non_neg_integer(),
          write_quorum :: non_neg_integer(),
          node_name :: any()
        ) :: %Core{}
  def new_configuration(
        nodes,
        vnodes,
        clients,
        replication_factor,
        read_quorum,
        write_quorum,
        node_name
      ) do
    ring = build_ring(nodes, vnodes)

    %__MODULE__{
      node: node_name,
      nodes: nodes,
      vnodes: vnodes,
      clients: clients,
      replication_factor: replication_factor,
      read_quorum: read_quorum,
      write_quorum: write_quorum,
      ring: ring,
      kv_store: %{},
      seq: 0,
      response_count: %{},
      responses: %{},
      vnodeToKeys: %{},
      status_of_nodes: Enum.into(nodes, %{}, fn n -> {n, {"Healthy", 0}} end),
      requestTimerMap: %{},
      clientRequestTimerMap: %{},
      hintedHandedOffMap: %{},
      inFailedState: false,
      failed_nodes: %{},
      membership_history: Enum.into(nodes, %{}, fn n -> {n, {"Healthy", 0}} end),
      pending_puts: %{},
      client_map: %{}
    }
  end

  # Build the consistent hash ring with vnodes for all nodes
  def build_ring(nodes, vnodes) do
    nodes
    |> Enum.flat_map(fn node ->
      for v <- 1..vnodes, do: {hash_node("#{node}-#{v}"), node}
    end)
    |> Enum.sort_by(fn {hash, _node} -> hash end)
  end

  # Hash function for consistent hashing
  defp hash_node(str) do
    :crypto.hash(:sha256, str) |> :binary.decode_unsigned()
  end

  # Returns the N distinct physical nodes responsible for a key (preference list)
  def preference_list(key, ring, n) do
    key_hash = hash_node(key)
    ring_size = length(ring)
    IO.puts("[preference_list] key=#{inspect(key)}, key_hash=#{inspect(key_hash)}, n=#{n}")

    # Find the index of the first vnode >= key_hash
    start_idx =
      Enum.with_index(ring)
      |> Enum.find(fn {{hash, _node}, _idx} -> hash >= key_hash end)
      |> case do
        nil ->
          IO.puts("[preference_list] No vnode >= key_hash, wrapping to index 0")
          0

        {_, idx} ->
          IO.puts("[preference_list] Found start_idx=#{idx}")
          idx
      end

    # Walk the ring once, collecting unique nodes
    result =
      0..(ring_size - 1)
      |> Enum.map(fn i ->
        idx = rem(start_idx + i, ring_size)
        node = elem(Enum.at(ring, idx), 1)
        IO.puts("[preference_list] Considering node=#{inspect(node)} at idx=#{idx}")
        node
      end)
      |> Enum.uniq()
      |> Enum.take(n)

    IO.puts("[preference_list] Selected nodes: #{inspect(result)}")
    result
  end

  # Returns the first N healthy nodes for sloppy quorum (skips failed nodes)
  def sloppy_quorum_nodes(key, ring, n, failed_nodes) do
    key_hash = hash_node(key)
    ring_size = length(ring)
    IO.puts("[sloppy_quorum_nodes] key=#{inspect(key)}, key_hash=#{inspect(key_hash)}, n=#{n}")

    IO.puts(
      "[sloppy_quorum_nodes] ring_size=#{ring_size}, failed_nodes=#{inspect(Map.keys(failed_nodes))}"
    )

    {_, start_idx} =
      Enum.with_index(ring)
      |> Enum.find(fn {{hash, _node}, _idx} -> hash >= key_hash end) ||
        {List.first(ring), 0}

    IO.puts("[sloppy_quorum_nodes] start_idx=#{inspect(start_idx)}")

    result =
      Stream.cycle(0..(ring_size - 1))
      |> Stream.drop(start_idx)
      |> Stream.map(fn idx ->
        node = elem(Enum.at(ring, rem(idx, ring_size)), 1)
        IO.puts("[sloppy_quorum_nodes] Considering node=#{inspect(node)} at idx=#{idx}")
        node
      end)
      |> Stream.uniq()
      |> Stream.reject(fn node ->
        failed = Map.has_key?(failed_nodes, node)
        if failed, do: IO.puts("[sloppy_quorum_nodes] Skipping failed node=#{inspect(node)}")
        failed
      end)
      |> Enum.take(n)

    IO.puts("[sloppy_quorum_nodes] Selected nodes: #{inspect(result)}")
    result
  end

  # Starts the server process for this node
  @spec make_server(%Core{}) :: no_return()
  def make_server(state) do
    _anti_entropy_timer = Emulation.timer(25, :anti_entropy)
    _gossip_timer = Emulation.timer(50, :gossip)

    now = Emulation.now()

    state = %{
      state
      | status_of_nodes: Map.put(state.status_of_nodes, whoami(), {"Healthy", now}),
        node: whoami()
    }

    server(state)
  end

  # Schedules periodic gossip
  defp schedule_gossip do
    Process.send_after(self(), :gossip, 1_000)
  end

  # Schedules periodic retry of failed nodes
  defp schedule_retry_failed do
    Process.send_after(self(), :retry_failed, 5_000)
  end

  # Schedules periodic anti-entropy (Merkle tree exchange)
  defp schedule_anti_entropy do
    Process.send_after(self(), :anti_entropy, 10_000)
  end

  # Main server loop: handles all messages (client and server)
  def server(state) do
    receive do
      # --- Client to Server: Put ---
      {_sender,
       %Messages.ClientPutRequest{key: key, value: value, client: client, context: context}} ->
        IO.puts(
          "[#{state.node}] [PUT] Received ClientPutRequest for key=#{inspect(key)}, value=#{inspect(value)}, client=#{inspect(client)}, context=#{inspect(context)}"
        )

        # Use sloppy quorum to select N healthy nodes for replication
        pref_nodes =
          sloppy_quorum_nodes(key, state.ring, state.replication_factor, state.failed_nodes)

        IO.puts("[#{state.node}] [PUT] Sloppy quorum nodes selected: #{inspect(pref_nodes)}")

        req = %Messages.ReplicaPutRequest{
          key: key,
          value: value,
          from: state.node,
          vector_clock: context
        }

        # Track which nodes we actually sent to (for write repair)
        sent_nodes =
          Enum.reduce(pref_nodes, [], fn node, acc ->
            if Map.has_key?(state.failed_nodes, node) do
              IO.puts(
                "[#{state.node}] [PUT] Node #{inspect(node)} is failed, storing hint for hinted handoff"
              )

              hints = Map.get(state.hintedHandedOffMap, node, [])

              state = %{
                state
                | hintedHandedOffMap: Map.put(state.hintedHandedOffMap, node, hints ++ [req])
              }

              acc
            else
              IO.puts("[#{state.node}] [PUT] Sending ReplicaPutRequest to node #{inspect(node)}")
              send(node, req)
              Process.send_after(self(), {:request_timeout, node, key}, 500)
              [node | acc]
            end
          end)

        # Track client and value for this key
        state =
          %{
            state
            | client_map: Map.put(state.client_map, {key, :put}, client),
              response_count: Map.put(state.response_count, {key, :put}, 0),
              responses: Map.put(state.responses, {key, :put}, []),
              pending_puts: Map.put(state.pending_puts, key, value),
              sent_nodes: Map.put(state.sent_nodes || %{}, key, sent_nodes)
          }

        IO.puts(
          "[#{state.node}] [PUT] State updated for key=#{inspect(key)}; waiting for replica responses."
        )

        server(state)

      # --- Client to Server: Get ---
      {sender, %Messages.ClientGetRequest{key: key, client: client}} ->
        IO.puts(
          "[#{state.node}] [GET] Received ClientGetRequest for key=#{inspect(key)}, client=#{inspect(client)}"
        )

        # Coordinator receives client get, sends ReplicaGetRequest to N replicas
        pref_nodes = preference_list(key, state.ring, state.replication_factor)

        IO.puts(
          "[#{state.node}] [GET] Preference list for key=#{inspect(key)}: #{inspect(pref_nodes)}"
        )

        req = %Messages.ReplicaGetRequest{key: key, from: state.node}

        Enum.each(pref_nodes, fn node ->
          IO.puts("[#{state.node}] [GET] Sending ReplicaGetRequest to node #{inspect(node)}")
          send(node, req)
          ref = Process.send_after(self(), {:request_timeout, node, key}, 500)
        end)

        # Track client for response
        state = %{
          state
          | client_map: Map.put(state.client_map, {key, :get}, client),
            response_count: Map.put(state.response_count, {key, :get}, 0),
            responses: Map.put(state.responses, {key, :get}, [])
        }

        IO.puts(
          "[#{state.node}] [GET] State updated for key=#{inspect(key)}; waiting for replica responses."
        )

        IO.puts("[DEBUG] client_map after GET request: #{inspect(state.client_map)}")

        server(state)

      # --- Server to Server: Replica Put ---
      {sender, %Messages.ReplicaPutRequest{key: key, value: value, from: from, vector_clock: vc}} ->
        IO.puts(
          "[#{state.node}] [REPLICA PUT] Received ReplicaPutRequest for key=#{inspect(key)}, value=#{inspect(value)}, from=#{inspect(from)}, vector_clock=#{inspect(vc)}"
        )

        # Replica applies write and responds with updated vector clock
        versions = Map.get(state.kv_store, key, [])
        new_vc = VectorClock.increment(VectorClock.merge(vc, latest_vc(versions)), state.node)
        new_version = %{value: value, vector_clock: new_vc}

        updated_versions =
          [new_version | versions]
          |> Enum.uniq_by(& &1.vector_clock)
          |> Enum.reject(fn v ->
            Enum.any?([new_version], fn nv ->
              VectorClock.compare(v.vector_clock, nv.vector_clock) == :before
            end)
          end)

        new_store = Map.put(state.kv_store, key, updated_versions)

        IO.puts(
          "[#{state.node}] [REPLICA PUT] Updated kv_store for key=#{inspect(key)}. Sending ReplicaPutResponse to #{inspect(from)} with vector_clock=#{inspect(new_vc)}"
        )

        send(from, %Messages.ReplicaPutResponse{
          key: key,
          status: :ok,
          to: from,
          vector_clock: new_vc
        })

        server(%{state | kv_store: new_store})

      # --- Server to Server: Replica Get ---
      {sender, %Messages.ReplicaGetRequest{key: key, from: from}} ->
        IO.puts(
          "[#{state.node}] [REPLICA GET] Received ReplicaGetRequest for key=#{inspect(key)} from #{inspect(from)}"
        )

        # Replica responds with all versions for the key
        versions = Map.get(state.kv_store, key, [])
        values = Enum.map(versions, & &1.value)
        vcs = Enum.map(versions, & &1.vector_clock)

        IO.puts(
          "[#{state.node}] [REPLICA GET] Sending ReplicaGetResponse to #{inspect(from)} with values=#{inspect(values)} and vector_clocks=#{inspect(vcs)}"
        )

        send(from, %Messages.ReplicaGetResponse{
          key: key,
          values: values,
          vector_clocks: vcs,
          to: from,
          node: state.node
        })

        server(state)

      # --- Replica Responses to Coordinator: Put ---
      {_sender, %Messages.ReplicaPutResponse{key: key, to: _to, vector_clock: vc}} ->
        IO.puts(
          "[#{state.node}] [PUT] Received ReplicaPutResponse for key=#{inspect(key)}, vector_clock=#{inspect(vc)}"
        )

        # Collect responses, and reply to client after write quorum is reached
        count = Map.get(state.response_count, {key, :put}, 0) + 1
        value = Map.get(state.pending_puts, key)

        responses =
          Map.get(state.responses, {key, :put}, []) ++ [%{vector_clock: vc, value: value}]

        IO.puts(
          "[#{state.node}] [PUT] Write responses for key=#{inspect(key)}: count=#{count}/#{state.write_quorum}"
        )

        state = %{
          state
          | response_count: Map.put(state.response_count, {key, :put}, count),
            responses: Map.put(state.responses, {key, :put}, responses)
        }

        if count >= state.write_quorum do
          client = Map.get(state.client_map, {key, :put})

          if client do
            IO.puts(
              "[#{state.node}] [PUT] Write quorum achieved for key=#{inspect(key)}. Sending ClientPutResponse to client #{inspect(client)}"
            )

            send(client, %Messages.ClientPutResponse{
              key: key,
              status: :ok,
              vector_clock: vc
            })
          end

          # Write repair: send the latest version to all preference nodes
          pref_nodes = preference_list(key, state.ring, state.replication_factor)

          IO.puts(
            "[#{state.node}] [PUT] Performing write repair for key=#{inspect(key)} to nodes: #{inspect(pref_nodes)}"
          )

          Enum.each(pref_nodes, fn node ->
            Enum.each(responses, fn %{vector_clock: vclock, value: val} ->
              if val != nil do
                IO.puts(
                  "[#{state.node}] [PUT] Write repair: sending ReplicaPutRequest to #{inspect(node)} with value=#{inspect(val)}, vector_clock=#{inspect(vclock)}"
                )

                send(node, %Messages.ReplicaPutRequest{
                  key: key,
                  value: val,
                  from: state.node,
                  vector_clock: vclock
                })
              end
            end)
          end)

          # Clean up state for this key
          state = %{
            state
            | response_count: Map.delete(state.response_count, {key, :put}),
              responses: Map.delete(state.responses, {key, :put}),
              client_map: Map.delete(state.client_map, {key, :put}),
              pending_puts: Map.delete(state.pending_puts, key)
          }

          IO.puts(
            "[#{state.node}] [PUT] State cleaned up for key=#{inspect(key)} after write quorum."
          )

          server(state)
        else
          server(state)
        end

      # --- Replica Responses to Coordinator: Get ---
      {sender,
       %Messages.ReplicaGetResponse{key: key, values: values, vector_clocks: vcs, node: node}} ->
        IO.puts(
          "[#{state.node}] [GET] Received ReplicaGetResponse for key=#{inspect(key)} from node=#{inspect(node)} with values=#{inspect(values)} and vector_clocks=#{inspect(vcs)}"
        )

        # Collect responses, and reply to client after read quorum is reached
        count = Map.get(state.response_count, {key, :get}, 0) + 1

        responses =
          Map.get(state.responses, {key, :get}, []) ++
            [%{values: values, vector_clocks: vcs, node: node}]

        IO.puts(
          "[#{state.node}] [GET] Read responses for key=#{inspect(key)}: count=#{count}/#{state.read_quorum}"
        )

        state = %{
          state
          | response_count: Map.put(state.response_count, {key, :get}, count),
            responses: Map.put(state.responses, {key, :get}, responses)
        }

        if count >= state.read_quorum do
          client = Map.get(state.client_map, {key, :get})
          merged = merge_versions(responses)

          IO.puts(
            "[#{state.node}] [GET] Read quorum achieved for key=#{inspect(key)}. Sending ClientGetResponse to client #{inspect(client)} with merged values=#{inspect(Enum.map(merged, & &1.value))}"
          )

          if client,
            do:
              send(client, %Messages.ClientGetResponse{
                key: key,
                values: merged
              })

          # --- Read Repair ---
          pref_nodes = preference_list(key, state.ring, state.replication_factor)

          IO.puts(
            "[#{state.node}] [GET] Performing read repair for key=#{inspect(key)} to nodes: #{inspect(pref_nodes)}"
          )

          Enum.each(pref_nodes, fn node ->
            node_resp = Enum.find(responses, fn r -> r.node == node end)
            node_vcs = if node_resp, do: node_resp.vector_clocks, else: []

            missing =
              merged -- Enum.map(node_vcs, fn vc -> %{value: nil, vector_clock: vc} end)

            Enum.each(missing, fn %{value: value, vector_clock: vc} ->
              IO.puts(
                "[#{state.node}] [GET] Read repair: sending ReplicaPutRequest to #{inspect(node)} with value=#{inspect(value)}, vector_clock=#{inspect(vc)}"
              )

              send(node, %Messages.ReplicaPutRequest{
                key: key,
                value: value,
                from: state.node,
                vector_clock: vc
              })
            end)
          end)

          # --- End Read Repair ---

          state = %{
            state
            | response_count: Map.delete(state.response_count, {key, :get}),
              responses: Map.delete(state.responses, {key, :get}),
              client_map: Map.delete(state.client_map, {key, :get}),
              pending_puts: Map.delete(state.pending_puts, key)
          }

          IO.puts(
            "[#{state.node}] [GET] State cleaned up for key=#{inspect(key)} after read quorum."
          )

          server(state)
        else
          server(state)
        end

      # --- Gossip Protocol: Membership and Ring Exchange ---
      %Messages.Gossip{from: from, membership: remote_membership, ring: remote_ring} ->
        handle_gossip(
          %Messages.Gossip{from: from, membership: remote_membership, ring: remote_ring},
          state
        )

      # --- Anti-Entropy: Merkle Tree Exchange ---
      %Messages.MerkleTreeExchange{from: from, tree: remote_tree} ->
        handle_merkle_exchange(%Messages.MerkleTreeExchange{from: from, tree: remote_tree}, state)

      # --- Periodic Tasks ---
      :gossip ->
        handle_info(:gossip, state)

      :anti_entropy ->
        handle_info(:anti_entropy, state)

      # Periodically retry failed nodes
      :retry_failed ->
        Enum.each(Map.keys(state.failed_nodes), fn node ->
          send(node, {:ping, state.node})
        end)

        schedule_retry_failed()
        server(state)

      # Handle ping/pong for failure detection
      {:ping, from} ->
        send(from, {:pong, state.node})
        server(state)

      {:pong, node} ->
        state = %{state | failed_nodes: Map.delete(state.failed_nodes, node)}
        {hints, new_hints} = Map.pop(state.hintedHandedOffMap, node, [])
        Enum.each(hints, fn req -> send(node, req) end)
        state = %{state | hintedHandedOffMap: new_hints}
        server(state)

      # Timeout handler for marking nodes as failed
      {:request_timeout, node, key} ->
        now = :os.system_time(:millisecond)
        state = %{state | failed_nodes: Map.put(state.failed_nodes, node, now)}
        server(state)
    end
  end

  # Handles periodic gossip: sends membership and ring info to a random peer
  def handle_info(:gossip, state) do
    peers = Enum.filter(state.nodes, fn n -> n != state.node end)

    if peers != [] do
      peer = Enum.random(peers)

      send(peer, %Messages.Gossip{
        from: state.node,
        membership: state.membership_history,
        ring: state.ring
      })
    end

    schedule_gossip()
    server(state)
  end

  # Handles periodic anti-entropy: sends Merkle tree root to a random peer
  def handle_info(:anti_entropy, state) do
    peers = Enum.filter(state.nodes, fn n -> n != state.node end)

    if peers != [] do
      peer = Enum.random(peers)
      tree = MerkleTree.build(state.kv_store)
      send(peer, %Messages.MerkleTreeExchange{from: state.node, tree: tree})
    end

    schedule_anti_entropy()
    server(state)
  end

  # Handles incoming gossip: merges membership and ring info
  def handle_gossip(
        %Messages.Gossip{from: from, membership: remote_membership, ring: remote_ring},
        state
      ) do
    # Merge membership histories (keep latest timestamp for each node)
    merged_membership =
      Map.merge(state.membership_history, remote_membership, fn _node,
                                                                {status1, ts1},
                                                                {status2, ts2} ->
        if ts1 >= ts2, do: {status1, ts1}, else: {status2, ts2}
      end)

    # For ring, you may want to reconcile based on membership or just union for now
    merged_ring = Enum.uniq(state.ring ++ remote_ring)

    new_state = %{state | membership_history: merged_membership, ring: merged_ring}
    server(new_state)
  end

  # Handles incoming Merkle tree exchange: triggers sync if roots differ
  def handle_merkle_exchange(%Messages.MerkleTreeExchange{from: from, tree: remote_tree}, state) do
    local_tree = MerkleTree.build(state.kv_store)

    if not MerkleTree.equal?(local_tree, remote_tree) do
      IO.puts("[#{state.node}] Merkle root mismatch with #{from}, triggering sync")
      # In a real system, you'd walk the tree to find differing subtrees/keys.
      # For simplicity, you could send your full kv_store or request theirs.
      # Example: send(from, {:request_full_sync, state.node})
    end

    server(state)
  end

  # Returns the latest vector clock from a list of versions
  defp latest_vc([]), do: %{}

  defp latest_vc(versions),
    do: Enum.max_by(versions, &Enum.sum(Map.values(&1.vector_clock))).vector_clock

  # Merges versions as described in the Dynamo paper:
  # - Removes obsolete versions (those dominated by another's vector clock)
  # - Returns only concurrent/latest versions
  defp merge_versions(responses) do
    # Flatten all versions from all responses
    versions =
      responses
      |> Enum.flat_map(fn
        %{values: values, vector_clocks: vcs} when is_list(values) and is_list(vcs) ->
          Enum.zip(values, vcs)
          |> Enum.map(fn {value, vc} -> %{value: value, vector_clock: vc} end)

        %{value: value, vector_clock: vc} ->
          [%{value: value, vector_clock: vc}]

        _ ->
          []
      end)
      |> Enum.reject(fn v -> v.value == nil end)

    # Remove obsolete versions: keep only those not dominated by any other
    Enum.filter(versions, fn v1 ->
      not Enum.any?(versions, fn v2 ->
        v1 != v2 and VectorClock.compare(v1.vector_clock, v2.vector_clock) == :before
      end)
    end)
  end
end

# Vector clock implementation for versioning and causality tracking
defmodule VectorClock do
  import Emulation, only: [whoami: 0]

  # Increment the vector clock for a given node
  def increment(vc, node) do
    Map.update(vc || %{}, node, 1, &(&1 + 1))
  end

  # Merge two vector clocks (take max for each node)
  def merge(vc1, vc2) do
    Map.merge(vc1 || %{}, vc2 || %{}, fn _k, v1, v2 -> max(v1, v2) end)
  end

  # Compare two vector clocks
  # :before if vc1 < vc2, :after if vc1 > vc2, :equal if same, :concurrent if neither dominates
  def compare(vc1, vc2) do
    keys = (Map.keys(vc1) ++ Map.keys(vc2)) |> Enum.uniq()
    less = Enum.any?(keys, fn k -> Map.get(vc1, k, 0) < Map.get(vc2, k, 0) end)
    more = Enum.any?(keys, fn k -> Map.get(vc1, k, 0) > Map.get(vc2, k, 0) end)

    cond do
      less and not more -> :before
      more and not less -> :after
      less and more -> :concurrent
      true -> :equal
    end
  end
end

# Simple Merkle tree implementation for anti-entropy
defmodule MerkleTree do
  # Build a Merkle tree from a map of key-value pairs
  def build(kv_store) do
    keys = Map.keys(kv_store) |> Enum.sort()

    leaves =
      Enum.map(keys, fn k -> :crypto.hash(:sha256, "#{k}:#{inspect(Map.get(kv_store, k))}") end)

    build_tree(leaves)
  end

  # Recursively build the tree up to the root
  defp build_tree([root]), do: root

  defp build_tree(leaves) do
    leaves
    |> Enum.chunk_every(2, 2, :discard)
    |> Enum.map(fn
      [a, b] -> :crypto.hash(:sha256, a <> b)
      [a] -> a
    end)
    |> build_tree()
  end

  # Compare two Merkle roots for equality
  def equal?(root1, root2), do: root1 == root2
end

defmodule Dynamo.Client do
  import Emulation, only: [send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  @moduledoc """
  A Dynamo client that can send get/put requests to any coordinator node.
  """

  @enforce_keys [:coordinator]
  defstruct(coordinator: nil)

  @doc """
  Construct a new Dynamo Client. Takes the ID of any node in the Dynamo ring.
  """
  @spec new_client(atom()) :: %__MODULE__{coordinator: atom()}
  def new_client(node) do
    %__MODULE__{coordinator: node}
  end

  @spec put(%__MODULE__{}, any(), any(), map()) :: {:ok, %__MODULE__{}} | {:error, :timeout}
  @doc """
  Send a put request to the Dynamo ring.
  """
  def put(client, key, value, context \\ %{}) do
    IO.puts("[Client] Sending PUT request to coordinator: #{inspect(client.coordinator)}")

    send(
      client.coordinator,
      %Messages.ClientPutRequest{
        key: key,
        value: value,
        client: self(),
        context: context
      }
    )

    receive do
      {:msg, _from, %Messages.ClientPutResponse{key: ^key, status: :ok, vector_clock: vc} = msg} ->
        IO.puts("[Client] Received ClientPutResponse: #{inspect(msg)}")
        {:ok, client}

        # other ->
        #   IO.puts("[Client] Received unexpected PUT message: #{inspect(other)}")
        #   {:error, client}
    after
      5_000 ->
        IO.puts("[Client] PUT request timed out for key=#{inspect(key)}")
        {:timeout, client}
    end
  end

  @doc """
  Send a get request to the Dynamo ring.
  """
  @spec get(%__MODULE__{}, any()) :: {:ok, [any()], %__MODULE__{}} | {:error, :timeout}
  def get(client, key) do
    IO.puts("[Client] Sending GET request to coordinator: #{inspect(client.coordinator)}")

    send(
      client.coordinator,
      %Messages.ClientGetRequest{
        key: key,
        client: self()
      }
    )

    receive do
      {:msg, _from, %Messages.ClientGetResponse{key: ^key, values: values} = msg} ->
        IO.puts("[Client] Received ClientGetResponse: #{inspect(msg)}")
        {:ok, values}

      other ->
        IO.puts("[Client] Received unexpected GET message: #{inspect(other)}")
        {:error, client}
    after
      15_000 ->
        IO.puts("[Client] GET request timed out for key=#{inspect(key)}")
        {:error, :timeout}
    end
  end
end
