defmodule Messages do
  defstruct [:type, :from, :to, :payload]

  defmodule Gossip do
    @enforce_keys [:from, :membership, :ring]
    defstruct from: nil, membership: %{}, ring: []
  end

  defmodule MerkleTreeExchange do
    @enforce_keys [:from, :tree]
    defstruct from: nil, tree: nil
  end

  defmodule GetRequest do
    @enforce_keys [:key, :from]
    defstruct key: nil, from: nil
  end

  defmodule GetResponse do
    @enforce_keys [:key, :values, :vector_clocks, :to]
    defstruct key: nil, values: [], vector_clocks: [], to: nil
  end

  defmodule PutRequest do
    @enforce_keys [:key, :value, :from, :vector_clock]
    defstruct key: nil, value: nil, from: nil, vector_clock: %{}
  end

  defmodule PutResponse do
    @enforce_keys [:key, :status, :to, :vector_clock]
    defstruct key: nil, status: :ok, to: nil, vector_clock: %{}
  end

  # Client to Server
  defmodule ClientPutRequest do
    @enforce_keys [:key, :value, :client, :context]
    defstruct key: nil, value: nil, client: nil, context: %{}
  end

  defmodule ClientGetRequest do
    @enforce_keys [:key, :client]
    defstruct key: nil, client: nil
  end

  defmodule ClientPutResponse do
    @enforce_keys [:key, :status, :to, :vector_clock]
    defstruct key: nil, status: :ok, to: nil, vector_clock: %{}
  end

  defmodule ClientGetResponse do
    @enforce_keys [:key, :values, :vector_clocks, :to]
    defstruct key: nil, values: [], vector_clocks: [], to: nil
  end

  # Server to Server (Replica)
  defmodule ReplicaPutRequest do
    @enforce_keys [:key, :value, :from, :vector_clock]
    defstruct key: nil, value: nil, from: nil, vector_clock: %{}
  end

  defmodule ReplicaGetRequest do
    @enforce_keys [:key, :from]
    defstruct key: nil, from: nil
  end

  defmodule ReplicaPutResponse do
    @enforce_keys [:key, :status, :to, :vector_clock]
    defstruct key: nil, status: :ok, to: nil, vector_clock: %{}
  end

  defmodule ReplicaGetResponse do
    @enforce_keys [:key, :values, :vector_clocks, :to, :node]
    defstruct key: nil, values: [], vector_clocks: [], to: nil, node: nil
  end
end
