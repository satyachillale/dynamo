defmodule Messages do
  defstruct [:type, :from, :to, :payload]

  defmodule Gossip do
    defstruct [:from, :membership, :ring]
  end

  defmodule MerkleTreeExchange do
    defstruct [:from, :tree]
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
    defstruct [:key, :value, :client, :context]
  end

  defmodule ClientGetRequest do
    defstruct [:key, :client]
  end

  defmodule ClientPutResponse do
    defstruct [:key, :status, :vector_clock]
  end

  defmodule ClientGetResponse do
    defstruct [:key, :values]
  end

  # Server to Server (Replica)
  defmodule ReplicaPutRequest do
    @enforce_keys [:key, :value, :from, :vector_clock]
    defstruct [:key, :value, :from, :vector_clock, repair: false]
  end

  defmodule ReplicaGetRequest do
    defstruct [:key, :from]
  end

  defmodule ReplicaPutResponse do
    defstruct [:key, :status, :to, :vector_clock]
  end

  defmodule ReplicaGetResponse do
    defstruct [:key, :values, :vector_clocks, :to, :node]
  end
end
