defmodule VectorClockTest do
  use ExUnit.Case

  test "vector clock increment and merge" do
    vc1 = VectorClock.increment(%{}, :a)
    assert vc1 == %{a: 1}
    vc2 = VectorClock.increment(vc1, :a)
    assert vc2 == %{a: 2}
    vc3 = VectorClock.increment(%{}, :b)
    merged = VectorClock.merge(vc2, vc3)
    assert merged == %{a: 2, b: 1}
  end

  test "vector clock compare" do
    vc1 = %{a: 1, b: 2}
    vc2 = %{a: 2, b: 2}
    assert VectorClock.compare(vc1, vc2) == :before
    assert VectorClock.compare(vc2, vc1) == :after
    assert VectorClock.compare(vc1, vc1) == :equal
    assert VectorClock.compare(%{a: 1}, %{b: 1}) == :concurrent
  end

  test "vector clock concurrent branches" do
    base = %{a: 1, b: 1}
    branch1 = VectorClock.increment(base, :a)
    branch2 = VectorClock.increment(base, :b)
    assert VectorClock.compare(branch1, branch2) == :concurrent
    assert VectorClock.compare(branch2, branch1) == :concurrent
  end

  test "vector clock ancestor detection" do
    ancestor = %{a: 1, b: 1}
    descendant = VectorClock.increment(ancestor, :a)
    assert VectorClock.compare(ancestor, descendant) == :before
    assert VectorClock.compare(descendant, ancestor) == :after
  end

  test "vector clock merge with missing keys" do
    vc1 = %{a: 2}
    vc2 = %{b: 3}
    merged = VectorClock.merge(vc1, vc2)
    assert merged == %{a: 2, b: 3}
  end

  test "vector clock compare with missing keys" do
    vc1 = %{a: 2}
    vc2 = %{a: 2, b: 1}
    assert VectorClock.compare(vc1, vc2) == :before
    assert VectorClock.compare(vc2, vc1) == :after
  end
end
