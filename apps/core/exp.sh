#!/bin/bash

COUNT=0

for i in {1..10}
do
  OUTPUT=$(mix test test/stale.exs)
  echo "$OUTPUT" > "run_${i}.txt"
  LINE=$(echo "$OUTPUT" | grep "Stale reads:")
  # Extract the percentage value (Z) from the output using sed
  PERCENT=$(echo "$LINE" | sed -n 's/.*(\([0-9]\+\)%)$/\1/p')
  if [ ! -z "$PERCENT" ] && [ "$PERCENT" -gt 0 ]; then
    COUNT=$((COUNT+1))
  fi
done

echo "Number of runs with stale reads > 0%: $COUNT out of 50"