#!/usr/bin/env bash
# Compiles the probe package against the shpanstream in ../.. (the replace in
# go.mod) and reports what a Stream element type costs: compile seconds and
# the machine code of the generic instantiations the package carries.
# Usage: measure.sh [N]   (default 50 types; pass the same N before and after)
set -euo pipefail
cd "$(dirname "$0")"
n=${1:-50}
./gen.sh "$n"
go mod tidy > /dev/null 2>&1
out=$(mktemp -d)
go build ./... > /dev/null                      # warm the library's own compile
# A fresh nonce so the probe really recompiles (touch alone hits the cache).
echo "// nonce $(date +%s%N)" >> probe/probe_gen.go
start=$(date +%s.%N)
go build -o "$out/probe.a" ./probe
secs=$(echo "$(date +%s.%N) - $start" | bc)
go tool nm -size "$out/probe.a" | awk '$3 == "T"' > "$out/text.txt"
total=$(awk '{s += $2} END {print s}' "$out/text.txt")
shape=$(grep 'go.shape' "$out/text.txt" | awk '{s += $2} END {print s + 0}')
syms=$(wc -l < "$out/text.txt")
shapesyms=$(grep -c 'go.shape' "$out/text.txt" || true)
printf 'types=%d  compile=%.1fs  text=%d bytes  shape-instantiations=%d bytes in %d/%d symbols  per-type=%d bytes\n' \
  "$n" "$secs" "$total" "$shape" "$shapesyms" "$syms" "$((shape / n))"
echo "by family (bytes, symbols):"
grep 'go.shape' "$out/text.txt" | sed 's/\[.*//' | awk '{a[$NF] += $2; c[$NF]++} END {for (k in a) printf "  %9d %5d %s\n", a[k], c[k], k}' | sort -rn | head -12
rm -rf "$out"
