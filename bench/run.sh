#!/usr/bin/env bash
# Regenerates every number in docs/BENCHMARKS.md.
#
# Usage:  bench/run.sh [output-dir]
# Default output dir is bench/results/.
#
# Run it on an otherwise idle machine. The benchmarks start real multi-node
# clusters on loopback and fsync a real log per node, so background CPU or
# disk load shows up directly in the numbers.
set -euo pipefail

cd "$(dirname "$0")/.."
OUT="${1:-bench/results}"
mkdir -p "$OUT"

echo "=== environment ==="
go version | tee "$OUT/env.txt"
go env GOOS GOARCH >> "$OUT/env.txt"

echo
echo "=== throughput and latency (writes and linearizable reads, 3 and 5 nodes) ==="
# -benchtime=2000x fixes the operation count so every configuration is
# measured over the same amount of work. -count=3 gives three independent
# runs per configuration; BENCHMARKS.md reports the median of the three.
go test ./internal/server/ \
  -run '^$' \
  -bench 'BenchmarkWrite|BenchmarkLinearizableRead' \
  -benchtime 2000x \
  -count 3 \
  -timeout 60m \
  | tee "$OUT/throughput.txt"

echo
echo "=== leader election time after killing the leader (25 trials each, 3 and 5 nodes) ==="
go test ./internal/server/ \
  -run 'TestMeasureElectionTime' \
  -quorum.measure -quorum.trials=25 \
  -v -timeout 60m \
  | tee "$OUT/election.txt"

echo
echo "=== throughput during and after a partition heal (5 nodes) ==="
go test ./internal/server/ \
  -run 'TestMeasurePartitionHealThroughput' \
  -quorum.measure \
  -v -timeout 60m \
  | tee "$OUT/partition-heal.txt"

echo
echo "=== CPU profile of the write path (3 nodes, 16 concurrent writers) ==="
go test ./internal/server/ \
  -run '^$' \
  -bench 'BenchmarkWrite/nodes=3/conc=16$' \
  -benchtime 4000x \
  -cpuprofile "$OUT/write-cpu.prof" \
  -timeout 60m \
  | tee "$OUT/profile-run.txt"
go tool pprof -top -nodecount=25 "$OUT/write-cpu.prof" \
  | tee "$OUT/write-cpu-top.txt"

echo
echo "done. raw output in $OUT/"
