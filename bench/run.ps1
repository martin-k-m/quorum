# Regenerates every number in docs/BENCHMARKS.md. Windows equivalent of bench/run.sh.
#
# Usage:  .\bench\run.ps1 [-Out bench\results]
#
# Run it on an otherwise idle machine. The benchmarks start real multi-node
# clusters on loopback and fsync a real log per node, so background CPU or
# disk load shows up directly in the numbers.
param([string]$Out = "bench\results")

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
New-Item -ItemType Directory -Force -Path $Out | Out-Null

Write-Output "=== environment ==="
go version | Tee-Object -FilePath "$Out\env.txt"
go env GOOS GOARCH | Tee-Object -FilePath "$Out\env.txt" -Append

Write-Output "=== throughput and latency (writes and linearizable reads, 3 and 5 nodes) ==="
# -benchtime=2000x fixes the operation count so every configuration is
# measured over the same amount of work. -count=3 gives three independent
# runs per configuration; BENCHMARKS.md reports the median of the three.
go test ./internal/server/ -run '^$' -bench 'BenchmarkWrite|BenchmarkLinearizableRead' -benchtime 2000x -count 3 -timeout 60m | Tee-Object -FilePath "$Out\throughput.txt"

Write-Output "=== leader election time after killing the leader (25 trials each, 3 and 5 nodes) ==="
go test ./internal/server/ -run 'TestMeasureElectionTime' -quorum.measure -quorum.trials=25 -v -timeout 60m | Tee-Object -FilePath "$Out\election.txt"

Write-Output "=== throughput during and after a partition heal (5 nodes) ==="
go test ./internal/server/ -run 'TestMeasurePartitionHealThroughput' -quorum.measure -v -timeout 60m | Tee-Object -FilePath "$Out\partition-heal.txt"

Write-Output "=== log compaction: disk and restart, with and without (3 runs) ==="
# Three separate invocations rather than -count 3, so each run gets a fresh
# temp directory; the whole point is measuring what a node left on disk.
Remove-Item "$Out\compaction.txt" -ErrorAction SilentlyContinue
foreach ($i in 1..3) {
  Add-Content "$Out\compaction.txt" "=== run $i ==="
  go test ./internal/server/ -run 'TestMeasureCompaction' -quorum.measure -v -timeout 60m | Tee-Object -FilePath "$Out\compaction.txt" -Append
}

Write-Output "=== CPU profile of the write path (3 nodes, 16 concurrent writers) ==="
go test ./internal/server/ -run '^$' -bench 'BenchmarkWrite/nodes=3/conc=16$' -benchtime 4000x -cpuprofile "$Out\write-cpu.prof" -timeout 60m | Tee-Object -FilePath "$Out\profile-run.txt"
go tool pprof -top -nodecount=25 "$Out\write-cpu.prof" | Tee-Object -FilePath "$Out\write-cpu-top.txt"

Write-Output "done. raw output in $Out\"
