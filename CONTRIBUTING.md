# Contributing to quorum

Thanks for taking a look. quorum is an implementation of a protocol with a
written specification and a proof, so the bar for a change is different from
most projects: the question is not just "does it work" but "is it still the
algorithm". [docs/DESIGN.md](docs/DESIGN.md) is the reference for what this
implementation promises; changes should keep it accurate.

## Setup

You need Go (the CI uses the current stable release). There are no third-party
dependencies.

```bash
git clone https://github.com/martin-k-m/quorum
cd quorum
go build ./cmd/quorum
go test ./... -race
```

The whole suite, chaos runs included, finishes in well under a minute.

## Ground rules

- **The Raft core has no I/O.** `internal/raft` is a state machine driven by
  `Step(message)` and `Tick()`: no clock, no goroutines, no network, no disk.
  That is what makes it testable under a deterministic simulator, and a change
  that reaches for a timer or a socket inside it gives that up.
- **Safety rules are not negotiable.** The log-matching check on append, the
  up-to-date restriction on granting votes, and current-term-only commitment
  (the Figure-8 guard) each exist because a specific history goes wrong without
  them. If a change makes one look redundant, the change is wrong or the test
  covering it is missing.
- **Nothing acknowledges before it is durable.** Term, vote and log entries are
  fsynced before the message that depends on them goes out.
- **Reads go through the log.** `Server.Get` commits a no-op and answers only
  once it applies. A local read is faster and is not linearizable; that
  shortcut was already tried, and the checker found it stale under partition.
- **Under a partition, refuse rather than diverge.** A minority side must fail
  closed. If a change lets it answer, it is a correctness regression however
  reasonable the availability argument sounds.
- **Behaviour changes come with tests**, in the package you touched. For
  anything touching the protocol, the useful test is a schedule under
  `internal/testsim`, seeded so it reproduces exactly.

## Correctness bugs

The most valuable contribution is a seed that fails. If
`TestLinearizabilityAcrossFaultInjectedSchedules` finds a violation, open an
issue with the seed and the printed history: that is a complete reproduction on
its own, and the three bugs the checker has already found were all found that
way.

Widening the search is welcome too, whether that means more schedules, longer
fault windows, or fault kinds the simulator does not yet inject.

## Before you open a pull request

CI gates on all of these, so run them locally first:

```bash
gofmt -l .              # must print nothing
go vet ./...
go test ./... -race
go build ./cmd/quorum
```

If you changed anything about the protocol, run the suite a few times
(`go test ./... -race -count=5`). Consensus bugs are frequently schedule
dependent, and one green run is weak evidence.

Keep pull requests focused. The commit history is one commit per milestone with
a long message explaining the reasoning, and that is the style to match.

## Reporting bugs

Open an issue with the cluster size, the exact commands, the fault schedule or
seed if there was one, and the node logs. For a suspected correctness violation
rather than a crash, see [SECURITY.md](SECURITY.md) first.
