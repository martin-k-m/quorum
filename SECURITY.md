# Security Policy

## Supported versions

quorum has not cut a release yet. Fixes land on `main`; there are no
maintenance branches.

## Reporting a vulnerability

Please report suspected vulnerabilities privately rather than in a public
issue. Use GitHub's [private vulnerability reporting](https://github.com/martin-k-m/quorum/security/advisories/new)
for this repository, or email martinkmuskov@gmail.com.

Include the cluster configuration, the sequence of client operations, and any
fault injection or partition involved. If a linearizability violation is what
you found, the seed from
`TestLinearizabilityAcrossFaultInjectedSchedules` is usually the whole
reproduction, since the checker's chaos runs are seeded. You can expect an
acknowledgement within a few days.

## The peer protocol is unauthenticated

`internal/transport` speaks `net/rpc` with `encoding/gob` over plain TCP. There
is no TLS, no authentication, and no authorization: anything that can reach a
node's listener can send it Raft messages, and a message claiming a higher term
makes a leader step down. Anything that can reach it can also propose writes.

That is a deliberate scope choice, not an oversight — this is a protocol
implementation, not a hardened service. Run a cluster only on a network you
control, and treat the peer addresses as trusted. `gob` decoding of untrusted
bytes is its own risk surface on top of the above.

## What is worth reporting

The classes of issue most worth a report are ones that touch the correctness
promise the project exists to make:

- A schedule of crashes, partitions or message loss under which an
  acknowledged write is lost, or under which reads and writes are not
  linearizable.
- A crafted or truncated `internal/storage` log that makes recovery read back
  the wrong data rather than fail cleanly, or that bypasses the CRC32 check.
- An input that drives unbounded memory or an unrecoverable panic in the Raft
  core.

Reports that amount to "an unauthenticated peer can disrupt the cluster" are
already covered by the section above and are not separately actionable.
