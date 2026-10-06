# ringjournal

A concurrent, bounded, in-memory **ring journal** for Go: library + demo CLI +
tests. No third-party dependencies, no background goroutines, no services.

## Behavior

- `New(capacity)` with `capacity > 0`; the journal retains only the most
  recent `capacity` records and overwrites the oldest when full.
- `Append(value)` assigns each record a `uint64` `Seq` starting at 1,
  strictly increasing, never repeated.
- `Read(ctx, afterSeq, limit)` returns retained records with `Seq > afterSeq`
  in ascending `Seq` order, at most `limit` of them:
  - returns immediately when matching records exist;
  - otherwise blocks until an `Append`, `Close`, or context cancellation
    (cancellation affects only that call);
  - returns `ErrLagged` when `afterSeq+1` is older than the oldest retained
    `Seq` — data is never silently skipped, including when a parked reader
    falls behind while waiting.
- Readers are fully independent: the cursor is just the `afterSeq` argument.
- `Close` rejects new appends (`ErrClosed`) and wakes all parked readers;
  retained records remain readable until drained, then `Read` returns
  `ErrClosed`.
- `Snapshot()` returns an independent copy of the retained records plus
  `NextSeq`, `Waiting`, `Closed`. `Len()` is the retained record count.

## API

```go
j, err := ringjournal.New(3)        // capacity > 0, else ErrInvalidCapacity
err = j.Append("event")             // assigns Seq 1, 2, 3, ...
recs, err := j.Read(ctx, 0, 10)     // Seq > 0, ascending, up to 10
snap := j.Snapshot()                // {Records, NextSeq, Waiting, Closed}
n := j.Len()
c := j.Capacity()
j.Close()                           // idempotent
```

Sentinel errors: `ErrLagged`, `ErrClosed`, `ErrInvalidCapacity`,
`ErrInvalidLimit`.

## Demo CLI

```sh
go run ./cmd/ringjournal-demo
```

Walks through appends, capacity overwrite, batched reads, one lagged read,
one blocking read woken by an append, and draining after `Close`.

## Tests

```sh
go test -race ./...
```

Covers basic append/read, overwrite + `ErrLagged`, batched reads, independent
readers, blocking wake + cancel, lagging while parked, close/drain semantics,
snapshot + invalid parameters, and a small concurrent append/read check.

## Non-goals

Persistence, subscription management, consumer groups, ACKs, retries, TTL,
networking, GUI, databases.
