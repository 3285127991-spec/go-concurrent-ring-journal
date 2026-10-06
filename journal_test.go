package ringjournal

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
)

type readResult struct {
	recs []Record
	err  error
}

func mustNew(t *testing.T, capacity int) *Journal {
	t.Helper()
	j, err := New(capacity)
	if err != nil {
		t.Fatalf("New(%d): %v", capacity, err)
	}
	return j
}

func appendValues(t *testing.T, j *Journal, values ...string) {
	t.Helper()
	for _, v := range values {
		if err := j.Append(v); err != nil {
			t.Fatalf("Append(%q): %v", v, err)
		}
	}
}

// waitForWaiting spins (without sleeping) until n readers are parked in Read.
func waitForWaiting(t *testing.T, j *Journal, n int) {
	t.Helper()
	for i := 0; i < 1_000_000; i++ {
		if j.Snapshot().Waiting == n {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("journal never reached %d waiting readers", n)
}

func seqsOf(recs []Record) []uint64 {
	out := make([]uint64, len(recs))
	for i, r := range recs {
		out[i] = r.Seq
	}
	return out
}

func wantSeqs(t *testing.T, recs []Record, want ...uint64) {
	t.Helper()
	got := seqsOf(recs)
	if len(got) != len(want) {
		t.Fatalf("got seqs %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got seqs %v, want %v", got, want)
		}
	}
}

func TestAppendReadBasic(t *testing.T) {
	j := mustNew(t, 4)
	appendValues(t, j, "a", "b", "c")

	if got := j.Len(); got != 3 {
		t.Fatalf("Len() = %d, want 3", got)
	}
	if got := j.Capacity(); got != 4 {
		t.Fatalf("Capacity() = %d, want 4", got)
	}
	recs, err := j.Read(context.Background(), 0, 10)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	wantSeqs(t, recs, 1, 2, 3)
	if recs[0].Value != "a" || recs[2].Value != "c" {
		t.Fatalf("unexpected values: %v", recs)
	}
}

func TestOverwriteAndLagged(t *testing.T) {
	j := mustNew(t, 3)
	appendValues(t, j, "a", "b", "c", "d", "e") // retains seq 3,4,5

	if got := j.Len(); got != 3 {
		t.Fatalf("Len() = %d, want 3", got)
	}
	if _, err := j.Read(context.Background(), 1, 10); !errors.Is(err, ErrLagged) {
		t.Fatalf("Read(afterSeq=1) err = %v, want ErrLagged", err)
	}
	recs, err := j.Read(context.Background(), 2, 10)
	if err != nil {
		t.Fatalf("Read(afterSeq=2): %v", err)
	}
	wantSeqs(t, recs, 3, 4, 5)
	if recs[0].Value != "c" || recs[2].Value != "e" {
		t.Fatalf("overwrite kept wrong values: %v", recs)
	}
}

func TestReadLimitBatches(t *testing.T) {
	j := mustNew(t, 8)
	appendValues(t, j, "a", "b", "c", "d", "e")
	ctx := context.Background()

	b1, err := j.Read(ctx, 0, 2)
	if err != nil {
		t.Fatalf("batch 1: %v", err)
	}
	wantSeqs(t, b1, 1, 2)

	b2, err := j.Read(ctx, b1[len(b1)-1].Seq, 2)
	if err != nil {
		t.Fatalf("batch 2: %v", err)
	}
	wantSeqs(t, b2, 3, 4)

	b3, err := j.Read(ctx, b2[len(b2)-1].Seq, 2)
	if err != nil {
		t.Fatalf("batch 3: %v", err)
	}
	wantSeqs(t, b3, 5) // short final batch at the head
}

func TestIndependentReaders(t *testing.T) {
	j := mustNew(t, 4)
	appendValues(t, j, "a", "b", "c")
	ctx := context.Background()

	// Two readers page through the same journal with separate cursors.
	r1, err := j.Read(ctx, 0, 1)
	if err != nil {
		t.Fatalf("reader 1: %v", err)
	}
	wantSeqs(t, r1, 1)

	r2, err := j.Read(ctx, 1, 2)
	if err != nil {
		t.Fatalf("reader 2: %v", err)
	}
	wantSeqs(t, r2, 2, 3)

	// Reader 1 continues from its own cursor, unaffected by reader 2.
	r1next, err := j.Read(ctx, r1[len(r1)-1].Seq, 2)
	if err != nil {
		t.Fatalf("reader 1 continue: %v", err)
	}
	wantSeqs(t, r1next, 2, 3)
}

func TestBlockingReadWakeAndCancel(t *testing.T) {
	j := mustNew(t, 2)
	ctx := context.Background()

	// A parked reader is woken by the next Append.
	done := make(chan readResult, 1)
	go func() {
		recs, err := j.Read(ctx, 0, 1)
		done <- readResult{recs, err}
	}()
	waitForWaiting(t, j, 1)
	appendValues(t, j, "x")
	res := <-done
	if res.err != nil {
		t.Fatalf("blocked Read: %v", res.err)
	}
	wantSeqs(t, res.recs, 1)

	// Cancelling the context unblocks only that Read; the journal stays usable.
	ctx2, cancel := context.WithCancel(context.Background())
	done2 := make(chan readResult, 1)
	go func() {
		recs, err := j.Read(ctx2, 1, 1)
		done2 <- readResult{recs, err}
	}()
	waitForWaiting(t, j, 1)
	cancel()
	if res := <-done2; !errors.Is(res.err, context.Canceled) {
		t.Fatalf("cancelled Read err = %v, want context.Canceled", res.err)
	}
	waitForWaiting(t, j, 0)

	appendValues(t, j, "y")
	recs, err := j.Read(ctx, 1, 1)
	if err != nil {
		t.Fatalf("Read after cancel: %v", err)
	}
	wantSeqs(t, recs, 2)
}

func TestLaggedWhileWaiting(t *testing.T) {
	j := mustNew(t, 2)

	done := make(chan readResult, 1)
	go func() {
		recs, err := j.Read(context.Background(), 0, 1)
		done <- readResult{recs, err}
	}()
	waitForWaiting(t, j, 1)

	// Overwrite the whole retained window while the reader is parked: hold
	// the lock so all appends land before the reader re-checks its position.
	j.mu.Lock()
	for _, v := range []string{"a", "b", "c"} {
		if err := j.appendLocked(v); err != nil {
			j.mu.Unlock()
			t.Fatalf("appendLocked: %v", err)
		}
	}
	j.mu.Unlock()

	if res := <-done; !errors.Is(res.err, ErrLagged) {
		t.Fatalf("blocked Read err = %v, want ErrLagged", res.err)
	}
}

func TestCloseSemantics(t *testing.T) {
	j := mustNew(t, 4)
	appendValues(t, j, "a", "b", "c")

	// Close wakes a parked reader with ErrClosed.
	done := make(chan readResult, 1)
	go func() {
		recs, err := j.Read(context.Background(), 3, 1)
		done <- readResult{recs, err}
	}()
	waitForWaiting(t, j, 1)
	j.Close()
	if res := <-done; !errors.Is(res.err, ErrClosed) {
		t.Fatalf("waiting Read after Close err = %v, want ErrClosed", res.err)
	}

	if err := j.Append("z"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Append after Close = %v, want ErrClosed", err)
	}

	// Retained records still drain in order...
	ctx := context.Background()
	recs, err := j.Read(ctx, 0, 2)
	if err != nil {
		t.Fatalf("drain 1: %v", err)
	}
	wantSeqs(t, recs, 1, 2)
	recs, err = j.Read(ctx, 2, 2)
	if err != nil {
		t.Fatalf("drain 2: %v", err)
	}
	wantSeqs(t, recs, 3)

	// ...then Read reports ErrClosed instead of blocking.
	if _, err := j.Read(ctx, 3, 2); !errors.Is(err, ErrClosed) {
		t.Fatalf("Read after drain err = %v, want ErrClosed", err)
	}

	j.Close() // idempotent
	if got := j.Len(); got != 3 {
		t.Fatalf("Len() after Close = %d, want 3", got)
	}
}

func TestSnapshotAndInvalidParams(t *testing.T) {
	if _, err := New(0); !errors.Is(err, ErrInvalidCapacity) {
		t.Fatalf("New(0) = %v, want ErrInvalidCapacity", err)
	}
	if _, err := New(-2); !errors.Is(err, ErrInvalidCapacity) {
		t.Fatalf("New(-2) = %v, want ErrInvalidCapacity", err)
	}

	j := mustNew(t, 3)
	if _, err := j.Read(context.Background(), 0, 0); !errors.Is(err, ErrInvalidLimit) {
		t.Fatalf("Read(limit=0) = %v, want ErrInvalidLimit", err)
	}
	if _, err := j.Read(context.Background(), 0, -1); !errors.Is(err, ErrInvalidLimit) {
		t.Fatalf("Read(limit=-1) = %v, want ErrInvalidLimit", err)
	}

	appendValues(t, j, "a", "b")
	snap := j.Snapshot()
	wantSeqs(t, snap.Records, 1, 2)
	if snap.NextSeq != 3 || snap.Waiting != 0 || snap.Closed {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}

	// The snapshot is an independent copy.
	snap.Records[0].Value = "mutated"
	recs, err := j.Read(context.Background(), 0, 1)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if recs[0].Value != "a" {
		t.Fatalf("snapshot mutation leaked into journal: %v", recs[0].Value)
	}

	j.Close()
	if snap := j.Snapshot(); !snap.Closed {
		t.Fatal("snapshot after Close: Closed = false")
	}
}

func TestConcurrentAppendRead(t *testing.T) {
	const writers = 4
	const perWriter = 25
	j := mustNew(t, writers*perWriter) // capacity fits everything: no overwrite

	ctx, cancel := context.WithCancel(context.Background())
	var readers sync.WaitGroup
	for r := 0; r < 2; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			var after uint64
			for {
				recs, err := j.Read(ctx, after, 3)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					t.Errorf("Read: %v", err)
					return
				}
				after = recs[len(recs)-1].Seq
			}
		}()
	}

	var writersWG sync.WaitGroup
	for w := 0; w < writers; w++ {
		writersWG.Add(1)
		go func(id int) {
			defer writersWG.Done()
			for i := 0; i < perWriter; i++ {
				if err := j.Append(id); err != nil {
					t.Errorf("Append: %v", err)
					return
				}
			}
		}(w)
	}
	writersWG.Wait()
	cancel()
	readers.Wait()

	// Seqs must be exactly 1..N: strictly increasing, never repeated.
	snap := j.Snapshot()
	if len(snap.Records) != writers*perWriter {
		t.Fatalf("snapshot has %d records, want %d", len(snap.Records), writers*perWriter)
	}
	for i, rec := range snap.Records {
		if want := uint64(i) + 1; rec.Seq != want {
			t.Fatalf("record %d has Seq %d, want %d", i, rec.Seq, want)
		}
	}
}
