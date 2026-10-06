// Package ringjournal implements a concurrent, bounded, in-memory ring
// journal: a fixed-capacity buffer of records with strictly increasing
// sequence numbers, blocking reads, and point-in-time snapshots.
//
// A Journal is safe for concurrent use and spawns no goroutines of its own.
package ringjournal

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrLagged is returned by Read when afterSeq+1 is older than the
	// oldest retained Seq, so a continuous read is no longer possible.
	ErrLagged = errors.New("ringjournal: afterSeq is behind the retained window")
	// ErrClosed is returned by Append after Close, and by Read once the
	// journal is closed and no retained record has Seq > afterSeq.
	ErrClosed = errors.New("ringjournal: journal is closed")
	// ErrInvalidCapacity is returned by New when capacity is not positive.
	ErrInvalidCapacity = errors.New("ringjournal: capacity must be greater than zero")
	// ErrInvalidLimit is returned by Read when limit is not positive.
	ErrInvalidLimit = errors.New("ringjournal: limit must be greater than zero")
)

// Record is a single journal entry. Seq is assigned by Append, starts at 1,
// and increases strictly without repeating for the life of the journal.
type Record struct {
	Seq   uint64
	Value any
}

// Snapshot is a consistent point-in-time view of a Journal.
type Snapshot struct {
	Records []Record // retained records, ordered by ascending Seq
	NextSeq uint64   // Seq the next Append will assign
	Waiting int      // number of readers currently blocked in Read
	Closed  bool     // whether Close has been called
}

// Journal is a concurrency-safe bounded ring of records. Only the most
// recent Capacity records are retained; older ones are silently overwritten
// for writers, but lagging readers are told via ErrLagged. Create with New;
// the zero value is not usable.
type Journal struct {
	mu      sync.Mutex
	buf     []Record
	start   int // ring index of the oldest retained record
	count   int
	nextSeq uint64 // next Seq to assign; starts at 1
	closed  bool
	waiting int
	changed chan struct{} // closed and replaced on every Append/Close to wake readers
}

// New returns a Journal that retains at most capacity records.
func New(capacity int) (*Journal, error) {
	if capacity <= 0 {
		return nil, ErrInvalidCapacity
	}
	return &Journal{
		buf:     make([]Record, capacity),
		nextSeq: 1,
		changed: make(chan struct{}),
	}, nil
}

// Append stores value with the next Seq, overwriting the oldest retained
// record when the journal is full, and wakes every blocked reader. It
// returns ErrClosed after Close.
func (j *Journal) Append(value any) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.appendLocked(value)
}

func (j *Journal) appendLocked(value any) error {
	if j.closed {
		return ErrClosed
	}
	rec := Record{Seq: j.nextSeq, Value: value}
	j.nextSeq++
	if j.count < len(j.buf) {
		j.buf[(j.start+j.count)%len(j.buf)] = rec
		j.count++
	} else {
		j.buf[j.start] = rec // overwrite oldest
		j.start = (j.start + 1) % len(j.buf)
	}
	j.wakeLocked()
	return nil
}

// Read returns up to limit retained records with Seq > afterSeq, in
// ascending Seq order. Readers are fully independent: the cursor is the
// afterSeq argument and no per-reader state is kept.
//
// If matching records exist they are returned immediately. Otherwise Read
// blocks until an Append arrives, the journal is closed, or ctx is done;
// cancelling ctx affects only this call. If afterSeq falls behind the
// retained window while waiting (or already is), Read returns ErrLagged
// instead of silently skipping records. After Close, retained records are
// still returned; once none remain with Seq > afterSeq, Read returns
// ErrClosed.
func (j *Journal) Read(ctx context.Context, afterSeq uint64, limit int) ([]Record, error) {
	if limit <= 0 {
		return nil, ErrInvalidLimit
	}
	for {
		j.mu.Lock()
		oldest := j.nextSeq - uint64(j.count) // Seq of oldest retained record; always >= 1
		if afterSeq < oldest-1 {
			j.mu.Unlock()
			return nil, ErrLagged
		}
		if afterSeq < j.nextSeq-1 { // retained records with Seq > afterSeq exist
			skip := int(afterSeq + 1 - oldest) // retained records with Seq <= afterSeq
			recs := make([]Record, min(j.count-skip, limit))
			for i := range recs {
				recs[i] = j.buf[(j.start+skip+i)%len(j.buf)]
			}
			j.mu.Unlock()
			return recs, nil
		}
		if j.closed {
			j.mu.Unlock()
			return nil, ErrClosed
		}
		ch := j.changed
		j.waiting++
		j.mu.Unlock()

		select {
		case <-ctx.Done():
			j.mu.Lock()
			j.waiting--
			j.mu.Unlock()
			return nil, ctx.Err()
		case <-ch:
			j.mu.Lock()
			j.waiting--
			j.mu.Unlock()
		}
	}
}

// Snapshot returns a consistent view of the journal. Records is an
// independent copy; mutating it does not affect the journal.
func (j *Journal) Snapshot() Snapshot {
	j.mu.Lock()
	defer j.mu.Unlock()
	recs := make([]Record, j.count)
	for i := range recs {
		recs[i] = j.buf[(j.start+i)%len(j.buf)]
	}
	return Snapshot{
		Records: recs,
		NextSeq: j.nextSeq,
		Waiting: j.waiting,
		Closed:  j.closed,
	}
}

// Len returns the number of currently retained records.
func (j *Journal) Len() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.count
}

// Capacity returns the maximum number of retained records. It is fixed at
// construction, so no lock is needed.
func (j *Journal) Capacity() int {
	return len(j.buf)
}

// Close rejects future Appends and wakes every blocked reader. Retained
// records remain readable until drained. Close is idempotent.
func (j *Journal) Close() {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return
	}
	j.closed = true
	j.wakeLocked()
}

// wakeLocked broadcasts a state change to all blocked readers.
func (j *Journal) wakeLocked() {
	close(j.changed)
	j.changed = make(chan struct{})
}
