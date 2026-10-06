// Command ringjournal-demo walks through the ringjournal API end to end:
// appends, capacity overwrite, batched reads, a lagged read, a blocking
// read woken by an append, and draining after Close.
package main

import (
	"context"
	"fmt"
	"runtime"

	"ringjournal"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func seqsOf(recs []ringjournal.Record) []uint64 {
	out := make([]uint64, len(recs))
	for i, r := range recs {
		out[i] = r.Seq
	}
	return out
}

func main() {
	ctx := context.Background()
	j, err := ringjournal.New(3)
	must(err)
	fmt.Printf("new journal: capacity=%d\n\n", j.Capacity())

	fmt.Println("== append 5 events into a capacity-3 journal (oldest get overwritten)")
	for i := 1; i <= 5; i++ {
		must(j.Append(fmt.Sprintf("event-%d", i)))
	}
	snap := j.Snapshot()
	fmt.Printf("len=%d retained seqs=%v nextSeq=%d\n\n", j.Len(), seqsOf(snap.Records), snap.NextSeq)

	fmt.Println("== read from an overwritten position -> ErrLagged")
	_, err = j.Read(ctx, 1, 10)
	fmt.Printf("read(afterSeq=1) err=%v\n\n", err)

	fmt.Println("== drain retained records in batches of 2")
	after := uint64(2)
	for {
		recs, err := j.Read(ctx, after, 2)
		must(err)
		fmt.Printf("read(afterSeq=%d, limit=2) -> seqs=%v\n", after, seqsOf(recs))
		after = recs[len(recs)-1].Seq
		if len(recs) < 2 {
			break
		}
	}
	fmt.Println()

	fmt.Println("== a reader blocks until the next append wakes it")
	type result struct {
		recs []ringjournal.Record
		err  error
	}
	done := make(chan result, 1)
	go func() {
		recs, err := j.Read(ctx, after, 1)
		done <- result{recs, err}
	}()
	for j.Snapshot().Waiting == 0 { // wait until the reader is parked
		runtime.Gosched()
	}
	fmt.Println("reader is parked; appending event-6")
	must(j.Append("event-6"))
	res := <-done
	fmt.Printf("woken reader got seqs=%v err=%v\n\n", seqsOf(res.recs), res.err)

	must(j.Append("event-7"))
	must(j.Append("event-8"))
	fmt.Println("appended event-7, event-8")

	fmt.Println("\n== close: appends are rejected, retained data still drains")
	j.Close()
	fmt.Printf("append after close err=%v\n", j.Append("event-9"))
	recs, err := j.Read(ctx, 6, 10)
	fmt.Printf("read(afterSeq=6) -> seqs=%v err=%v\n", seqsOf(recs), err)
	_, err = j.Read(ctx, 8, 10)
	fmt.Printf("read(afterSeq=8) err=%v (drained)\n", err)

	final := j.Snapshot()
	fmt.Printf("final snapshot: records=%v nextSeq=%d waiting=%d closed=%v\n",
		seqsOf(final.Records), final.NextSeq, final.Waiting, final.Closed)
}
