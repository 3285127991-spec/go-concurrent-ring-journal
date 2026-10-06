package journal_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"ringjournal/journal"
)

type readResult struct {
	recs []journal.Record
	err  error
}

func readAsync(j *journal.Journal, ctx context.Context, afterSeq uint64, limit int) <-chan readResult {
	ch := make(chan readResult, 1)
	go func() {
		recs, err := j.Read(ctx, afterSeq, limit)
		ch <- readResult{recs: recs, err: err}
	}()
	return ch
}

func waitForWaiting(t *testing.T, j *journal.Journal, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for j.Snapshot().Waiting != want {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: waiting readers = %d, want %d", j.Snapshot().Waiting, want)
		}
		runtime.Gosched()
	}
}

func mustValues(t *testing.T, recs []journal.Record, want ...string) {
	t.Helper()
	if len(recs) != len(want) {
		t.Fatalf("got %d records, want %d: %v", len(recs), len(want), recs)
	}
	for i, w := range want {
		if string(recs[i].Value) != w {
			t.Fatalf("record %d: got value %q, want %q", i, recs[i].Value, w)
		}
		if i > 0 && recs[i].Seq <= recs[i-1].Seq {
			t.Fatalf("records not strictly ascending by Seq: %v", recs)
		}
	}
}

func TestAppendReadBasic(t *testing.T) {
	j, err := journal.New(4)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer j.Close()

	for i := 1; i <= 3; i++ {
		seq, err := j.Append([]byte(fmt.Sprintf("v%d", i)))
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if seq != uint64(i) {
			t.Fatalf("Append returned seq %d, want %d", seq, i)
		}
	}
	if got := j.Len(); got != 3 {
		t.Fatalf("Len = %d, want 3", got)
	}
	if got := j.Capacity(); got != 4 {
		t.Fatalf("Capacity = %d, want 4", got)
	}

	recs, err := j.Read(context.Background(), 0, 10)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	mustValues(t, recs, "v1", "v2", "v3")
	if recs[0].Seq != 1 || recs[2].Seq != 3 {
		t.Fatalf("unexpected seqs: %v", recs)
	}

	recs, err = j.Read(context.Background(), 1, 10)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	mustValues(t, recs, "v2", "v3")
}

func TestOverwriteAndLagged(t *testing.T) {
	j, err := journal.New(3)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer j.Close()

	for i := 1; i <= 5; i++ {
		if _, err := j.Append([]byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if got := j.Len(); got != 3 {
		t.Fatalf("Len = %d, want 3", got)
	}

	if _, err := j.Read(context.Background(), 1, 10); !errors.Is(err, journal.ErrLagged) {
		t.Fatalf("Read(afterSeq=1) err = %v, want ErrLagged", err)
	}
	if _, err := j.Read(context.Background(), 0, 10); !errors.Is(err, journal.ErrLagged) {
		t.Fatalf("Read(afterSeq=0) err = %v, want ErrLagged", err)
	}

	recs, err := j.Read(context.Background(), 2, 10)
	if err != nil {
		t.Fatalf("Read(afterSeq=2): %v", err)
	}
	mustValues(t, recs, "v3", "v4", "v5")

	recs, err = j.Read(context.Background(), 4, 10)
	if err != nil {
		t.Fatalf("Read(afterSeq=4): %v", err)
	}
	mustValues(t, recs, "v5")
}

func TestReadLimitBatches(t *testing.T) {
	j, err := journal.New(5)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer j.Close()

	for i := 1; i <= 5; i++ {
		if _, err := j.Append([]byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	recs, err := j.Read(context.Background(), 0, 2)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	mustValues(t, recs, "v1", "v2")

	recs, err = j.Read(context.Background(), 2, 2)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	mustValues(t, recs, "v3", "v4")

	recs, err = j.Read(context.Background(), 4, 2)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	mustValues(t, recs, "v5")

	recs, err = j.Read(context.Background(), 1, 100)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	mustValues(t, recs, "v2", "v3", "v4", "v5")
}

func TestIndependentReaders(t *testing.T) {
	j, err := journal.New(4)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer j.Close()

	for i := 1; i <= 3; i++ {
		if _, err := j.Append([]byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	// 游标是显式参数：相同 afterSeq 重复读取结果一致，读取不消耗任何状态。
	first, err := j.Read(context.Background(), 0, 10)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	second, err := j.Read(context.Background(), 0, 10)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	mustValues(t, first, "v1", "v2", "v3")
	mustValues(t, second, "v1", "v2", "v3")

	// 修改任一 reader 的结果不影响其他 reader 和 Journal 内部数据。
	first[0].Value[0] = 'X'
	if got := string(second[0].Value); got != "v1" {
		t.Fatalf("reader results share memory: %q", got)
	}
	again, err := j.Read(context.Background(), 0, 1)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got := string(again[0].Value); got != "v1" {
		t.Fatalf("journal data mutated through reader: %q", got)
	}

	// 一个 reader 阻塞等待不影响其他 reader 读取已有数据。
	blocked := readAsync(j, context.Background(), 3, 1)
	waitForWaiting(t, j, 1)
	recs, err := j.Read(context.Background(), 1, 10)
	if err != nil {
		t.Fatalf("Read while another reader blocked: %v", err)
	}
	mustValues(t, recs, "v2", "v3")
	if _, err := j.Append([]byte("v4")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	res := <-blocked
	if res.err != nil {
		t.Fatalf("blocked Read: %v", res.err)
	}
	mustValues(t, res.recs, "v4")
	waitForWaiting(t, j, 0)
}

func TestBlockingReadAndCancel(t *testing.T) {
	j, err := journal.New(2)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer j.Close()

	// 没有新数据时 Read 阻塞，Append 到达后被唤醒。
	blocked := readAsync(j, context.Background(), 0, 1)
	waitForWaiting(t, j, 1)
	if _, err := j.Append([]byte("hello")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	res := <-blocked
	if res.err != nil {
		t.Fatalf("Read: %v", res.err)
	}
	mustValues(t, res.recs, "hello")

	// 等待期间 ctx 取消只影响当前这次 Read。
	ctx, cancel := context.WithCancel(context.Background())
	blocked = readAsync(j, ctx, 1, 1)
	waitForWaiting(t, j, 1)
	cancel()
	res = <-blocked
	if !errors.Is(res.err, context.Canceled) {
		t.Fatalf("Read err = %v, want context.Canceled", res.err)
	}
	if res.recs != nil {
		t.Fatalf("canceled Read returned records: %v", res.recs)
	}
	waitForWaiting(t, j, 0)

	// Journal 本身仍然可用。
	recs, err := j.Read(context.Background(), 0, 1)
	if err != nil {
		t.Fatalf("Read after cancel: %v", err)
	}
	mustValues(t, recs, "hello")
}

func TestLaggedWhileWaiting(t *testing.T) {
	// 单 P 调度保证三次 Append 全部完成后，等待中的 reader 才重新检查窗口，
	// 从而确定性地覆盖"等待期间变成 lag"的路径。
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	j, err := journal.New(2)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer j.Close()

	blocked := readAsync(j, context.Background(), 0, 1)
	waitForWaiting(t, j, 1)

	for _, v := range []string{"a", "b", "c"} {
		if _, err := j.Append([]byte(v)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	res := <-blocked
	if !errors.Is(res.err, journal.ErrLagged) {
		t.Fatalf("Read err = %v, want ErrLagged", res.err)
	}
}

func TestCloseDrain(t *testing.T) {
	j, err := journal.New(3)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for i := 1; i <= 3; i++ {
		if _, err := j.Append([]byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	// Close 唤醒等待中的 Read：没有更多数据可读，返回 ErrClosed。
	blocked := readAsync(j, context.Background(), 3, 1)
	waitForWaiting(t, j, 1)
	j.Close()
	if res := <-blocked; !errors.Is(res.err, journal.ErrClosed) {
		t.Fatalf("blocked Read err = %v, want ErrClosed", res.err)
	}

	// Close 前保留的数据仍可继续读取。
	recs, err := j.Read(context.Background(), 0, 10)
	if err != nil {
		t.Fatalf("Read after Close: %v", err)
	}
	mustValues(t, recs, "v1", "v2", "v3")

	// 读尽后立即返回 ErrClosed，不再阻塞。
	if _, err := j.Read(context.Background(), 3, 1); !errors.Is(err, journal.ErrClosed) {
		t.Fatalf("Read err = %v, want ErrClosed", err)
	}

	// Close 后拒绝新的 Append。
	if _, err := j.Append([]byte("v4")); !errors.Is(err, journal.ErrClosed) {
		t.Fatalf("Append err = %v, want ErrClosed", err)
	}

	// 重复 Close 安全。
	j.Close()

	if !j.Snapshot().Closed {
		t.Fatal("Snapshot().Closed = false, want true")
	}
}

func TestSnapshotAndInvalidParams(t *testing.T) {
	if _, err := journal.New(0); !errors.Is(err, journal.ErrInvalidCapacity) {
		t.Fatalf("New(0) err = %v, want ErrInvalidCapacity", err)
	}
	if _, err := journal.New(-2); !errors.Is(err, journal.ErrInvalidCapacity) {
		t.Fatalf("New(-2) err = %v, want ErrInvalidCapacity", err)
	}

	j, err := journal.New(2)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer j.Close()

	if _, err := j.Read(context.Background(), 0, 0); !errors.Is(err, journal.ErrInvalidLimit) {
		t.Fatalf("Read(limit=0) err = %v, want ErrInvalidLimit", err)
	}
	if _, err := j.Read(context.Background(), 0, -1); !errors.Is(err, journal.ErrInvalidLimit) {
		t.Fatalf("Read(limit=-1) err = %v, want ErrInvalidLimit", err)
	}

	if _, err := j.Append([]byte("a")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := j.Append([]byte("b")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	snap := j.Snapshot()
	if snap.NextSeq != 3 || snap.Waiting != 0 || snap.Closed {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}
	mustValues(t, snap.Records, "a", "b")

	// Snapshot 是独立副本：修改返回值不影响 Journal。
	snap.Records[0].Value[0] = 'X'
	if got := string(j.Snapshot().Records[0].Value); got != "a" {
		t.Fatalf("snapshot shares memory with journal: %q", got)
	}

	// Append 复制输入：调用方随后修改输入不影响 Journal。
	in := []byte("cc")
	if _, err := j.Append(in); err != nil {
		t.Fatalf("Append: %v", err)
	}
	in[0] = 'X'
	snap = j.Snapshot()
	if snap.NextSeq != 4 {
		t.Fatalf("NextSeq = %d, want 4", snap.NextSeq)
	}
	mustValues(t, snap.Records, "b", "cc")

	if got := j.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2", got)
	}
	if got := j.Capacity(); got != 2 {
		t.Fatalf("Capacity = %d, want 2", got)
	}
}
