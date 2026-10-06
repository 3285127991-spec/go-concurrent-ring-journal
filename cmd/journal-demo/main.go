// journal-demo 演示并发有界 Ring Journal 的核心行为：
// 连续 Append、容量覆盖、limit 分批读取、ErrLagged、
// 阻塞等待被 Append 唤醒，以及 Close 后读取剩余数据。
package main

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"time"

	"ringjournal/journal"
)

func main() {
	j, err := journal.New(3)
	must(err)

	fmt.Println("== 1. 连续 Append（capacity=3）==")
	for i := 1; i <= 3; i++ {
		seq, err := j.Append([]byte(fmt.Sprintf("msg-%d", i)))
		must(err)
		fmt.Printf("  Append -> seq=%d value=msg-%d\n", seq, i)
	}
	fmt.Printf("  Len=%d Capacity=%d\n", j.Len(), j.Capacity())

	fmt.Println("\n== 2. 容量覆盖（再 Append 两条，最旧记录被覆盖）==")
	for i := 4; i <= 5; i++ {
		seq, err := j.Append([]byte(fmt.Sprintf("msg-%d", i)))
		must(err)
		fmt.Printf("  Append -> seq=%d value=msg-%d\n", seq, i)
	}
	snap := j.Snapshot()
	fmt.Printf("  保留窗口: %s NextSeq=%d\n", formatRecords(snap.Records), snap.NextSeq)

	fmt.Println("\n== 3. limit 分批读取（limit=2）==")
	after := uint64(2)
	for {
		recs, err := j.Read(context.Background(), after, 2)
		must(err)
		fmt.Printf("  Read(afterSeq=%d, limit=2) -> %s\n", after, formatRecords(recs))
		after = recs[len(recs)-1].Seq
		if len(recs) < 2 {
			break
		}
	}

	fmt.Println("\n== 4. 落后读取 -> ErrLagged ==")
	_, err = j.Read(context.Background(), 1, 2)
	fmt.Printf("  Read(afterSeq=1) -> err: %v\n", err)

	fmt.Println("\n== 5. 阻塞 Read 被 Append 唤醒 ==")
	type result struct {
		recs []journal.Record
		err  error
	}
	resCh := make(chan result, 1)
	go func() {
		recs, err := j.Read(context.Background(), 5, 1)
		resCh <- result{recs: recs, err: err}
	}()
	waitForWaiting(j)
	fmt.Println("  reader 阻塞等待 Seq>5 ...")
	seq, err := j.Append([]byte("msg-6"))
	must(err)
	fmt.Printf("  Append -> seq=%d value=msg-6\n", seq)
	res := <-resCh
	must(res.err)
	fmt.Printf("  阻塞的 Read 返回: %s\n", formatRecords(res.recs))

	fmt.Println("\n== 6. Close 后读取剩余数据 ==")
	j.Close()
	if _, err := j.Append([]byte("msg-7")); err != nil {
		fmt.Printf("  Close 后 Append -> err: %v\n", err)
	}
	recs, err := j.Read(context.Background(), 5, 10)
	must(err)
	fmt.Printf("  Close 后 Read(afterSeq=5) -> %s\n", formatRecords(recs))
	_, err = j.Read(context.Background(), 6, 1)
	fmt.Printf("  数据读尽后 Read(afterSeq=6) -> err: %v\n", err)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func formatRecords(recs []journal.Record) string {
	parts := make([]string, len(recs))
	for i, rec := range recs {
		parts[i] = fmt.Sprintf("{seq:%d %s}", rec.Seq, rec.Value)
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func waitForWaiting(j *journal.Journal) {
	deadline := time.Now().Add(5 * time.Second)
	for j.Snapshot().Waiting == 0 {
		if time.Now().After(deadline) {
			panic("timeout waiting for reader to block")
		}
		runtime.Gosched()
	}
}
