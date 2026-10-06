# ring-journal

并发安全的有界内存 Ring Journal（Go 实现，仅标准库，无第三方依赖，无内部后台 goroutine）。

- 每条记录分配从 1 开始严格递增、永不重复的 \u0060uint64\u0060 Seq
- 只保留最近 \u0060capacity\u0060 条记录，写满后新记录自动覆盖最旧记录，Seq 继续递增
- 多个 reader 通过显式 \u0060afterSeq\u0060 游标独立读取，互不共享或修改彼此状态
- 无数据时 \u0060Read\u0060 阻塞等待 \u0060Append\u0060 唤醒，支持 \u0060context\u0060 取消
- reader 落后到保留窗口之外时返回 \u0060ErrLagged\u0060，绝不悄悄跳过数据
- \u0060Close\u0060 后拒绝新 \u0060Append\u0060、唤醒所有等待者，已保留数据可继续读尽

## API

| 方法 | 说明 |
| --- | --- |
| \u0060New(capacity int) (*Journal, error)\u0060 | 创建 Journal；\u0060capacity <= 0\u0060 返回 \u0060ErrInvalidCapacity\u0060 |
| \u0060Append(value []byte) (uint64, error)\u0060 | 追加记录并返回 Seq；关闭后返回 \u0060ErrClosed\u0060 |
| \u0060Read(ctx, afterSeq, limit) ([]Record, error)\u0060 | 返回 \u0060Seq > afterSeq\u0060 的升序记录，最多 \u0060limit\u0060 条 |
| \u0060Snapshot() Snapshot\u0060 | 返回保留记录的独立副本及 \u0060NextSeq\u0060/\u0060Waiting\u0060/\u0060Closed\u0060 |
| \u0060Len() int\u0060 / \u0060Capacity() int\u0060 | 当前保留条数 / 容量 |
| \u0060Close()\u0060 | 关闭并唤醒所有等待中的 \u0060Read\u0060，可重复调用 |

## 读取语义

- 已有符合条件的记录时立即返回；否则在 Journal 开启时阻塞等待新的 \u0060Append\u0060。
- \u0060afterSeq+1\u0060 小于当前最旧保留 Seq 时返回 \u0060ErrLagged\u0060；等待期间因持续 Append 而落后，重新检查时同样返回 \u0060ErrLagged\u0060。
- 等待中 \u0060ctx\u0060 取消返回 \u0060ctx.Err()\u0060，只影响当前这一次 \u0060Read\u0060。
- \u0060Close\u0060 后不存在 \u0060Seq > afterSeq\u0060 的保留记录时，\u0060Read\u0060 返回 \u0060ErrClosed\u0060（不再阻塞）。

## 快速开始

\u0060\u0060\u0060go
package main

import (
	"context"
	"fmt"

	"ringjournal/journal"
)

func main() {
	j, _ := journal.New(3)
	defer j.Close()

	seq, _ := j.Append([]byte("hello")) // seq = 1
	recs, _ := j.Read(context.Background(), 0, 10)
	fmt.Println(seq, string(recs[0].Value)) // 1 hello
}
\u0060\u0060\u0060

## 运行演示

\u0060\u0060\u0060
go run ./cmd/journal-demo
\u0060\u0060\u0060

依次演示：连续 Append、容量覆盖、limit 分批读取、一次 ErrLagged、
阻塞 Read 被 Append 唤醒、Close 后读尽剩余数据。

## 测试

\u0060\u0060\u0060
go test -race ./...
\u0060\u0060\u0060

## 明确不做

持久化、订阅管理、消费者组、ACK、重试、TTL、网络、GUI、数据库。
