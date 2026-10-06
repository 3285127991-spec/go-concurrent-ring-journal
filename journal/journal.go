// Package journal 提供并发安全的有界内存 Ring Journal。
//
// Journal 按追加顺序为每条记录分配从 1 开始严格递增、永不重复的 Seq，
// 只保留最近 capacity 条记录，写满后最旧记录被新记录覆盖。
// 多个 reader 通过显式 afterSeq 游标独立读取，互不影响。
// 实现不启动任何后台 goroutine。
package journal

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrClosed 表示 Journal 已关闭：Append 被拒绝；Read 在没有更多
	// 保留记录可读时返回该错误。
	ErrClosed = errors.New("journal: closed")
	// ErrLagged 表示 reader 的 afterSeq 已落后到保留窗口之外，
	// 无法从当前窗口连续读取，数据已被覆盖。
	ErrLagged = errors.New("journal: reader lagged behind retained window")
	// ErrInvalidCapacity 表示创建 Journal 时 capacity 非法。
	ErrInvalidCapacity = errors.New("journal: capacity must be greater than zero")
	// ErrInvalidLimit 表示 Read 的 limit 非法。
	ErrInvalidLimit = errors.New("journal: limit must be greater than zero")
)

// Record 是 Journal 中保存的一条记录。
type Record struct {
	Seq   uint64
	Value []byte
}

// Snapshot 是 Journal 某一时刻状态的一致性快照。
type Snapshot struct {
	Records []Record // 当前保留记录，按 Seq 升序的独立副本
	NextSeq uint64   // 下一条 Append 将分配的 Seq
	Waiting int      // 正在阻塞等待的 Read 数量
	Closed  bool     // 是否已关闭
}

// Journal 是并发安全的有界环形日志。
type Journal struct {
	mu      sync.Mutex
	buf     []Record // 定长环形缓冲区，Seq 为 s 的记录存放在 slot (s-1)%cap
	count   int      // 当前保留记录数
	nextSeq uint64   // 下一条待分配的 Seq，从 1 开始
	closed  bool
	waiting int
	changed chan struct{} // 每次 Append/Close 时关闭并替换，用于广播唤醒
}

// New 创建容量为 capacity 的 Journal，capacity 必须大于 0。
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

// Append 追加一条记录并返回分配的 Seq；Journal 关闭后返回 ErrClosed。
// 写满后自动覆盖最旧记录，Seq 继续递增。value 会被复制，调用方可安全复用。
func (j *Journal) Append(value []byte) (uint64, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return 0, ErrClosed
	}
	seq := j.nextSeq
	j.buf[j.slot(seq)] = Record{Seq: seq, Value: cloneBytes(value)}
	j.nextSeq++
	if j.count < len(j.buf) {
		j.count++
	}
	close(j.changed)
	j.changed = make(chan struct{})
	return seq, nil
}

// Read 返回 Seq > afterSeq 的保留记录，按 Seq 严格升序，最多 limit 条。
//
// 已有符合条件的记录时立即返回；否则在 Journal 开启时阻塞等待新的
// Append，直到有数据可读、ctx 取消（返回 ctx.Err()）或 Journal 关闭
// （返回 ErrClosed）。若 afterSeq+1 已小于当前最旧保留 Seq，返回
// ErrLagged；等待期间因持续 Append 而落后时，重新检查同样返回 ErrLagged。
func (j *Journal) Read(ctx context.Context, afterSeq uint64, limit int) ([]Record, error) {
	if limit <= 0 {
		return nil, ErrInvalidLimit
	}
	for {
		j.mu.Lock()
		if j.count > 0 {
			oldest := j.nextSeq - uint64(j.count)
			if afterSeq < oldest-1 {
				j.mu.Unlock()
				return nil, ErrLagged
			}
			if newest := j.nextSeq - 1; newest > afterSeq {
				n := newest - afterSeq
				if uint64(limit) < n {
					n = uint64(limit)
				}
				recs := make([]Record, 0, n)
				for seq := afterSeq + 1; seq <= afterSeq+n; seq++ {
					rec := j.buf[j.slot(seq)]
					recs = append(recs, Record{Seq: rec.Seq, Value: cloneBytes(rec.Value)})
				}
				j.mu.Unlock()
				return recs, nil
			}
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

// Snapshot 返回当前状态的一致性独立副本，调用方修改返回值不影响 Journal。
func (j *Journal) Snapshot() Snapshot {
	j.mu.Lock()
	defer j.mu.Unlock()
	snap := Snapshot{
		NextSeq: j.nextSeq,
		Waiting: j.waiting,
		Closed:  j.closed,
	}
	if j.count > 0 {
		snap.Records = make([]Record, 0, j.count)
		for seq := j.nextSeq - uint64(j.count); seq < j.nextSeq; seq++ {
			rec := j.buf[j.slot(seq)]
			snap.Records = append(snap.Records, Record{Seq: rec.Seq, Value: cloneBytes(rec.Value)})
		}
	}
	return snap
}

// Len 返回当前实际保留的记录数。
func (j *Journal) Len() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.count
}

// Capacity 返回创建时指定的容量。
func (j *Journal) Capacity() int {
	return len(j.buf)
}

// Close 关闭 Journal：拒绝后续 Append，唤醒所有等待中的 Read。
// 已保留的数据仍可继续读取，读尽后 Read 返回 ErrClosed。重复调用安全。
func (j *Journal) Close() {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return
	}
	j.closed = true
	close(j.changed)
	j.changed = make(chan struct{})
}

// slot 返回 Seq 在环形缓冲区中的下标。
func (j *Journal) slot(seq uint64) int {
	return int((seq - 1) % uint64(len(j.buf)))
}

func cloneBytes(b []byte) []byte {
	return append([]byte(nil), b...)
}
