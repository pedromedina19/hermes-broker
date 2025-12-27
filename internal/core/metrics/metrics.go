package metrics

import (
	"runtime"
	"sync/atomic"
	"time"
)

var (
	PublishedCount    uint64
	ConsumedCount     uint64
	FailedCount       uint64
	ActiveSubscribers int64
	StartTime         time.Time
)

func init() {
	StartTime = time.Now()
}

type StatsSnapshot struct {
	Uptime             string `json:"uptime"`
	MessagesPublished  uint64 `json:"messages_published"`
	MessagesConsumed   uint64 `json:"messages_consumed"`
	FailedPublishes    uint64 `json:"failed_publishes"`
	ActiveSubscribers  int64  `json:"active_subscribers"`
	GoRoutines         int    `json:"goroutines"`
	MemoryAllocatedMB  uint64 `json:"memory_allocated_mb"`
}

func GetSnapshot() StatsSnapshot {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return StatsSnapshot{
		Uptime:            time.Since(StartTime).String(),
		MessagesPublished: atomic.LoadUint64(&PublishedCount),
		MessagesConsumed:  atomic.LoadUint64(&ConsumedCount),
		FailedPublishes:   atomic.LoadUint64(&FailedCount),
		ActiveSubscribers: atomic.LoadInt64(&ActiveSubscribers),
		GoRoutines:        runtime.NumGoroutine(),
		MemoryAllocatedMB: m.Alloc / 1024 / 1024,
	}
}