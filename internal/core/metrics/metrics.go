package metrics

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type TopicStats struct {
	PublishedCount    uint64 `json:"published_total"`
	ConsumedCount     uint64 `json:"consumed_total"`
	ActiveSubscribers int64  `json:"active_subscribers"`
	PublishRate       uint64 `json:"publish_rate_sec"` // Msg/sec
	ConsumeRate       uint64 `json:"consume_rate_sec"` // Msg/sec

	lastPub  uint64
	lastCons uint64
}

type GlobalStats struct {
	PublishedCount uint64 `json:"published_total"`
	ConsumedCount  uint64 `json:"consumed_total"`
	FailedCount    uint64 `json:"failed_total"`
	MsgPerSec      uint64 `json:"msg_per_sec"`
	BytesAllocMB   uint64 `json:"bytes_alloc_mb"`
	NumGoroutine   int    `json:"num_goroutines"`
	Uptime         string `json:"uptime"`
}

type Snapshot struct {
	Global GlobalStats            `json:"global"`
	Topics map[string]*TopicStats `json:"topics"`
}

var (
	publishedGlobal uint64
	consumedGlobal  uint64
	failedGlobal    uint64
	startTime       time.Time

	topicRegistry = make(map[string]*TopicStats)
	mu            sync.RWMutex
)

func init() {
	startTime = time.Now()
	go runRateCalculator()
}


func IncPublished(topic string, count uint64) {
	atomic.AddUint64(&publishedGlobal, count)
	getTopic(topic).incPub(count)
}

func IncPublishedBatch(topic string, count uint64) {
    atomic.AddUint64(&publishedGlobal, count)
    t := getTopic(topic) 
    t.incPub(count) 
}

func IncConsumed(topic string, count uint64) {
	atomic.AddUint64(&consumedGlobal, count)
	getTopic(topic).incCons(count)
}

func IncConsumedBatch(topic string, count uint64) {
    atomic.AddUint64(&consumedGlobal, count)
    getTopic(topic).incCons(count)
}

func IncFailed() {
	atomic.AddUint64(&failedGlobal, 1)
}

func UpdateSubscribers(topic string, delta int64) {
	getTopic(topic).updateSubs(delta)
}

func GetSnapshot() Snapshot {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	g := GlobalStats{
		PublishedCount: atomic.LoadUint64(&publishedGlobal),
		ConsumedCount:  atomic.LoadUint64(&consumedGlobal),
		FailedCount:    atomic.LoadUint64(&failedGlobal),
		BytesAllocMB:   m.Alloc / 1024 / 1024,
		NumGoroutine:   runtime.NumGoroutine(),
		Uptime:         time.Since(startTime).Truncate(time.Second).String(),
	}

	mu.RLock()
	defer mu.RUnlock()
	
	tSnapshot := make(map[string]*TopicStats)
	
	var totalRate uint64 = 0

	for name, stats := range topicRegistry {
		pubRate := atomic.LoadUint64(&stats.PublishRate)
		totalRate += pubRate

		tSnapshot[name] = &TopicStats{
			PublishedCount:    atomic.LoadUint64(&stats.PublishedCount),
			ConsumedCount:     atomic.LoadUint64(&stats.ConsumedCount),
			ActiveSubscribers: atomic.LoadInt64(&stats.ActiveSubscribers),
			PublishRate:       pubRate,
			ConsumeRate:       atomic.LoadUint64(&stats.ConsumeRate),
		}
	}
	g.MsgPerSec = totalRate

	return Snapshot{
		Global: g,
		Topics: tSnapshot,
	}
}


func getTopic(name string) *TopicStats {
	mu.RLock()
	stats, exists := topicRegistry[name]
	mu.RUnlock()

	if exists {
		return stats
	}

	mu.Lock()
	defer mu.Unlock()
	if stats, exists = topicRegistry[name]; exists {
		return stats
	}

	stats = &TopicStats{}
	topicRegistry[name] = stats
	return stats
}

func (t *TopicStats) incPub(c uint64) {
	atomic.AddUint64(&t.PublishedCount, c)
}

func (t *TopicStats) incCons(c uint64) {
	atomic.AddUint64(&t.ConsumedCount, c)
}

func (t *TopicStats) updateSubs(delta int64) {
	atomic.AddInt64(&t.ActiveSubscribers, delta)
}

func runRateCalculator() {
	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		mu.RLock()
		for _, t := range topicRegistry {
			currPub := atomic.LoadUint64(&t.PublishedCount)
			currCons := atomic.LoadUint64(&t.ConsumedCount)

			ratePub := currPub - t.lastPub
			rateCons := currCons - t.lastCons

			atomic.StoreUint64(&t.PublishRate, ratePub)
			atomic.StoreUint64(&t.ConsumeRate, rateCons)

			t.lastPub = currPub
			t.lastCons = currCons
		}
		mu.RUnlock()
	}
}