package metrics

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Estruturas de Dados
type TopicStats struct {
	PublishedCount    uint64 `json:"published_total"`
	ConsumedCount     uint64 `json:"consumed_total"`
	ActiveSubscribers int64  `json:"active_subscribers"`
	PublishRate       uint64 `json:"publish_rate_sec"` // Msg/sec
	ConsumeRate       uint64 `json:"consume_rate_sec"` // Msg/sec

	// Contadores internos para resetar a cada segundo
	lastPub  uint64
	lastCons uint64
}

type GlobalStats struct {
	PublishedCount uint64 `json:"published_total"`
	ConsumedCount  uint64 `json:"consumed_total"`
	FailedCount    uint64 `json:"failed_total"`
	MsgPerSec      uint64 `json:"msg_per_sec"` // Throughput Global
	BytesAllocMB   uint64 `json:"bytes_alloc_mb"`
	NumGoroutine   int    `json:"num_goroutines"`
	Uptime         string `json:"uptime"`
}

type Snapshot struct {
	Global GlobalStats            `json:"global"`
	Topics map[string]*TopicStats `json:"topics"`
}

// Variáveis Globais (Atomic)
var (
	publishedGlobal uint64
	consumedGlobal  uint64
	failedGlobal    uint64
	startTime       time.Time

	// Registro de Tópicos
	topicRegistry = make(map[string]*TopicStats)
	mu            sync.RWMutex
)

func init() {
	startTime = time.Now()
	// Goroutine de cálculo de taxas (Roda a cada 1s)
	go runRateCalculator()
}

// --- API Pública de Escrita ---

func IncPublished(topic string, count uint64) {
	atomic.AddUint64(&publishedGlobal, count)
	getTopic(topic).incPub(count)
}

func IncConsumed(topic string, count uint64) {
	atomic.AddUint64(&consumedGlobal, count)
	getTopic(topic).incCons(count)
}

func IncFailed() {
	atomic.AddUint64(&failedGlobal, 1)
}

func UpdateSubscribers(topic string, delta int64) {
	getTopic(topic).updateSubs(delta)
}

// --- API Pública de Leitura ---

func GetSnapshot() Snapshot {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Snapshot Global
	g := GlobalStats{
		PublishedCount: atomic.LoadUint64(&publishedGlobal),
		ConsumedCount:  atomic.LoadUint64(&consumedGlobal),
		FailedCount:    atomic.LoadUint64(&failedGlobal),
		BytesAllocMB:   m.Alloc / 1024 / 1024,
		NumGoroutine:   runtime.NumGoroutine(),
		Uptime:         time.Since(startTime).Truncate(time.Second).String(),
	}

	// Snapshot dos Tópicos (Com Lock de Leitura para evitar Race na iteração)
	mu.RLock()
	defer mu.RUnlock()
	
	tSnapshot := make(map[string]*TopicStats)
	
	// Calculando taxa global baseada na soma das taxas dos tópicos
	var totalRate uint64 = 0

	for name, stats := range topicRegistry {
		// Copia o valor atual para o snapshot
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

// --- Helpers Internos ---

func getTopic(name string) *TopicStats {
	mu.RLock()
	stats, exists := topicRegistry[name]
	mu.RUnlock()

	if exists {
		return stats
	}

	mu.Lock()
	defer mu.Unlock()
	// Double check
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

// Calculadora de Throughput (Roda em Background)
func runRateCalculator() {
	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		mu.RLock()
		for _, t := range topicRegistry {
			// Snapshot dos totais atuais
			currPub := atomic.LoadUint64(&t.PublishedCount)
			currCons := atomic.LoadUint64(&t.ConsumedCount)

			// Calcula o delta (O que aconteceu nesse último segundo)
			// Nota: Não usamos lock aqui pois lastPub é acessado apenas por essa goroutine
			ratePub := currPub - t.lastPub
			rateCons := currCons - t.lastCons

			// Atualiza as taxas e os pontos de referência
			atomic.StoreUint64(&t.PublishRate, ratePub)
			atomic.StoreUint64(&t.ConsumeRate, rateCons)

			t.lastPub = currPub
			t.lastCons = currCons
		}
		mu.RUnlock()
	}
}