package memory

import (
	"context"
	"hash/fnv"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pedromedina19/hermes-broker/internal/adapters/secondary/storage"
	"github.com/pedromedina19/hermes-broker/internal/core/domain"
	"github.com/pedromedina19/hermes-broker/internal/core/metrics"
	"github.com/pedromedina19/hermes-broker/internal/core/ports"
)

const (
	AckTimeout = 60 * time.Second
	MaxRetries = 3
	DlqTopic   = "hermes.dlq"

	BatchSize  = 1000 // Reads large blocks of the disc.
	ShardCount = 64   // to reduce ACK lock contention
	ChannelBuf = 5000 // Output channel buffer
)

type PendingMessage struct {
	Msg    domain.Message
	SentAt time.Time
}

type Subscriber struct {
	ID            string
	GroupID       string
	Channel       chan domain.Message
	CurrentOffset uint64
	quit          chan struct{}
	// BYPASS: Channel for receiving "hot" data directly from Publish
	liveChan chan domain.Message
}

type AckShard struct {
	mu          sync.RWMutex
	pendingAcks map[string]map[string]*PendingMessage
}

type HybridBroker struct {
	store ports.LocalLogStore

	subsMu sync.RWMutex
	subs   map[string]map[string]*Subscriber

	ackShards []*AckShard

	logger ports.Logger
	ctx    context.Context
	cancel context.CancelFunc
}

func NewHybridBroker(dataDir string, bufferSize int, logger ports.Logger) *HybridBroker {
	storePath := filepath.Join(dataDir, "messages.db")
	store, err := storage.NewBoltLogStore(storePath)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	aShards := make([]*AckShard, ShardCount)
	for i := 0; i < ShardCount; i++ {
		aShards[i] = &AckShard{
			pendingAcks: make(map[string]map[string]*PendingMessage),
		}
	}

	broker := &HybridBroker{
		store:     store,
		subs:      make(map[string]map[string]*Subscriber),
		ackShards: aShards,
		logger:    logger,
		ctx:       ctx,
		cancel:    cancel,
	}

	go broker.redeliveryLoop()

	return broker
}

func (b *HybridBroker) getAckShard(subID string) *AckShard {
	h := fnv.New32a()
	h.Write([]byte(subID))
	idx := h.Sum32() % uint32(ShardCount)
	return b.ackShards[idx]
}

func (b *HybridBroker) Publish(ctx context.Context, msg domain.Message) error {
	if msg.ID == "" {
		msg.ID = uuid.New().String()
	}

	// Persistence
	_, err := b.store.Append(msg)
	if err != nil {
		return err
	}

	// Bypass (Fast Delivery)
	b.broadcastToMemory(msg)

	return nil
}

func (b *HybridBroker) broadcastToMemory(msg domain.Message) {
	b.subsMu.RLock()
	defer b.subsMu.RUnlock()

	if subs, ok := b.subs[msg.Topic]; ok {
		for _, sub := range subs {
			select {
			case sub.liveChan <- msg:
			default:
				// Buffer full, we drop it silently. The sub will pick it up from disk
			}
		}
	}
}

func (b *HybridBroker) Subscribe(ctx context.Context, topic string, groupID string) (<-chan domain.Message, string, error) {
	b.subsMu.Lock()
	defer b.subsMu.Unlock()

	if _, ok := b.subs[topic]; !ok {
		b.subs[topic] = make(map[string]*Subscriber)
	}

	ch := make(chan domain.Message, ChannelBuf)
	subID := uuid.New().String()

	var startOffset uint64

	switch groupID {
	case "":
		startOffset = b.store.LastOffset(topic) + 1
	case "__REPLAY__":
		startOffset = 0
	default:
		savedOffset, err := b.store.GetOffset(topic, groupID)
		if err != nil || savedOffset == 0 {
			startOffset = 0
		} else {
			startOffset = savedOffset + 1
		}
	}

	liveChan := make(chan domain.Message, 5000)

	sub := &Subscriber{
		ID:            subID,
		GroupID:       groupID,
		Channel:       ch,
		CurrentOffset: startOffset,
		quit:          make(chan struct{}),
		liveChan:      liveChan,
	}

	b.subs[topic][subID] = sub
	b.logger.Info("Subscriber connected", "id", subID, "start_offset", startOffset, "group", groupID)

	go b.runSubscriberPump(sub, topic)
	atomic.AddInt64(&metrics.ActiveSubscribers, 1)
	return ch, subID, nil
}

func (b *HybridBroker) runSubscriberPump(sub *Subscriber, topic string) {
	// Auto-Commit
	commitTicker := time.NewTicker(1 * time.Second)
	defer commitTicker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-sub.quit:
			return

		case <-commitTicker.C:
			if sub.GroupID != "" && sub.GroupID != "__REPLAY__" {
				b.store.SaveOffset(topic, sub.GroupID, sub.CurrentOffset-1)
			}

		// HOT PATH: Try reading from memory first
		case msg := <-sub.liveChan:
			select {
			case sub.Channel <- msg:
				b.trackPending(sub.ID, msg)
				sub.CurrentOffset++
			case <-b.ctx.Done():
				return
			case <-sub.quit:
				return
			}
			continue

		default:
		}

		// COLD PATH: Checks if we are behind schedule with the disk
		lastOffset := b.store.LastOffset(topic)
		if lastOffset >= sub.CurrentOffset {
			processedCount := b.processFromDisk(sub, topic)
			if processedCount > 0 {
				drainChannel(sub.liveChan)
				runtime.Gosched()
				continue
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (b *HybridBroker) processFromDisk(sub *Subscriber, topic string) int {
	msgs, nextOffset, err := b.store.ReadBatch(topic, sub.CurrentOffset, BatchSize)
	if err != nil {
		b.logger.Error("Disk Read Error", "err", err)
		return 0
	}

	if len(msgs) == 0 {
		return 0
	}

	for _, msg := range msgs {
		select {
		case sub.Channel <- msg:
			b.trackPending(sub.ID, msg)
		case <-b.ctx.Done():
			return 0
		case <-sub.quit:
			return 0
		}
	}

	sub.CurrentOffset = nextOffset
	return len(msgs)
}

func drainChannel(ch <-chan domain.Message) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func (b *HybridBroker) trackPending(subID string, msg domain.Message) {
	shard := b.getAckShard(subID)
	shard.mu.Lock()
	if _, ok := shard.pendingAcks[subID]; !ok {
		shard.pendingAcks[subID] = make(map[string]*PendingMessage)
	}
	shard.pendingAcks[subID][msg.ID] = &PendingMessage{
		Msg:    msg,
		SentAt: time.Now(),
	}
	shard.mu.Unlock()
	atomic.AddUint64(&metrics.ConsumedCount, 1)
}

func (b *HybridBroker) Acknowledge(subID, msgID string) {
	shard := b.getAckShard(subID)
	shard.mu.Lock()
	if msgs, ok := shard.pendingAcks[subID]; ok {
		delete(msgs, msgID)
	}
	shard.mu.Unlock()
}

func (b *HybridBroker) redeliveryLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			b.checkPendingMessages()
		}
	}
}

func (b *HybridBroker) checkPendingMessages() {
	now := time.Now()
	for _, shard := range b.ackShards {
		b.processAckShard(shard, now)
	}
}

func (b *HybridBroker) processAckShard(shard *AckShard, now time.Time) {
	type retryItem struct {
		subID string
		msg   domain.Message
	}

	shard.mu.Lock()
	var retries []retryItem
	var dlqs []domain.Message

	for subID, msgs := range shard.pendingAcks {
		for msgID, pending := range msgs {
			if now.Sub(pending.SentAt) > AckTimeout {
				pending.Msg.DeliveryAttempts++
				if pending.Msg.DeliveryAttempts > MaxRetries {
					dlqs = append(dlqs, pending.Msg)
					delete(msgs, msgID)
					continue
				}
				pending.SentAt = now
				retries = append(retries, retryItem{subID: subID, msg: pending.Msg})
			}
		}
	}
	shard.mu.Unlock()

	for _, item := range retries {
		b.sendDirect(item.subID, item.msg)
	}
	for _, msg := range dlqs {
		b.moveToDLQ(msg)
	}
}

func (b *HybridBroker) sendDirect(subID string, msg domain.Message) {
	b.subsMu.RLock()
	defer b.subsMu.RUnlock()

	if topicSubs, ok := b.subs[msg.Topic]; ok {
		if sub, ok := topicSubs[subID]; ok {
			select {
			case sub.Channel <- msg:
			default:
			}
		}
	}
}

func (b *HybridBroker) moveToDLQ(msg domain.Message) {
	dlqMsg := msg
	dlqMsg.Topic = DlqTopic
	dlqMsg.ID = "dlq-" + msg.ID

	b.subsMu.RLock()
	defer b.subsMu.RUnlock()
	if subs, ok := b.subs[DlqTopic]; ok {
		for _, sub := range subs {
			select {
			case sub.Channel <- dlqMsg:
			default:
			}
		}
	}
}

func (b *HybridBroker) Unsubscribe(topic string, subID string) {
	b.subsMu.Lock()
	defer b.subsMu.Unlock()

	if m, ok := b.subs[topic]; ok {
		if sub, ok := m[subID]; ok {
			select {
			case <-sub.quit:
			default:
				close(sub.quit)
			}
			close(sub.Channel)
			delete(m, subID)
		}
	}

	shard := b.getAckShard(subID)
	shard.mu.Lock()
	delete(shard.pendingAcks, subID)
	shard.mu.Unlock()

	b.logger.Info("Subscriber removed gracefully", "topic", topic, "id", subID)
	atomic.AddInt64(&metrics.ActiveSubscribers, -1)
}

func (b *HybridBroker) Close() {
	b.cancel()
	b.store.Close()
	b.logger.Info("Hybrid Broker shutdown complete")
}