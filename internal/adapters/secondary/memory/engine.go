package memory

import (
	"context"
	"hash/fnv"
	"path/filepath"
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
	AckTimeout = 30 * time.Minute
	MaxRetries = 10
	DlqTopic   = "hermes.dlq"

	BatchSize  = 2000
	ShardCount = 64
	ChannelBuf = 100000
)

type PendingMessage struct {
	Msg    domain.Message
	SentAt time.Time
}

type Subscriber struct {
	ID      string
	GroupID string
	Channel chan domain.Message

	CurrentOffset uint64
	AckedCount    uint64
	StartOffset   uint64

	quit chan struct{}
}

type AckShard struct {
	mu          sync.RWMutex
	pendingAcks map[string]map[string]*PendingMessage
}

type HybridBroker struct {
	store           ports.LocalLogStore
	subsMu          sync.RWMutex
	subs            map[string]map[string]*Subscriber
	ackShards       []*AckShard
	logger          ports.Logger
	ctx             context.Context
	cancel          context.CancelFunc
	replicationFunc func(topic, groupID string, offset uint64)
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

func (b *HybridBroker) SetReplicationCallback(fn func(topic, groupID string, offset uint64)) {
	b.replicationFunc = fn
}

func (b *HybridBroker) SyncOffset(topic, groupID string, offset uint64) error {
	return b.store.SaveOffset(topic, groupID, offset, 0)
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
	_, err := b.store.Append(msg)
	return err
}

func (b *HybridBroker) GetLastAppliedIndex() (uint64, error) {
	return b.store.GetLastRaftIndex()
}

func (b *HybridBroker) PublishBatch(ctx context.Context, msgs []*domain.Message, raftIndex uint64) error {
	for _, msg := range msgs {
		if msg.ID == "" {
			msg.ID = uuid.New().String()
		}
	}
	return b.store.AppendBatch(msgs, raftIndex)
}

func (b *HybridBroker) PublishDirect(ctx context.Context, msgs []*domain.Message) error {
	b.subsMu.RLock()

	deliveryMap := make(map[*Subscriber][]*domain.Message)
	topicCounts := make(map[string]uint64)

	for _, msg := range msgs {
		if topicSubs, ok := b.subs[msg.Topic]; ok {
			for _, sub := range topicSubs {
				deliveryMap[sub] = append(deliveryMap[sub], msg)
			}
		}
		topicCounts[msg.Topic]++
	}

	b.subsMu.RUnlock()

	for sub, batch := range deliveryMap {
		b.trackPendingBatch(sub.ID, batch)

		for _, msg := range batch {
			msgCopy := *msg

			select {
			case sub.Channel <- msgCopy:
				// Success
			case <-b.ctx.Done():
				return b.ctx.Err()
			case <-sub.quit:
				continue
			}
		}
	}

	for topic, count := range topicCounts {
		metrics.IncConsumedBatch(topic, count)
	}

	return nil
}

func (b *HybridBroker) trackPendingBatch(subID string, msgs []*domain.Message) {
	shard := b.getAckShard(subID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if _, ok := shard.pendingAcks[subID]; !ok {
		shard.pendingAcks[subID] = make(map[string]*PendingMessage)
	}

	now := time.Now()
	pendingMap := shard.pendingAcks[subID]

	for _, msg := range msgs {
		pendingMap[msg.ID] = &PendingMessage{
			Msg:    *msg,
			SentAt: now,
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

	sub := &Subscriber{
		ID:            subID,
		GroupID:       groupID,
		Channel:       ch,
		CurrentOffset: startOffset,
		StartOffset:   startOffset,
		quit:          make(chan struct{}),
	}

	b.subs[topic][subID] = sub
	b.logger.Info("Subscriber connected (Disk Mode)", "id", subID, "start_offset", startOffset, "group", groupID)

	go b.runDiskPump(sub, topic)
	go b.runOffsetCommitter(sub, topic)

	metrics.UpdateSubscribers(topic, 1)
	return ch, subID, nil
}

func (b *HybridBroker) runDiskPump(sub *Subscriber, topic string) {
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-sub.quit:
			return
		default:
		}

		current := atomic.LoadUint64(&sub.CurrentOffset)

		msgs, nextOffset, err := b.store.ReadBatch(topic, current, BatchSize)
		if err != nil {
			b.logger.Error("Disk Read Error", "err", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if len(msgs) == 0 {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		for _, msg := range msgs {
			b.trackPending(sub.ID, msg)

			select {
			case sub.Channel <- msg:
				// success
			case <-b.ctx.Done():
				return
			case <-sub.quit:
				return
			}
		}

		atomic.StoreUint64(&sub.CurrentOffset, nextOffset)
	}
}

func (b *HybridBroker) runOffsetCommitter(sub *Subscriber, topic string) {
	if sub.GroupID == "" || sub.GroupID == "__REPLAY__" {
		return
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-sub.quit:
			return
		case <-ticker.C:
			acked := atomic.LoadUint64(&sub.AckedCount)
			if acked > 0 {
				newOffset := sub.StartOffset + acked - 1
				if b.replicationFunc != nil {
					b.replicationFunc(topic, sub.GroupID, newOffset)
				} else {
					err := b.store.SaveOffset(topic, sub.GroupID, newOffset, 0)
					if err != nil {
						b.logger.Error("Failed to save offset locally", "err", err)
					}
				}
			}
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
	metrics.IncConsumed(msg.Topic, 1)
}

func (b *HybridBroker) Acknowledge(subID, msgID string) {
	shard := b.getAckShard(subID)
	shard.mu.Lock()

	if msgs, ok := shard.pendingAcks[subID]; ok {
		if _, exists := msgs[msgID]; exists {
			delete(msgs, msgID)
			shard.mu.Unlock()

			b.subsMu.RLock()
			for _, subs := range b.subs {
				if sub, ok := subs[subID]; ok {
					atomic.AddUint64(&sub.AckedCount, 1)
					break
				}
			}
			b.subsMu.RUnlock()
			return
		}
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
	metrics.UpdateSubscribers(topic, -1)
}

func (b *HybridBroker) SyncOffsetWithIndex(topic, groupID string, offset uint64, raftIndex uint64) error {
	return b.store.SaveOffset(topic, groupID, offset, raftIndex)
}

func (b *HybridBroker) Close() {
	b.cancel()
	b.store.Close()
	b.logger.Info("Hybrid Broker shutdown complete")
}
