package memory

import (
	"context"
	"hash/fnv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pedromedina19/hermes-broker/internal/core/domain"
	"github.com/pedromedina19/hermes-broker/internal/core/ports"
)

const (
	AckTimeout = 5 * time.Second
	MaxRetries = 3
	DlqTopic   = "hermes.dlq"

	// to reduce lock containment
	ShardCount = 32
)

type PendingMessage struct {
	Msg    domain.Message
	SentAt time.Time
}

type Subscriber struct {
	ID      string
	GroupID string
	Channel chan domain.Message
}

type GroupRoundRobin struct {
	subIDs []string
	next   int
}

// TopicShard protects subscriber and group tree.
type TopicShard struct {
	mu          sync.RWMutex
	subscribers map[string]map[string]*Subscriber      // topic -> subID -> sub
	groupStates map[string]map[string]*GroupRoundRobin // topic -> groupID -> state
}

// AckShard protects pending issues
type AckShard struct {
	mu          sync.RWMutex
	pendingAcks map[string]map[string]*PendingMessage // subID -> msgID -> Pending
}

type MemoryBroker struct {
	topicShards []*TopicShard
	ackShards   []*AckShard

	bufferSize int
	logger     ports.Logger

	ctx    context.Context
	cancel context.CancelFunc
}

func NewMemoryBroker(bufferSize int, logger ports.Logger) *MemoryBroker {
	ctx, cancel := context.WithCancel(context.Background())

	tShards := make([]*TopicShard, ShardCount)
	aShards := make([]*AckShard, ShardCount)

	for i := 0; i < ShardCount; i++ {
		tShards[i] = &TopicShard{
			subscribers: make(map[string]map[string]*Subscriber),
			groupStates: make(map[string]map[string]*GroupRoundRobin),
		}
		aShards[i] = &AckShard{
			pendingAcks: make(map[string]map[string]*PendingMessage),
		}
	}

	broker := &MemoryBroker{
		topicShards: tShards,
		ackShards:   aShards,
		bufferSize:  bufferSize,
		logger:      logger,
		ctx:         ctx,
		cancel:      cancel,
	}

	go broker.redeliveryLoop()

	return broker
}

func getShardIndex(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32() % ShardCount
}

func (b *MemoryBroker) getTopicShard(topic string) *TopicShard {
	return b.topicShards[getShardIndex(topic)]
}

func (b *MemoryBroker) getAckShard(subID string) *AckShard {
	return b.ackShards[getShardIndex(subID)]
}

func (b *MemoryBroker) Publish(ctx context.Context, msg domain.Message) error {
	shard := b.getTopicShard(msg.Topic)

	// need full locking of the Shard to ensure RoundRobin consistency
	shard.mu.Lock()
	defer shard.mu.Unlock()

	subs, ok := shard.subscribers[msg.Topic]
	if !ok || len(subs) == 0 {
		return nil
	}

	if msg.ID == "" {
		msg.ID = uuid.New().String()
	}

	groupsProcessed := make(map[string]bool)

	for _, sub := range subs {
		targetSub := sub

		// Round-Robin Logic
		if sub.GroupID != "" {
			if groupsProcessed[sub.GroupID] {
				continue
			}
			groupsProcessed[sub.GroupID] = true

			// Internal logic of RR accessing the groupStates map of the same Shard
			targetID := b.getNextRoundRobinSubUnsafe(shard, msg.Topic, sub.GroupID)
			if targetID != "" {
				if s, exists := subs[targetID]; exists {
					targetSub = s
				}
			}
		}

		msgToSend := msg
		select {
		case targetSub.Channel <- msgToSend:
			b.trackPending(targetSub.ID, msgToSend)
		default:
			b.logger.Warn("Dropped (Buffer Full)", "sub", targetSub.ID)
		}
	}
	return nil
}

func (b *MemoryBroker) getNextRoundRobinSubUnsafe(shard *TopicShard, topic, groupID string) string {
	if states, ok := shard.groupStates[topic]; ok {
		if state, ok := states[groupID]; ok {
			if len(state.subIDs) == 0 {
				return ""
			}
			id := state.subIDs[state.next]
			state.next = (state.next + 1) % len(state.subIDs)
			return id
		}
	}
	return ""
}

func (b *MemoryBroker) trackPending(subID string, msg domain.Message) {
	shard := b.getAckShard(subID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if _, ok := shard.pendingAcks[subID]; !ok {
		shard.pendingAcks[subID] = make(map[string]*PendingMessage)
	}

	shard.pendingAcks[subID][msg.ID] = &PendingMessage{
		Msg:    msg,
		SentAt: time.Now(),
	}
}

func (b *MemoryBroker) Acknowledge(subID, msgID string) {
	shard := b.getAckShard(subID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if msgs, ok := shard.pendingAcks[subID]; ok {
		if pending, exists := msgs[msgID]; exists {
			latency := time.Since(pending.SentAt)
			delete(msgs, msgID)
			b.logger.Info("ACK Received", "sub", subID, "latency", latency, "attempts", pending.Msg.DeliveryAttempts)
		}
	}
}

func (b *MemoryBroker) redeliveryLoop() {
	ticker := time.NewTicker(1 * time.Second)
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

func (b *MemoryBroker) checkPendingMessages() {
	now := time.Now()
	// processes each shard independently so it doesn`t block everything
	for _, shard := range b.ackShards {
		b.processAckShard(shard, now)
	}
}

func (b *MemoryBroker) processAckShard(shard *AckShard, now time.Time) {
	// Auxiliar structure to prevent holding the lock during retransmission
	type retryItem struct {
		subID string
		msg   domain.Message
	}

	shard.mu.Lock()

	var retries []retryItem
	var dlqs []domain.Message

	// Identify what needs to be done under Lock.
	for subID, msgs := range shard.pendingAcks {
		for msgID, pending := range msgs {
			if now.Sub(pending.SentAt) > AckTimeout {
				pending.Msg.DeliveryAttempts++

				if pending.Msg.DeliveryAttempts > MaxRetries {
					b.logger.Error("Message Dead (Max Retries)", "msgID", msgID, "sub", subID)
					dlqs = append(dlqs, pending.Msg)
					delete(msgs, msgID)
					continue
				}

				b.logger.Warn("Redelivering message...", "msgID", msgID, "attempt", pending.Msg.DeliveryAttempts)
				pending.SentAt = now // timer reset

				// Add to temporary list to process AFTER Unlock
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

func (b *MemoryBroker) sendDirect(subID string, msg domain.Message) {
	shard := b.getTopicShard(msg.Topic)

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	if subs, ok := shard.subscribers[msg.Topic]; ok {
		if sub, ok := subs[subID]; ok {
			select {
			case sub.Channel <- msg:
				// success
			default:
				// full channel
			}
		}
	}
}

func (b *MemoryBroker) moveToDLQ(msg domain.Message) {
	dlqMsg := msg
	dlqMsg.Topic = DlqTopic
	dlqMsg.ID = "dlq-" + msg.ID

	shard := b.getTopicShard(DlqTopic)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	if subs, ok := shard.subscribers[DlqTopic]; ok {
		for _, sub := range subs {
			select {
			case sub.Channel <- dlqMsg:
			default:
			}
		}
	}
}

func (b *MemoryBroker) Subscribe(ctx context.Context, topic string, groupID string) (<-chan domain.Message, string, error) {
	shard := b.getTopicShard(topic)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if _, ok := shard.subscribers[topic]; !ok {
		shard.subscribers[topic] = make(map[string]*Subscriber)
	}

	if _, ok := shard.groupStates[topic]; !ok {
		shard.groupStates[topic] = make(map[string]*GroupRoundRobin)
	}

	ch := make(chan domain.Message, b.bufferSize)
	subID := uuid.New().String()

	shard.subscribers[topic][subID] = &Subscriber{
		ID:      subID,
		GroupID: groupID,
		Channel: ch,
	}

	if groupID != "" {
		if _, ok := shard.groupStates[topic][groupID]; !ok {
			shard.groupStates[topic][groupID] = &GroupRoundRobin{
				subIDs: make([]string, 0),
				next:   0,
			}
		}
		shard.groupStates[topic][groupID].subIDs = append(shard.groupStates[topic][groupID].subIDs, subID)
	}

	// Pre-allocate the map in AckShard to avoid a null check later
	ackShard := b.getAckShard(subID)
	ackShard.mu.Lock()
	if _, ok := ackShard.pendingAcks[subID]; !ok {
		ackShard.pendingAcks[subID] = make(map[string]*PendingMessage)
	}
	ackShard.mu.Unlock()

	b.logger.Info("New subscriber added", "topic", topic, "id", subID, "group", groupID)
	return ch, subID, nil
}

func (b *MemoryBroker) Unsubscribe(topic string, subID string) {
	shard := b.getTopicShard(topic)
	shard.mu.Lock()

	if subs, ok := shard.subscribers[topic]; ok {
		if sub, exists := subs[subID]; exists {
			if sub.GroupID != "" {
				b.removeFromGroupState(shard, topic, sub.GroupID, subID)
			}
			close(sub.Channel)
			delete(subs, subID)
		}
		if len(subs) == 0 {
			delete(shard.subscribers, topic)
			delete(shard.groupStates, topic)
		}
	}
	shard.mu.Unlock()

	ackShard := b.getAckShard(subID)
	ackShard.mu.Lock()
	delete(ackShard.pendingAcks, subID)
	ackShard.mu.Unlock()

	b.logger.Info("Subscriber removed", "topic", topic, "id", subID)
}

func (b *MemoryBroker) removeFromGroupState(shard *TopicShard, topic, groupID, subID string) {
	if states, ok := shard.groupStates[topic]; ok {
		if state, ok := states[groupID]; ok {
			for i, id := range state.subIDs {
				if id == subID {
					state.subIDs = append(state.subIDs[:i], state.subIDs[i+1:]...)
					if i < state.next {
						state.next--
					}
					if state.next >= len(state.subIDs) {
						state.next = 0
					}
					break
				}
			}
			if len(state.subIDs) == 0 {
				delete(states, groupID)
			}
		}
	}
}

func (b *MemoryBroker) Close() {
	b.cancel()

	for _, shard := range b.topicShards {
		shard.mu.Lock()
		for _, subs := range shard.subscribers {
			for _, sub := range subs {
				close(sub.Channel)
			}
		}
		shard.mu.Unlock()
	}

	b.logger.Info("Memory Broker engine shutdown complete")
}
