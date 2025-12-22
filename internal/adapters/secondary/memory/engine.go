package memory

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pedromedina19/hermes-broker/internal/core/domain"
	"github.com/pedromedina19/hermes-broker/internal/core/ports"
)

const (
	AckTimeout = 5 * time.Second // If don't confirm within 5 seconds, resend
	MaxRetries = 3               // Try 3 times before giving up
	DlqTopic   = "hermes.dlq"    // Topic Where dead messages go
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
	subIDs []string // Ordered list of subscriber IDs for this group
	next   int      // Index of next subscriber to receive (0 to len-1)
}

type MemoryBroker struct {
	subscribers map[string]map[string]*Subscriber
	groupStates map[string]map[string]*GroupRoundRobin
	pendingAcks map[string]map[string]*PendingMessage

	mu         sync.RWMutex
	bufferSize int
	logger     ports.Logger

	// redelivery loop control
	ctx    context.Context
	cancel context.CancelFunc
}

func NewMemoryBroker(bufferSize int, logger ports.Logger) *MemoryBroker {
	ctx, cancel := context.WithCancel(context.Background())

	broker := &MemoryBroker{
		subscribers: make(map[string]map[string]*Subscriber),
		groupStates: make(map[string]map[string]*GroupRoundRobin),
		pendingAcks: make(map[string]map[string]*PendingMessage),
		bufferSize:  bufferSize,
		logger:      logger,
		ctx:         ctx,
		cancel:      cancel,
	}

	go broker.redeliveryLoop()

	return broker
}

func (b *MemoryBroker) Publish(ctx context.Context, msg domain.Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	subs, ok := b.subscribers[msg.Topic]
	if !ok || len(subs) == 0 {
		return nil
	}

	if msg.ID == "" {
		msg.ID = uuid.New().String()
	}

	// Control to ensure that only ONE subscriber per group receives the message
	groupsProcessed := make(map[string]bool)

	for _, sub := range subs {

		targetSub := sub

		if sub.GroupID != "" {
			if groupsProcessed[sub.GroupID] {
				continue
			}
			groupsProcessed[sub.GroupID] = true
			targetID := b.getNextRoundRobinSub(msg.Topic, sub.GroupID)
			if targetID == "" {
				targetSub = sub
			} else {
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

func (b *MemoryBroker) getNextRoundRobinSub(topic, groupID string) string {
	if states, ok := b.groupStates[topic]; ok {
		if state, ok := states[groupID]; ok {
			if len(state.subIDs) == 0 {
				return ""
			}

			id := state.subIDs[state.next]

			// rotate pointer: (0 -> 1 -> 2 -> 0 ...)
			state.next = (state.next + 1) % len(state.subIDs)

			return id
		}
	}
	return ""
}

func (b *MemoryBroker) trackPending(subID string, msg domain.Message) {

	if _, ok := b.pendingAcks[subID]; !ok {
		b.pendingAcks[subID] = make(map[string]*PendingMessage)
	}

	b.pendingAcks[subID][msg.ID] = &PendingMessage{
		Msg:    msg,
		SentAt: time.Now(),
	}
}

func (b *MemoryBroker) Acknowledge(subID, msgID string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if msgs, ok := b.pendingAcks[subID]; ok {
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
	// In production, we would use lock sharding
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()

	for subID, msgs := range b.pendingAcks {
		for msgID, pending := range msgs {
			if now.Sub(pending.SentAt) > AckTimeout {

				pending.Msg.DeliveryAttempts++

				if pending.Msg.DeliveryAttempts > MaxRetries {
					b.logger.Error("Message Dead (Max Retries)", "msgID", msgID, "sub", subID)
					b.moveToDLQ(pending.Msg)
					delete(msgs, msgID) // Remove from local pending
					continue
				}

				// retry
				b.logger.Warn("Redelivering message...", "msgID", msgID, "attempt", pending.Msg.DeliveryAttempts)
				pending.SentAt = now

				if ch := b.findSubscriberChannel(pending.Msg.Topic, subID); ch != nil {
					select {
					case ch <- pending.Msg:
						// success retry
					default:
						// Channel full, we'll try again on the next tick
					}
				} else {
					// Subscriber disappeared, remove pending item
					delete(msgs, msgID)
				}
			}
		}
	}
}

func (b *MemoryBroker) findSubscriberChannel(topic, subID string) chan domain.Message {
	if subs, ok := b.subscribers[topic]; ok {
		if sub, ok := subs[subID]; ok {
			return sub.Channel
		}
	}
	return nil
}

func (b *MemoryBroker) moveToDLQ(msg domain.Message) {
	dlqMsg := msg
	dlqMsg.Topic = DlqTopic
	dlqMsg.ID = "dlq-" + msg.ID

	if subs, ok := b.subscribers[DlqTopic]; ok {
		for _, sub := range subs {
			select {
			case sub.Channel <- dlqMsg:
			default:
			}
		}
	}
}

func (b *MemoryBroker) Subscribe(ctx context.Context, topic string, groupID string) (<-chan domain.Message, string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.subscribers[topic]; !ok {
		b.subscribers[topic] = make(map[string]*Subscriber)
	}

	if _, ok := b.groupStates[topic]; !ok {
		b.groupStates[topic] = make(map[string]*GroupRoundRobin)
	}

	ch := make(chan domain.Message, b.bufferSize)
	subID := uuid.New().String()

	b.subscribers[topic][subID] = &Subscriber{
		ID:      subID,
		GroupID: groupID,
		Channel: ch,
	}

	if groupID != "" {
		if _, ok := b.groupStates[topic][groupID]; !ok {
			b.groupStates[topic][groupID] = &GroupRoundRobin{
				subIDs: make([]string, 0),
				next:   0,
			}
		}
		b.groupStates[topic][groupID].subIDs = append(b.groupStates[topic][groupID].subIDs, subID)
	}

	b.pendingAcks[subID] = make(map[string]*PendingMessage)

	b.logger.Info("New subscriber added", "topic", topic, "id", subID, "group", groupID)
	return ch, subID, nil
}

func (b *MemoryBroker) Unsubscribe(topic string, subID string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if subs, ok := b.subscribers[topic]; ok {
		if sub, exists := subs[subID]; exists {
			if sub.GroupID != "" {
				b.removeFromGroupState(topic, sub.GroupID, subID)
			}
			close(sub.Channel)
			delete(subs, subID)
		}
		if len(subs) == 0 {
			delete(b.subscribers, topic)
			delete(b.groupStates, topic)
		}
	}
	delete(b.pendingAcks, subID)
	b.logger.Info("Subscriber removed", "topic", topic, "id", subID)
}

func (b *MemoryBroker) removeFromGroupState(topic, groupID, subID string) {
	if states, ok := b.groupStates[topic]; ok {
		if state, ok := states[groupID]; ok {
			// Find the subscriber's index in the list
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
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, subs := range b.subscribers {
		for _, sub := range subs {
			close(sub.Channel)
		}
	}
	b.subscribers = nil
	b.pendingAcks = nil
	b.logger.Info("Memory Broker engine shutdown complete")
}
