package services

import (
	"context"
	"fmt"
	"time"

	"github.com/pedromedina19/hermes-broker/internal/core/domain"
	"github.com/pedromedina19/hermes-broker/internal/core/ports"
)

type ConsensusNode interface {
	ApplyMessage(msg domain.Message) error
	Join(nodeID, raftAddr string) error
}
type BrokerService struct {
	engine ports.BrokerEngine
	consensus ConsensusNode
}

func NewBrokerService(engine ports.BrokerEngine, consensus ConsensusNode) *BrokerService {
	return &BrokerService{
		engine:    engine,
		consensus: consensus,
	}
}

func (s *BrokerService) Publish(ctx context.Context, topic string, payload []byte) error {
	msg := domain.Message{
		Topic:     topic,
		Payload:   payload,
		Timestamp: time.Now(),
	}

	// RAFT: We send it to consensus
	// If it's a leader, it replicates and then applies it to the local engine
	// If it's a follower, it will return a "not leader" error
	if err := s.consensus.ApplyMessage(msg); err != nil {
		return fmt.Errorf("consensus error (try leader): %w", err)
	}

	return nil
}

func (s *BrokerService) Acknowledge(subID, msgID string) {
	s.engine.Acknowledge(subID, msgID)
}

func (s *BrokerService) Subscribe(ctx context.Context, topic string, groupID string) (<-chan domain.Message, string, error) {
	return s.engine.Subscribe(ctx, topic, groupID)
}

func (s *BrokerService) RemoveSubscriber(topic, subID string) {
	s.engine.Unsubscribe(topic, subID)
}

func (s *BrokerService) JoinCluster(nodeID, addr string) error {
	return s.consensus.Join(nodeID, addr)
}