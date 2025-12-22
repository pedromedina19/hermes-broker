package services

import (
	"context"
	"time"

	"github.com/pedromedina19/hermes-broker/internal/core/domain"
	"github.com/pedromedina19/hermes-broker/internal/core/ports"
)

type BrokerService struct {
	engine ports.BrokerEngine
	repo   ports.MessageRepository
}

func NewBrokerService(engine ports.BrokerEngine, repo ports.MessageRepository) *BrokerService {
	return &BrokerService{
		engine: engine,
		repo:   repo,
	}
}

func (s *BrokerService) Publish(ctx context.Context, topic string, payload []byte) error {
	msg := domain.Message{
		Topic:     topic,
		Payload:   payload,
		Timestamp: time.Now(),
	}

	// Durability: First we save to disk (WAL)
	if err := s.repo.Save(msg); err != nil {
		// If the disk fails, we reject the request cause don't want volatile data
		return err
	}

	// Availability: distribute in memory
	return s.engine.Publish(ctx, msg)
}

func (s *BrokerService) RecoverState() error {
	messages, err := s.repo.LoadAll()
	if err != nil {
		return err
	}

	// Replay: Plays everything back into memory engine
	ctx := context.Background()
	for _, msg := range messages {
		_ = s.engine.Publish(ctx, msg)
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
