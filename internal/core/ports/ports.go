package ports

import (
	"context"
	"github.com/pedromedina19/hermes-broker/internal/core/domain"
)

// define how the messaging engine should behave
type BrokerEngine interface {
	Publish(ctx context.Context, msg domain.Message) error
	Subscribe(ctx context.Context, topic string, groupID string) (<-chan domain.Message, string, error)	
	Acknowledge(subID string, msgID string)
	Unsubscribe(topic string, subID string)
	Close()
}
//defines how to persist messages
type MessageRepository interface {
	Save(msg domain.Message) error
	LoadAll() ([]domain.Message, error)
	Close() error
}

type Logger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
	Warn(msg string, args ...any)
}