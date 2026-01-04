package ports

import (
	"context"
	"github.com/pedromedina19/hermes-broker/internal/core/domain"
)

// define how the messaging engine should behave
type BrokerEngine interface {
    Publish(ctx context.Context, msg domain.Message) error
    PublishBatch(ctx context.Context, msgs []*domain.Message, raftIndex uint64) error    
    PublishDirect(ctx context.Context, msgs []*domain.Message) error
    Subscribe(ctx context.Context, topic string, groupID string) (<-chan domain.Message, string, error) 
    Acknowledge(subID string, msgID string)
    Unsubscribe(topic string, subID string)
    Close()
    SyncOffset(topic, groupID string, offset uint64) error
    SyncOffsetWithIndex(topic, groupID string, offset uint64, raftIndex uint64) error
	GetLastAppliedIndex() (uint64, error)
	SetReplicationCallback(fn func(topic, groupID string, offset uint64))
}
//defines how to persist messages
type MessageRepository interface {
	Save(msg domain.Message) error
	LoadAll() ([]domain.Message, error)
	Close() error
}

type LocalLogStore interface {
    Append(msg domain.Message) (uint64, error)
    AppendBatch(msgs []*domain.Message, raftIndex uint64) error
    ReadBatch(topic string, startOffset uint64, limit int) ([]domain.Message, uint64, error) 
    LastOffset(topic string) uint64
    Close() error

    SaveOffset(topic, groupID string, offset uint64, raftIndex uint64) error
    GetOffset(topic, groupID string) (uint64, error)
    GetLastRaftIndex() (uint64, error)
}

type Logger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
	Warn(msg string, args ...any)
}