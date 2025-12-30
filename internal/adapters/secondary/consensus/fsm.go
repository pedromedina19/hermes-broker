package consensus

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"

	"github.com/hashicorp/raft"
	"github.com/pedromedina19/hermes-broker/internal/core/domain"
	"github.com/pedromedina19/hermes-broker/internal/core/metrics"
	"github.com/pedromedina19/hermes-broker/internal/core/ports"
)

// BrokerFSM is the bridge between Raft and your Memory Engine
type BrokerFSM struct {
	engine ports.BrokerEngine
	logger *slog.Logger
}

func NewBrokerFSM(engine ports.BrokerEngine, logger *slog.Logger) *BrokerFSM {
	return &BrokerFSM{
		engine: engine,
		logger: logger,
	}
}

// Apply is called by Raft when a log is committed
func (fsm *BrokerFSM) Apply(l *raft.Log) interface{} {
	var messages []domain.Message
	if err := json.Unmarshal(l.Data, &messages); err != nil {
		var msg domain.Message
		if err := json.Unmarshal(l.Data, &msg); err != nil {
			fsm.logger.Error("FSM: Failed to unmarshal", "error", err)
			return err
		}
		messages = append(messages, msg)
	}

	for _, msg := range messages {
		if err := fsm.engine.Publish(context.TODO(), msg); err != nil {
			fsm.logger.Error("FSM: Failed to publish to engine", "id", msg.ID, "error", err)
		}
		metrics.IncPublished(msg.Topic, 1)
	}

	return nil
}

func (fsm *BrokerFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &NoOpSnapshot{}, nil
}

func (fsm *BrokerFSM) Restore(rc io.ReadCloser) error {
	// In a real-world scenario, we would read the binary from the database and replace the local file
	// This requires closing the current database, replacing the file, and reopening it
	// It's a complex "Hot Swap" operation
	// For the benchmark, just drain the Reader to avoid crashing Raft
	defer rc.Close()
	io.Copy(io.Discard, rc)
	return nil
}

type NoOpSnapshot struct{}

func (s *NoOpSnapshot) Persist(sink raft.SnapshotSink) error {
	return sink.Close()
}

func (s *NoOpSnapshot) Release() {}
