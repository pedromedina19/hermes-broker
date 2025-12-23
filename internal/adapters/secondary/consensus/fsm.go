package consensus

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"

	"github.com/hashicorp/raft"
	"github.com/pedromedina19/hermes-broker/internal/core/domain"
	"github.com/pedromedina19/hermes-broker/internal/core/ports"
)

//BrokerFSM is the bridge between Raft and your Memory Engine
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
	var msg domain.Message
	if err := json.Unmarshal(l.Data, &msg); err != nil {
		fsm.logger.Error("FSM: Failed to unmarshal command", "error", err)
		return nil
	}

	fsm.logger.Info("⚡ FSM APPLIED (Raft Replicated)", "node_apply", "TRUE", "topic", msg.Topic, "msg_id", msg.ID)

	// The context is background because this runs internally in the Raft loop
	if err := fsm.engine.Publish(context.TODO(), msg); err != nil {
		fsm.logger.Error("FSM: Failed to publish to engine", "error", err)
		return err
	}

	return nil
}

// Snapshots are used to compress the log.

func (fsm *BrokerFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &NoOpSnapshot{}, nil
}

// Restore recovers the state of a snapshot.
func (fsm *BrokerFSM) Restore(rc io.ReadCloser) error {
	return nil
}


type NoOpSnapshot struct{}

func (s *NoOpSnapshot) Persist(sink raft.SnapshotSink) error {
	return sink.Close()
}

func (s *NoOpSnapshot) Release() {}