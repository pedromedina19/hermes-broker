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
	nodeID string
}

func NewBrokerFSM(engine ports.BrokerEngine, nodeID string, logger *slog.Logger) *BrokerFSM {
	return &BrokerFSM{
		engine: engine,
		nodeID: nodeID,
		logger: logger,
	}
}

// Apply is called by Raft when a log is committed
func (fsm *BrokerFSM) Apply(l *raft.Log) interface{} {
	var cmd domain.RaftCommand

	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		var messages []domain.Message
		if errLegacy := json.Unmarshal(l.Data, &messages); errLegacy == nil {
			fsm.applyPublish(messages)
			return nil
		}
		fsm.logger.Error("FSM: Failed to unmarshal", "error", err)
		return err
	}

	switch cmd.Type {
	case domain.LogTypePublish:
		fsm.applyPublish(cmd.Messages)
	case domain.LogTypeOffset:
		if cmd.OffsetCommit != nil {
			fsm.applyOffset(*cmd.OffsetCommit)
		}
	case domain.LogTypeReplica:
		if cmd.OriginNodeID == fsm.nodeID {
			fsm.logger.Error("IGNORING SELF REPLICA - WORKING CORRECTLY")
			return nil
		}
		fsm.applyPublish(cmd.Messages)

	default:
		if cmd.Type == "" && len(cmd.Messages) > 0 {
			fsm.applyPublish(cmd.Messages)
		} else {
			fsm.logger.Warn("Unknown Raft Command Type", "type", cmd.Type)
		}
	}

	return nil
}

func (fsm *BrokerFSM) applyPublish(messages []domain.Message) {
	batchPtrs := make([]*domain.Message, len(messages))
	for i := range messages {
		batchPtrs[i] = &messages[i]
	}

	if err := fsm.engine.PublishBatch(context.TODO(), batchPtrs); err != nil {
		fsm.logger.Error("FSM: Failed to publish batch to engine", "count", len(messages), "error", err)
	}

	for _, msg := range messages {
		metrics.IncPublished(msg.Topic, 1)
	}
}

func (fsm *BrokerFSM) applyOffset(off domain.Offset) {
	if err := fsm.engine.SyncOffset(off.Topic, off.GroupID, off.Value); err != nil {
		fsm.logger.Error("FSM: Failed to sync offset", "group", off.GroupID, "error", err)
	}
}

func (fsm *BrokerFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &NoOpSnapshot{}, nil
}

func (fsm *BrokerFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	io.Copy(io.Discard, rc)
	return nil
}

type NoOpSnapshot struct{}

func (s *NoOpSnapshot) Persist(sink raft.SnapshotSink) error {
	return sink.Close()
}

func (s *NoOpSnapshot) Release() {}
