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
	engine           ports.BrokerEngine
	logger           *slog.Logger
	nodeID           string
	lastAppliedIndex uint64
}

func NewBrokerFSM(engine ports.BrokerEngine, nodeID string, logger *slog.Logger) *BrokerFSM {
	lastIndex, err := engine.GetLastAppliedIndex()
	if err != nil {
		logger.Error("Failed to load last applied index", "err", err)
		lastIndex = 0
	}

	logger.Info("FSM Initialized", "last_applied_index", lastIndex)
	return &BrokerFSM{
		engine:           engine,
		nodeID:           nodeID,
		logger:           logger,
		lastAppliedIndex: lastIndex,
	}
}

// Apply is called by Raft when a log is committed
func (fsm *BrokerFSM) Apply(l *raft.Log) interface{} {
	if l.Index <= fsm.lastAppliedIndex {
		// fsm.logger.Debug("Skipping replay", "index", l.Index, "last", fsm.lastAppliedIndex)
		return nil
	}

	var cmd domain.RaftCommand

	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		var messages []domain.Message
		if errLegacy := json.Unmarshal(l.Data, &messages); errLegacy == nil {
			fsm.applyPublish(messages, l.Index)
			return nil
		}
		fsm.logger.Error("FSM: Failed to unmarshal", "error", err)
		return err
	}

	switch cmd.Type {
	case domain.LogTypePublish:
		fsm.applyPublish(cmd.Messages, l.Index)

	case domain.LogTypeOffset:
		if cmd.OffsetCommit != nil {
			fsm.applyOffset(*cmd.OffsetCommit, l.Index)
		}

	case domain.LogTypeReplica:
		if cmd.OriginNodeID == fsm.nodeID {
			// Leader has already persisted locally in fastBatcher (raftIndex=0)
			// Here we only advance the last_raft_index in the store for debug/recovery
			if err := fsm.engine.PublishBatch(context.TODO(), nil, l.Index); err != nil {
				fsm.logger.Warn("FSM: failed to persist last_raft_index for self replica", "err", err, "index", l.Index)
			}
			break
		}
		fsm.applyPublish(cmd.Messages, l.Index)


	default:
		if cmd.Type == "" && len(cmd.Messages) > 0 {
			fsm.applyPublish(cmd.Messages, l.Index)
		} else {
			fsm.logger.Warn("Unknown Raft Command Type", "type", cmd.Type)
		}
	}

	fsm.lastAppliedIndex = l.Index

	return nil
}

func (fsm *BrokerFSM) applyPublish(messages []domain.Message, raftIndex uint64) {
	batchPtrs := make([]*domain.Message, len(messages))
	for i := range messages {
		batchPtrs[i] = &messages[i]
	}

	if err := fsm.engine.PublishBatch(context.TODO(), batchPtrs, raftIndex); err != nil {
		fsm.logger.Error("FSM: Failed to publish batch", "error", err)
	}

	for _, msg := range messages {
		metrics.IncPublished(msg.Topic, 1)
	}
}

func (fsm *BrokerFSM) applyOffset(off domain.Offset, raftIndex uint64) {
	if err := fsm.engine.SyncOffsetWithIndex(off.Topic, off.GroupID, off.Value, raftIndex); err != nil {
		fsm.logger.Error("FSM: Failed to sync offset", "error", err)
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
