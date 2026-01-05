package consensus

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftpebble "github.com/syncplify/raft-pebble"
)

const (
	RaftTimeout = 10 * time.Second
)

type RaftNode struct {
	Raft     *raft.Raft
	logger   *slog.Logger
	config   RaftConfig
	hasState bool
	store *raftpebble.PebbleKVStore
}

type RaftConfig struct {
	NodeID    string // Ex: "node-1"
	RaftPort  string // Ex: ":6000" (Exclusive port for communication between us)
	DataDir   string // Where to save Raft log
	Bootstrap bool   // If true, it starts as the leader (only for the first node of the cluster)
}

func NewRaftNode(conf RaftConfig, fsm *BrokerFSM, logger *slog.Logger) (*RaftNode, error) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(conf.NodeID)

	config.BatchApplyCh = true

	raftLogger := hclog.New(&hclog.LoggerOptions{
		Name:   "raft",
		Level:  hclog.Warn,
		Output: os.Stderr,
	})
	config.Logger = raftLogger
	config.LocalID = raft.ServerID(conf.NodeID)
	config.HeartbeatTimeout = 2000 * time.Millisecond
	config.ElectionTimeout = 2000 * time.Millisecond
	config.CommitTimeout = 100 * time.Millisecond
	config.SnapshotThreshold = 32768
	config.SnapshotInterval = 60 * time.Second
	config.TrailingLogs = 8192

	var addr *net.TCPAddr
	var err error
	maxRetries := 30

	for i := 0; i < maxRetries; i++ {
		addr, err = net.ResolveTCPAddr("tcp", conf.RaftPort)
		if err == nil {
			logger.Info("DNS Resolved successfully", "addr", addr.String())
			break
		}
		logger.Warn("Waiting for DNS resolution...", "target", conf.RaftPort, "error", err, "attempt", i+1)
		time.Sleep(2 * time.Second)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to resolve raft address after retries: %w", err)
	}

	transport, err := raft.NewTCPTransport(conf.RaftPort, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	os.MkdirAll(conf.DataDir, 0700)

	dbDir := filepath.Join(conf.DataDir, "raft.pebble")
	walDir := filepath.Join(conf.DataDir, "raft.pebble.wal")

	kv, err := raftpebble.New(
		raftpebble.WithDbDirPath(dbDir),
		raftpebble.WithWalDirPath(walDir),
		raftpebble.WithConfig(raftpebble.GetTinyMemRaftLogRocksDBConfig()),
	)
	if err != nil {
		return nil, fmt.Errorf("new pebble raft store: %w", err)
	}

	// Snapshot Store
	snapshotStore, err := raft.NewFileSnapshotStore(conf.DataDir, 1, os.Stderr)
	if err != nil {
		_ = kv.Close()
		return nil, fmt.Errorf("file snapshot store: %w", err)
	}

	hasState, err := raft.HasExistingState(kv, kv, snapshotStore)
	if err != nil {
		_ = kv.Close()
		return nil, err
	}

	r, err := raft.NewRaft(config, fsm, kv, kv, snapshotStore, transport)
	if err != nil {
		_ = kv.Close()
		return nil, fmt.Errorf("new raft: %w", err)
	}

	if conf.Bootstrap && !hasState {
		logger.Info("Bootstrapping new cluster with DNS", "node", conf.NodeID, "addr", conf.RaftPort)
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: raft.ServerAddress(conf.RaftPort),
				},
			},
		}
		r.BootstrapCluster(configuration)
	}

	return &RaftNode{
		Raft:     r,
		logger:   logger,
		config:   conf,
		hasState: hasState,
	}, nil
}

func (rn *RaftNode) ApplyMessage(data interface{}) error {
	if rn.Raft.State() != raft.Leader {
		return fmt.Errorf("not the leader")
	}

	buf, err := json.Marshal(data)
	if err != nil {
		return err
	}

	future := rn.Raft.Apply(buf, RaftTimeout)
	if err := future.Error(); err != nil {
		return err
	}

	response := future.Response()
	if err, ok := response.(error); ok {
		return err
	}
	return nil
}

func (rn *RaftNode) Join(nodeID, raftAddr string) error {
	if rn.Raft.State() != raft.Leader {
		return fmt.Errorf("not the leader")
	}
	future := rn.Raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(raftAddr), 0, 0)
	if err := future.Error(); err != nil {
		return err
	}
	return nil
}

func (rn *RaftNode) GetLeaderAddr() string {
	_, id := rn.Raft.LeaderWithID()
	return string(id)
}

func (rn *RaftNode) IsLeader() bool {
	return rn.Raft.State() == raft.Leader
}

func (rn *RaftNode) HasState() bool {
	return rn.hasState
}

func (rn *RaftNode) Close() {
	if rn == nil {
		return
	}
	if rn.Raft != nil {
		_ = rn.Raft.Shutdown().Error()
	}
}