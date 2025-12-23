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
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/pedromedina19/hermes-broker/internal/core/domain"
)

const (
	RaftTimeout = 10 * time.Second
)

type RaftNode struct {
	Raft   *raft.Raft
	logger *slog.Logger
	config RaftConfig
}

type RaftConfig struct {
	NodeID    string // Ex: "node-1"
	RaftPort  string // Ex: ":6000" (Exclusive port for communication between us)
	DataDir   string // Where to save Raft log
	Bootstrap bool   // If true, it starts as the leader (only for the first node of the cluster)
}

func NewRaftNode(conf RaftConfig, fsm *BrokerFSM, logger *slog.Logger) (*RaftNode, error) {
	config := raft.DefaultConfig()
	raftLogger := hclog.New(&hclog.LoggerOptions{
		Name:   "raft",
		Level:  hclog.Info,
		Output: os.Stderr,
	})
	config.Logger = raftLogger
	config.LocalID = raft.ServerID(conf.NodeID)
	config.HeartbeatTimeout = 1000 * time.Millisecond
	config.ElectionTimeout = 1000 * time.Millisecond
	config.CommitTimeout = 500 * time.Millisecond

	addr, err := net.ResolveTCPAddr("tcp", conf.RaftPort)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(conf.RaftPort, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	os.MkdirAll(conf.DataDir, 0700)

	boltFile := filepath.Join(conf.DataDir, "raft.db")
	logStore, err := raftboltdb.NewBoltStore(boltFile)
	if err != nil {
		return nil, fmt.Errorf("new bolt store: %w", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(conf.DataDir, 1, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("file snapshot store: %w", err)
	}

	r, err := raft.NewRaft(config, fsm, logStore, logStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("new raft: %w", err)
	}

	if conf.Bootstrap {
		logger.Info("Bootstrapping new cluster", "node", conf.NodeID)
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		r.BootstrapCluster(configuration)
	}

	return &RaftNode{
		Raft:   r,
		logger: logger,
		config: conf,
	}, nil
}

func (rn *RaftNode) ApplyMessage(msg domain.Message) error {
	if rn.Raft.State() != raft.Leader {
		return fmt.Errorf("not the leader")
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	future := rn.Raft.Apply(data, RaftTimeout)
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
