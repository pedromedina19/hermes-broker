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
)

const (
	RaftTimeout = 10 * time.Second
)

type RaftNode struct {
	Raft   *raft.Raft
	logger *slog.Logger
	config RaftConfig
	hasState bool
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

	raftLogger := hclog.New(&hclog.LoggerOptions{
		Name:   "raft",
		Level:  hclog.Info,
		Output: os.Stderr,
	})
	config.Logger = raftLogger
	config.LocalID = raft.ServerID(conf.NodeID)
	config.HeartbeatTimeout = 500 * time.Millisecond
	config.ElectionTimeout = 500 * time.Millisecond
	config.CommitTimeout = 50 * time.Millisecond
	config.SnapshotThreshold = 1024
	config.SnapshotInterval = 120 * time.Second
	config.TrailingLogs = 1024

	var advertiseAddr *net.TCPAddr
    var err error
    for i := 0; i < 10; i++ {
        advertiseAddr, err = net.ResolveTCPAddr("tcp", conf.RaftPort)
        if err == nil {
            break
        }
        logger.Warn("Waiting for DNS to become available...", "host", conf.RaftPort, "attempt", i+1)
        time.Sleep(2 * time.Second)
    }
    
    if err != nil {
        return nil, fmt.Errorf("failed to resolve advertise address after retries: %w", err)
    }

    transport, err := raft.NewTCPTransport(conf.RaftPort, advertiseAddr, 3, 10*time.Second, os.Stderr)
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

	hasState, err := raft.HasExistingState(logStore, logStore, snapshotStore)
    if err != nil {
        return nil, err
    }

	r, err := raft.NewRaft(config, fsm, logStore, logStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("new raft: %w", err)
	}

	if conf.Bootstrap && !hasState {
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