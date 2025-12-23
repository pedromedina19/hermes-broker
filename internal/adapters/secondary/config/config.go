package config

import "os"
import "strconv"

type Config struct {
	GRPCPort   string
	HTTPPort   string
	BufferSize int

	// Raft Configs
	NodeID      string // Unique Identity (e.g., hermes-0)
	RaftPort    string // Internal door (e.g., :6000)
	RaftDataDir string // Persistent volume
	Bootstrap   bool   // If it is the initial cluster

	JoinAddr string
}


func Load() Config {
	bufferSize, _ := strconv.Atoi(os.Getenv("HERMES_BUFFER_SIZE"))
	if bufferSize == 0 {
		bufferSize = 20000
	}

	grpcPort := os.Getenv("HERMES_GRPC_PORT")
	if grpcPort == "" {
		grpcPort = ":50051"
	}

	httpPort := os.Getenv("HERMES_HTTP_PORT")
	if httpPort == "" {
		httpPort = ":8080"
	}

	// Raft Defaults
	nodeID := os.Getenv("HERMES_NODE_ID")
	if nodeID == "" {
		nodeID = "node-1"
	}

	raftPort := os.Getenv("HERMES_RAFT_PORT")
	if raftPort == "" {
		raftPort = ":6000"
	}
	
	dataDir := os.Getenv("HERMES_DATA_DIR")
	if dataDir == "" {
		dataDir = "./data/" + nodeID
	}

	bootstrap := os.Getenv("HERMES_BOOTSTRAP") == "true"

	joinAddr := os.Getenv("HERMES_JOIN_ADDR")

	return Config{
		GRPCPort:    grpcPort,
		HTTPPort:    httpPort,
		BufferSize:  bufferSize,
		NodeID:      nodeID,
		RaftPort:    raftPort,
		RaftDataDir: dataDir,
		Bootstrap:   bootstrap,
		JoinAddr:    joinAddr,
	}
}