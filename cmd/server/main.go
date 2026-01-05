package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/pedromedina19/hermes-broker/graph"
	grpc_adapter "github.com/pedromedina19/hermes-broker/internal/adapters/primary/grpc"
	http_adapter "github.com/pedromedina19/hermes-broker/internal/adapters/primary/http"
	"github.com/pedromedina19/hermes-broker/internal/adapters/secondary/config"
	"github.com/pedromedina19/hermes-broker/internal/adapters/secondary/consensus"
	"github.com/pedromedina19/hermes-broker/internal/adapters/secondary/memory"
	"github.com/pedromedina19/hermes-broker/internal/core/services"
	"github.com/pedromedina19/hermes-broker/pb"
)

func main() {
	cfg := config.Load()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	logger.Info("Starting Hermes Broker", "node_id", cfg.NodeID, "raft_port", cfg.RaftPort)

	raftConf := consensus.RaftConfig{
		NodeID:    cfg.NodeID,
		RaftPort:  cfg.RaftPort,
		DataDir:   cfg.RaftDataDir,
		Bootstrap: cfg.Bootstrap,
	}

	brokerDataDir := filepath.Join(cfg.RaftDataDir, "broker_data")
	os.MkdirAll(brokerDataDir, 0755)

	brokerEngine := memory.NewHybridBroker(brokerDataDir, cfg.BufferSize, logger)
	fsm := consensus.NewBrokerFSM(brokerEngine, cfg.NodeID, logger)

	raftNode, err := consensus.NewRaftNode(raftConf, fsm, logger)
	if err != nil {
		logger.Error("Failed to initialize Raft", "error", err)
		os.Exit(1)
	}

	brokerService := services.NewBrokerService(brokerEngine, raftNode, cfg.NodeID)

	if cfg.JoinAddr != "" && !cfg.Bootstrap && !raftNode.HasState() {
		go autoJoin(cfg, logger)
	} else {
		logger.Info("Skipping Automatic Join",
			"is_bootstrap", cfg.Bootstrap,
			"has_state", raftNode.HasState())
	}

	grpcListener, err := net.Listen("tcp", cfg.GRPCPort)
	if err != nil {
		logger.Error("Failed to listen gRPC", "error", err)
		os.Exit(1)
	}
	grpcServer := grpc.NewServer()
	grpcHandler := grpc_adapter.NewGrpcHandler(brokerService, logger)
	pb.RegisterBrokerServiceServer(grpcServer, grpcHandler)
	reflection.Register(grpcServer)

	go func() {
		logger.Info("gRPC Server listening", "address", cfg.GRPCPort)
		if err := grpcServer.Serve(grpcListener); err != nil {
			logger.Error("gRPC Server error", "error", err)
		}
	}()

	// server HTTP REST + GraphQL (8080)

	gqlServer := handler.NewDefaultServer(graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{Service: brokerService}}))

	// subscription support
	gqlServer.AddTransport(&transport.Websocket{
		KeepAlivePingInterval: 10 * time.Second,
		Upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	})

	httpHandler := http_adapter.NewHttpHandler(brokerService, cfg)

	// Mux (default go router)
	mux := http.NewServeMux()
	mux.HandleFunc("/publish", httpHandler.HandlePublish) // REST Endpoint
	mux.HandleFunc("/status", httpHandler.HandleStatus)
	mux.HandleFunc("/join", httpHandler.HandleJoin)
	mux.HandleFunc("/publish/batch", httpHandler.HandlePublishBatch)
	mux.HandleFunc("/subscribe", httpHandler.HandleSubscribeSSE)
	mux.HandleFunc("/query", httpHandler.HandleGraphQL(gqlServer)) // GraphQL Endpoint
	mux.Handle("/", playground.Handler("GraphQL playground", "/query"))

	httpServer := &http.Server{
		Addr:    cfg.HTTPPort,
		Handler: mux,
	}

	go func() {
		logger.Info("HTTP Gateway listening (REST/GraphQL)", "address", cfg.HTTPPort)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP Server error", "error", err)
		}
	}()

	// Graceful Shutdown Global
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	<-stopChan
	logger.Info("Shutting down servers...")

	grpcServer.GracefulStop()
	_ = httpServer.Close()
	brokerEngine.Close()
	if raftNode != nil {
		raftNode.Close()
	}

	logger.Info("Hermes Shutdown complete")
}

func autoJoin(cfg config.Config, logger *slog.Logger) {
	// Wait a moment to ensure that both the server and the leader are up
	time.Sleep(2 * time.Second)

	target := "http://" + cfg.JoinAddr + "/join"

	reqBody := map[string]string{
		"node_id":   cfg.NodeID,
		"raft_addr": cfg.RaftPort,
	}

	body, _ := json.Marshal(reqBody)

	// Try for 1 minute (Retry with Backoff)
	for i := 0; i < 10; i++ {
		logger.Info("Attempting to join cluster...", "target", target)

		resp, err := http.Post(target, "application/json", bytes.NewBuffer(body))
		if err == nil && resp.StatusCode == 200 {
			logger.Info("Successfully joined the cluster!")
			return
		}

		if err != nil {
			logger.Warn("Failed to join cluster (retrying...)", "error", err)
		} else {
			logger.Warn("Failed to join cluster (retrying...)", "status", resp.StatusCode)
		}

		time.Sleep(5 * time.Second)
	}

	logger.Error("Could not join cluster after multiple attempts. Running in isolation.")
}
