package main

import (
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
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
	"github.com/pedromedina19/hermes-broker/internal/adapters/secondary/memory"
	"github.com/pedromedina19/hermes-broker/internal/adapters/secondary/persistence"
	"github.com/pedromedina19/hermes-broker/internal/core/services"
	"github.com/pedromedina19/hermes-broker/pb"
)

func main() {
	cfg := config.Load()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	
	logger.Info("Starting Hermes Broker", "config", cfg)

	brokerEngine := memory.NewMemoryBroker(cfg.BufferSize, logger)

	wal, err := persistence.NewFileWAL("hermes.wal", logger)
	if err != nil {
		logger.Error("Failed to initialize WAL", "error", err)
		os.Exit(1)
	}
	
	brokerService := services.NewBrokerService(brokerEngine, wal)

	logger.Info("Recovering state from WAL...")
	if err := brokerService.RecoverState(); err != nil {
		logger.Error("Failed to recover state", "error", err)
		// Depending on the criticality, we could either end up with an error or continue empty
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

	restHandler := http_adapter.NewRestHandler(brokerService)

	// Mux (default go router)
	mux := http.NewServeMux()
	mux.HandleFunc("/publish", restHandler.HandlePublish)           // REST Endpoint
	mux.Handle("/query", gqlServer)                                 // GraphQL Endpoint
	mux.Handle("/", playground.Handler("GraphQL playground", "/query"))

	httpServer := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		logger.Info("HTTP Gateway listening (REST/GraphQL)", "address", ":8080")
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
	httpServer.Close()
	brokerEngine.Close()
	wal.Close()

	logger.Info("Hermes Shutdown complete")
}