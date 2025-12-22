package main

import (
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	grpc_adapter "github.com/pedromedina19/hermes-broker/internal/adapters/primary/grpc"
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
	
	grpcHandler := grpc_adapter.NewGrpcHandler(brokerService, logger)

	listener, err := net.Listen("tcp", cfg.GRPCPort)
	if err != nil {
		logger.Error("Failed to listen", "error", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterBrokerServiceServer(grpcServer, grpcHandler)
	reflection.Register(grpcServer)

	//Graceful Shutdown
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		logger.Info("gRPC Server listening", "address", cfg.GRPCPort)
		if err := grpcServer.Serve(listener); err != nil {
			logger.Error("gRPC Server error", "error", err)
		}
	}()

	<-stopChan
	logger.Info("Shutting down server...")

	grpcServer.GracefulStop()
	brokerEngine.Close()
	wal.Close()

	logger.Info("Server exited properly")
}