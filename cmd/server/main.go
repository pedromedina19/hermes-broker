package main

import (
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"time"

	"github.com/pedromedina19/hermes-broker/internal/pkg/monitoring"
	"github.com/pedromedina19/hermes-broker/internal/service"
	"github.com/pedromedina19/hermes-broker/pb"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to start listener TCP: %v", err)
	}

	grpcServer := grpc.NewServer()

	// (8 cores) -> Buffer 80.000
	// (2 cores) -> Buffer 20.000
	cpuCores := runtime.NumCPU()
	bufferSize := cpuCores * 10000

	log.Printf("Hermes Tuning: %d CPUs | Channel Buffer: %d msgs", cpuCores, bufferSize)

	brokerService := service.NewBrokerServer(bufferSize)
	pb.RegisterBrokerServiceServer(grpcServer, brokerService)

	// Enable Reflection so we can use tools like Postman
	reflection.Register(grpcServer)

	monitoring.StartMonitoring(5 * time.Second)

	go func() {
        http.Handle("/metrics", promhttp.Handler())
        http.ListenAndServe(":2112", nil) 
    }()

	log.Println("Hermes Broker (gRPC) started at port :50051")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve gRPC: %v", err)
	}
}
