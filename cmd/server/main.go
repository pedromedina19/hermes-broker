package main

import (
	"log"
	"net"

	"github.com/pedromedina19/hermes-broker/internal/service"
	"github.com/pedromedina19/hermes-broker/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to start listener TCP: %v", err)
	}

	grpcServer := grpc.NewServer()

	brokerService := service.NewBrokerServer()
	pb.RegisterBrokerServiceServer(grpcServer, brokerService)

	// Enable Reflection so we can use tools like Postman
	reflection.Register(grpcServer)

	log.Println("Hermes Broker (gRPC) started at port :50051")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve gRPC: %v", err)
	}
}
