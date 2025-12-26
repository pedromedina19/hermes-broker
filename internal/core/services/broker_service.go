package services

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pedromedina19/hermes-broker/internal/core/domain"
	"github.com/pedromedina19/hermes-broker/internal/core/ports"
	"github.com/pedromedina19/hermes-broker/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ConsensusNode interface {
	ApplyMessage(msg domain.Message) error
	Join(nodeID, raftAddr string) error
	GetLeaderAddr() string
	IsLeader() bool
}
type BrokerService struct {
	engine ports.BrokerEngine
	consensus ConsensusNode
}

func NewBrokerService(engine ports.BrokerEngine, consensus ConsensusNode) *BrokerService {
	return &BrokerService{
		engine:    engine,
		consensus: consensus,
	}
}

func (s *BrokerService) Publish(ctx context.Context, topic string, payload []byte) error {
	msg := domain.Message{
		Topic:     topic,
		Payload:   payload,
		Timestamp: time.Now(),
	}

	err := s.consensus.ApplyMessage(msg)
	if err != nil && strings.Contains(err.Error(), "not the leader") {
		// Server Proxy Logic
		leaderID := s.consensus.GetLeaderAddr()
		if leaderID == "" {
			return fmt.Errorf("cluster in election, no leader found")
		}

		// Simple mapping: node-1 -> hermes-1:50051
		// In a real-world system, this would come from a Service Discovery process
		leaderHost := strings.Replace(leaderID, "node-", "hermes-", 1)
		leaderGrpcAddr := leaderHost + ":50051"

		return s.proxyPublishToLeader(ctx, leaderGrpcAddr, topic, payload)
	}

	return err
}

// proxyPublishToLeader forwards the request to the correct node
func (s *BrokerService) proxyPublishToLeader(ctx context.Context, addr, topic string, payload []byte) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("proxy failure: could not connect to leader %s: %w", addr, err)
	}
	defer conn.Close()

	client := pb.NewBrokerServiceClient(conn)
	_, err = client.Publish(ctx, &pb.PublishRequest{
		Topic:   topic,
		Payload: payload,
	})
	return err
}

func (s *BrokerService) Acknowledge(subID, msgID string) {
	s.engine.Acknowledge(subID, msgID)
}

func (s *BrokerService) Subscribe(ctx context.Context, topic string, groupID string) (<-chan domain.Message, string, error) {
	return s.engine.Subscribe(ctx, topic, groupID)
}

func (s *BrokerService) RemoveSubscriber(topic, subID string) {
	s.engine.Unsubscribe(topic, subID)
}

func (s *BrokerService) JoinCluster(nodeID, addr string) error {
	return s.consensus.Join(nodeID, addr)
}