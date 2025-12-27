package services

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pedromedina19/hermes-broker/internal/core/domain"
	"github.com/pedromedina19/hermes-broker/internal/core/metrics"
	"github.com/pedromedina19/hermes-broker/internal/core/ports"
	"github.com/pedromedina19/hermes-broker/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ConsensusNode interface {
	ApplyMessage(msg interface{}) error
	Join(nodeID, raftAddr string) error
	GetLeaderAddr() string
	IsLeader() bool
}
type publishResult struct {
	err error
}
type BrokerService struct {
	engine    ports.BrokerEngine
	consensus ConsensusNode

	batchChan chan *domain.Message
	pending   map[string]chan publishResult
	mu        sync.RWMutex

	// Message Pool to reduce pressure on the GC
	messagePool sync.Pool
}

func NewBrokerService(engine ports.BrokerEngine, consensus ConsensusNode) *BrokerService {
	s := &BrokerService{
		engine:    engine,
		consensus: consensus,
		batchChan: make(chan *domain.Message, 20000),
		pending:   make(map[string]chan publishResult),
		messagePool: sync.Pool{
			New: func() interface{} {
				return &domain.Message{
					Payload: make([]byte, 0, 1024), // Pre-allocates 1KB per message
				}
			},
		},
	}

	go s.runBatcher()

	return s
}

func (s *BrokerService) Publish(ctx context.Context, topic string, payload []byte) error {
	// If not a leader, immediate proxy (before allocating from the pool)
	if !s.consensus.IsLeader() {
		leaderID := s.consensus.GetLeaderAddr()

		if leaderID == "" {
			return fmt.Errorf("cluster in election: no leader found at the moment")
		}

		leaderHost := strings.Replace(leaderID, "node-", "hermes-", 1)
		leaderTarget := leaderHost + ":50051"

		err := s.proxyPublishToLeader(ctx, leaderTarget, topic, payload)
		if err != nil {
			atomic.AddUint64(&metrics.FailedCount, 1)
		}
		return err
	}

	// Get a "clean" message from the Pool
	msg := s.messagePool.Get().(*domain.Message)
	msg.Reset()
	msg.ID = fmt.Sprintf("%d", time.Now().UnixNano())
	msg.Topic = topic
	msg.Payload = append(msg.Payload, payload...)
	msg.Timestamp = time.Now()

	resChan := make(chan publishResult, 1)

	s.mu.Lock()
	s.pending[msg.ID] = resChan
	s.mu.Unlock()

	select {
	case s.batchChan <- msg:
	case <-ctx.Done():
		atomic.AddUint64(&metrics.FailedCount, 1)
		s.mu.Lock()
		delete(s.pending, msg.ID)
		s.mu.Unlock()
		s.messagePool.Put(msg)
		return ctx.Err()
	}

	select {
	case res := <-resChan:
		s.messagePool.Put(msg)
		return res.err
	case <-ctx.Done():
		s.mu.Lock()
		delete(s.pending, msg.ID)
		s.mu.Unlock()
		s.messagePool.Put(msg)
		return ctx.Err()
	}
}

func (s *BrokerService) runBatcher() {
	const maxBatchSize = 500
	const lingerTime = 5 * time.Millisecond

	for {
		batch := make([]*domain.Message, 0, maxBatchSize)

		msg, ok := <-s.batchChan
		if !ok {
			return
		}
		batch = append(batch, msg)

		timeout := time.After(lingerTime)
		full := false

		for !full && len(batch) < maxBatchSize {
			select {
			case m := <-s.batchChan:
				batch = append(batch, m)
			case <-timeout:
				full = true
			}
		}

		// Convert pointers to values ​​only at Consensus time
		// to prevent the pool from clearing the data before Raft finishes
		raftPayload := make([]domain.Message, len(batch))
		for i, m := range batch {
			raftPayload[i] = *m
		}

		err := s.consensus.ApplyMessage(raftPayload)

		s.mu.Lock()
		for _, m := range batch {
			if ch, ok := s.pending[m.ID]; ok {
				ch <- publishResult{err: err}
				delete(s.pending, m.ID)
			}
		}
		s.mu.Unlock()
	}
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

func (s *BrokerService) IsLeader() bool      { return s.consensus.IsLeader() }
func (s *BrokerService) GetLeaderID() string { return s.consensus.GetLeaderAddr() }
