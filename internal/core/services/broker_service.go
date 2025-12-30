package services

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
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

	batchChan     chan *domain.Message
	fastBatchChan chan *domain.Message
	pending       map[string]chan publishResult
	mu            sync.RWMutex

	messagePool sync.Pool

	proxyMu      sync.RWMutex
	proxyStreams map[string]pb.BrokerService_SubscribeClient
}

func NewBrokerService(engine ports.BrokerEngine, consensus ConsensusNode) *BrokerService {
	s := &BrokerService{
		engine:        engine,
		consensus:     consensus,
		batchChan:     make(chan *domain.Message, 20000),
		fastBatchChan: make(chan *domain.Message, 20000),
		pending:       make(map[string]chan publishResult),
		proxyStreams:  make(map[string]pb.BrokerService_SubscribeClient),
		messagePool: sync.Pool{
			New: func() interface{} {
				return &domain.Message{
					Payload: make([]byte, 0, 1024),
				}
			},
		},
	}

	go s.runBatcher()
	go s.runFastBatcher()
	return s
}

func (s *BrokerService) Publish(ctx context.Context, topic string, payload []byte, mode pb.DeliveryMode) error {
	if !s.consensus.IsLeader() {
		leaderTarget, err := s.getLeaderTarget()
		if err != nil {
			return err
		}

		err = s.proxyPublishToLeader(ctx, leaderTarget, topic, payload, mode)
		if err != nil {
			metrics.IncFailed()
			slog.Error("Proxy Publish Failed", "target", leaderTarget, "error", err)
		}
		return err
	}

	msg := s.messagePool.Get().(*domain.Message)
	msg.Reset()
	msg.ID = fmt.Sprintf("%d", time.Now().UnixNano())
	msg.Topic = topic
	msg.Payload = append(msg.Payload, payload...)
	msg.Timestamp = time.Now()

	if mode == pb.DeliveryMode_PERFORMANCE {
		select {
		case s.fastBatchChan <- msg:
			return nil
		default:
			metrics.IncFailed()
			s.messagePool.Put(msg)
			return fmt.Errorf("performance buffer full")
		}
	}

	resChan := make(chan publishResult, 1)

	s.mu.Lock()
	s.pending[msg.ID] = resChan
	s.mu.Unlock()

	if len(s.batchChan) > 15000 {
		slog.Warn("Hermes Batch Channel High Load", "size", len(s.batchChan))
	}

	select {
	case s.batchChan <- msg:
	case <-ctx.Done():
		metrics.IncFailed()
		s.cleanupPending(msg.ID)
		s.messagePool.Put(msg)
		return ctx.Err()
	}

	select {
	case res := <-resChan:
		s.messagePool.Put(msg)
		return res.err
	case <-ctx.Done():
		s.cleanupPending(msg.ID)
		s.messagePool.Put(msg)
		return ctx.Err()
	}
}

func (s *BrokerService) Subscribe(ctx context.Context, topic string, groupID string) (<-chan domain.Message, string, error) {
	if s.consensus.IsLeader() {
		return s.engine.Subscribe(ctx, topic, groupID)
	}
	return s.proxySubscribeToLeader(ctx, topic, groupID)
}

func (s *BrokerService) Acknowledge(subID, msgID string) {
	s.proxyMu.RLock()
	upstreamStream, isProxy := s.proxyStreams[subID]
	s.proxyMu.RUnlock()

	if isProxy {
		err := upstreamStream.Send(&pb.SubscribeRequest{
			Action:       "ACK",
			AckMessageId: msgID,
		})
		if err != nil {
			slog.Error("Failed to forward ACK to leader", "subID", subID, "msgID", msgID, "error", err)
		}
		return
	}

	s.engine.Acknowledge(subID, msgID)
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

		raftPayload := make([]domain.Message, len(batch))
		for i, m := range batch {
			raftPayload[i] = *m
		}

		start := time.Now()
		err := s.consensus.ApplyMessage(raftPayload)
		duration := time.Since(start)
		if duration > 500*time.Millisecond {
			slog.Warn("Slow Raft Apply", "duration", duration, "batch_size", len(batch))
		}

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

func (s *BrokerService) runFastBatcher() {
	const maxBatchSize = 2000
	const lingerTime = 10 * time.Millisecond

	batch := make([]*domain.Message, 0, maxBatchSize)

	for {
		msg := <-s.fastBatchChan
		batch = append(batch, msg)

		timeout := time.After(lingerTime)
		full := false
		for !full && len(batch) < maxBatchSize {
			select {
			case m := <-s.fastBatchChan:
				batch = append(batch, m)
			case <-timeout:
				full = true
			}
		}

		if len(batch) > 0 {
			if err := s.engine.PublishBatch(context.Background(), batch); err != nil {
				slog.Error("FastBatcher failed", "err", err)
			} else {
				for _, m := range batch {
                     metrics.IncPublished(m.Topic, 1)
                 }
			}

			for _, m := range batch {
				s.messagePool.Put(m)
			}
			batch = batch[:0]
		}
	}
}

func (s *BrokerService) getLeaderTarget() (string, error) {
    leaderID := s.consensus.GetLeaderAddr()
    if leaderID == "" {
        return "", fmt.Errorf("no leader")
    }
    host := strings.Replace(leaderID, "node-", "hermes-", 1)
    if !strings.Contains(host, "hermes-internal") {
        host = fmt.Sprintf("%s.hermes-internal", host)
    }
    return fmt.Sprintf("%s:50051", host), nil
}

func (s *BrokerService) proxyPublishToLeader(ctx context.Context, addr, topic string, payload []byte, mode pb.DeliveryMode) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("proxy connect failed to %s: %w", addr, err)
	}
	defer conn.Close()

	client := pb.NewBrokerServiceClient(conn)
	_, err = client.Publish(ctx, &pb.PublishRequest{
		Topic:   topic,
		Payload: payload,
		Mode:    mode,
	})
	return err
}

func (s *BrokerService) proxySubscribeToLeader(ctx context.Context, topic, groupID string) (<-chan domain.Message, string, error) {
	target, err := s.getLeaderTarget()
	if err != nil {
		return nil, "", err
	}

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, "", fmt.Errorf("dial leader failed: %w", err)
	}

	client := pb.NewBrokerServiceClient(conn)
	stream, err := client.Subscribe(ctx)
	if err != nil {
		conn.Close()
		return nil, "", fmt.Errorf("open stream failed: %w", err)
	}

	err = stream.Send(&pb.SubscribeRequest{
		Action:  "SUBSCRIBE",
		Topic:   topic,
		GroupId: groupID,
	})
	if err != nil {
		conn.Close()
		return nil, "", fmt.Errorf("handshake failed: %w", err)
	}

	proxySubID := fmt.Sprintf("proxy-%d", time.Now().UnixNano())
	msgChan := make(chan domain.Message, 5000)

	s.proxyMu.Lock()
	s.proxyStreams[proxySubID] = stream
	s.proxyMu.Unlock()

	go func() {
		defer func() {
			conn.Close()
			close(msgChan)
			s.RemoveSubscriber(topic, proxySubID)
		}()

		for {
			in, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				if ctx.Err() == nil {
					slog.Error("Proxy stream error", "err", err)
				}
				return
			}

			select {
			case msgChan <- domain.Message{
				ID:        in.Id,
				Topic:     in.Topic,
				Payload:   in.Payload,
				Timestamp: time.Unix(0, in.Timestamp),
			}:
				metrics.IncConsumed(in.Topic, 1)
			case <-ctx.Done():
				return
			}
		}
	}()

	return msgChan, proxySubID, nil
}

func (s *BrokerService) RemoveSubscriber(topic, subID string) {
	s.proxyMu.Lock()
	if stream, ok := s.proxyStreams[subID]; ok {
		stream.CloseSend()
		delete(s.proxyStreams, subID)
		s.proxyMu.Unlock()
		return
	}
	s.proxyMu.Unlock()

	s.engine.Unsubscribe(topic, subID)
}

func (s *BrokerService) cleanupPending(msgID string) {
	s.mu.Lock()
	delete(s.pending, msgID)
	s.mu.Unlock()
}

func (s *BrokerService) JoinCluster(nodeID, addr string) error {
	return s.consensus.Join(nodeID, addr)
}

func (s *BrokerService) IsLeader() bool      { return s.consensus.IsLeader() }
func (s *BrokerService) GetLeaderID() string { return s.consensus.GetLeaderAddr() }
