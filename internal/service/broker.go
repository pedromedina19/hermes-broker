package service

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/pedromedina19/hermes-broker/pb"
)

type BrokerServer struct {
	pb.UnimplementedBrokerServiceServer
	// Topic Map -> List of Subscriber Channels
	subscribers map[string][]chan *pb.Message
	mu          sync.RWMutex // protect subscriber map
	bufferSize  int
}

func NewBrokerServer(bufferSize int) *BrokerServer {
	return &BrokerServer{
		subscribers: make(map[string][]chan *pb.Message),
		bufferSize:  bufferSize,
	}
}

// receives a message and distributes it to all subscribers of the topic.
func (s *BrokerServer) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	// Read Blocking (allows multiple publishers, but no new subscribers during the loop)
	s.mu.RLock()
	subs := make([]chan *pb.Message, len(s.subscribers[req.Topic]))
	copy(subs, s.subscribers[req.Topic])
	s.mu.RUnlock()

	if len(subs) == 0 {
		return &pb.PublishResponse{Success: true}, nil
	}

	msg := &pb.Message{
		Topic:     req.Topic,
		Payload:   req.Payload,
		Timestamp: time.Now().UnixNano(),
	}

	// send to all channels
	for _, subChan := range subs {
		// system slows down the reading to keep up with database
		subChan <- msg
	}

	return &pb.PublishResponse{Success: true}, nil
}

// stream of messages
func (s *BrokerServer) Subscribe(req *pb.SubscribeRequest, stream pb.BrokerService_SubscribeServer) error {
	// Buffer = 50000 to handle peak loads
	// This consumes ~15 MB of RAM.
	clientChan := make(chan *pb.Message, s.bufferSize)

	s.mu.Lock()
	s.subscribers[req.Topic] = append(s.subscribers[req.Topic], clientChan)
	s.mu.Unlock()

	log.Printf("New subscriber in the topic: %s", req.Topic)

	defer func() {
		s.removeSubscriber(req.Topic, clientChan)
		close(clientChan)
		log.Printf("Subscriber removed from the thread: %s", req.Topic)
	}()

	// stays here forever, as long as the client is connected
	for {
		select {
		case msg := <-clientChan:
			// send msg to gRPC Stream
			if err := stream.Send(msg); err != nil {
				// if error trigger 'defer'
				return err
			}
		case <-stream.Context().Done():
			// client closed connection
			return nil
		}
	}
}

// helper that removes a specific channel from list
func (s *BrokerServer) removeSubscriber(topic string, targetChan chan *pb.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()

	subscribers, ok := s.subscribers[topic]
	if !ok {
		return
	}

	// filter list to remove target channel
	newSubs := make([]chan *pb.Message, 0, len(subscribers)-1)
	for _, ch := range subscribers {
		if ch != targetChan {
			newSubs = append(newSubs, ch)
		}
	}

	if len(newSubs) == 0 {
		delete(s.subscribers, topic)
	} else {
		s.subscribers[topic] = newSubs
	}
}
