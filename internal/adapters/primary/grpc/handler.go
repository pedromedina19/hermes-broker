package grpc_adapter

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/pedromedina19/hermes-broker/internal/core/services"
	"github.com/pedromedina19/hermes-broker/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type GrpcHandler struct {
	pb.UnimplementedBrokerServiceServer
	service *services.BrokerService
	logger  *slog.Logger
}

func NewGrpcHandler(service *services.BrokerService, logger *slog.Logger) *GrpcHandler {
	return &GrpcHandler{
		service: service,
		logger:  logger,
	}
}

func (h *GrpcHandler) getLeaderGrpcAddr() string {
	leaderRaftAddr := h.service.GetLeaderID()
	if leaderRaftAddr == "" {
		return ""
	}

	host := strings.Replace(leaderRaftAddr, "node-", "hermes-", 1)
	host = strings.Split(host, ":")[0]
	if !strings.Contains(host, "hermes-internal") {
		host = fmt.Sprintf("%s.hermes-internal", host)
	}
	return fmt.Sprintf("%s:50051", host)
}

func (h *GrpcHandler) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	if !h.service.IsLeader() {
		if addr := h.getLeaderGrpcAddr(); addr != "" {
			grpc.SetHeader(ctx, metadata.Pairs("x-leader-hint", addr))
		}
	}

	err := h.service.Publish(ctx, req.Topic, req.Payload, req.Mode)
	if err != nil {
		h.logger.Error("Failed to publish", "error", err)
		return &pb.PublishResponse{Success: false}, status.Error(codes.Internal, err.Error())
	}
	return &pb.PublishResponse{Success: true}, nil
}

func (h *GrpcHandler) PublishStream(stream pb.BrokerService_PublishStreamServer) error {
	ctx := stream.Context()

	md := metadata.MD{}

	if !h.service.IsLeader() {
		if addr := h.getLeaderGrpcAddr(); addr != "" {
			md.Set("x-leader-hint", addr)
		}
	}

	if err := stream.SendHeader(md); err != nil {
		h.logger.Error("Failed to send stream header", "error", err)
		return status.Error(codes.Internal, "failed to send header")
	}

	var processed uint64
	var failed uint64

	for {
		req, err := stream.Recv()

		if err == io.EOF {
			return stream.SendAndClose(&pb.PublishSummary{
				ProcessedCount: processed,
				FailedCount:    failed,
			})
		}
		if err != nil {
			if status.Code(err) != codes.Canceled {
				h.logger.Error("Stream receive error", "err", err)
			}
			return err
		}
		err = h.service.Publish(ctx, req.Topic, req.Payload, req.Mode)
		if err != nil {
			failed++
		} else {
			processed++
		}
	}
}

func (h *GrpcHandler) PublishBatch(ctx context.Context, req *pb.PublishBatchRequest) (*pb.PublishResponse, error) {
	err := h.service.PublishBatchList(ctx, req.Topic, req.Payloads, req.Mode)
	if err != nil {
		return &pb.PublishResponse{Success: false}, status.Error(codes.Internal, err.Error())
	}
	return &pb.PublishResponse{Success: true}, nil
}

func (h *GrpcHandler) Subscribe(stream pb.BrokerService_SubscribeServer) error {
	ctx := stream.Context()

	// Handshake: The first package MUST be the SUBSCRIBE package with the Topic
	firstReq, err := stream.Recv()
	if err != nil {
		return status.Error(codes.InvalidArgument, "Stream error waiting for handshake")
	}
	if firstReq.Action != "SUBSCRIBE" || firstReq.Topic == "" {
		return status.Error(codes.InvalidArgument, "First message must be SUBSCRIBE with Topic")
	}

	topic := firstReq.Topic
	groupID := firstReq.GroupId
	h.logger.Info("Client requesting subscription", "topic", topic, "group", groupID)
	// subascribe for Engine
	msgChan, subID, err := h.service.Subscribe(ctx, topic, groupID)
	if err != nil {
		return err
	}
	defer h.service.RemoveSubscriber(topic, subID)

	// Output Loop (Server -> Client)
	// Goroutine to send messages while the main loop listens for ACKs.
	errChan := make(chan error, 1)

	go func() {
		for {
			select {
			case msg, ok := <-msgChan:
				if !ok {
					return // Channel closed
				}
				resp := &pb.Message{
					Id:        msg.ID,
					Topic:     msg.Topic,
					Payload:   msg.Payload,
					Timestamp: msg.Timestamp.UnixNano(),
				}
				if err := stream.Send(resp); err != nil {
					h.logger.Warn("Failed to send msg", "id", subID, "err", err)
					errChan <- err
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Input Loop (Client -> Server: ACKs)
	// This loop blocks Subscribe function, keeping stream alive.
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil // client close connection
		}
		if err != nil {
			// client drop connection
			// checked if there was a shipping error in the goroutine above
			select {
			case e := <-errChan:
				return e
			default:
				h.logger.Error("Stream recv error", "err", err)
				return err
			}
		}

		if req.Action == "ACK" {
			h.service.Acknowledge(subID, req.AckMessageId)
		}
	}
}

func (h *GrpcHandler) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	if req.NodeId == "" || req.RaftAddr == "" {
		return &pb.JoinResponse{Success: false, Error: "missing node_id or raft_addr"}, status.Error(codes.InvalidArgument, "missing params")
	}

	h.logger.Info("Received gRPC Join request", "node_id", req.NodeId, "addr", req.RaftAddr)

	err := h.service.JoinCluster(req.NodeId, req.RaftAddr)
	if err != nil {
		h.logger.Error("Failed to join cluster via gRPC", "error", err)
		// return a gRPC error to inform the client.
		return &pb.JoinResponse{Success: false, Error: err.Error()}, status.Error(codes.Internal, err.Error())
	}

	return &pb.JoinResponse{Success: true}, nil
}
