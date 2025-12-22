package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/pedromedina19/hermes-broker/pb"
)

func main() {
	mode := flag.String("mode", "sub", "Modo: 'pub' ou 'sub'")
	topic := flag.String("topic", "default-topic", "Tópico")
	groupID := flag.String("group", "", "Consumer Group ID (optional)")
	msgContent := flag.String("msg", "Hello Hermes", "content (pub)")
	count := flag.Int("count", 1, "Qtd (pub)")
	shouldAck := flag.Bool("ack", true, "If true, sends an ACK. If false, simulates a failure")
	flag.Parse()

	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewBrokerServiceClient(conn)
	ctx := context.Background()

	switch *mode {
	case "sub":
		runSubscriber(ctx, client, *topic, *groupID, *shouldAck)
	case "pub":
		runPublisher(ctx, client, *topic, *msgContent, *count)
	default:
		log.Fatalf("Unknown mode")
	}
}

func runSubscriber(ctx context.Context, client pb.BrokerServiceClient, topic, groupID string, shouldAck bool) {
	log.Printf("🎧 Connecting to topic: [%s] | Group: [%s]...", topic, groupID)

	stream, err := client.Subscribe(ctx)
	if err != nil {
		log.Fatalf("Error opening stream: %v", err)
    }

	log.Println("Sending Handshake...")
	err = stream.Send(&pb.SubscribeRequest{
		Action:  "SUBSCRIBE",
		Topic:   topic,
		GroupId: groupID,
	})
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	waitc := make(chan struct{})
	go func() {
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				close(waitc)
				return
			}
			if err != nil {
				log.Fatalf("Stream receive error: %v", err)
			}

			ts := time.Unix(0, msg.Timestamp)
			fmt.Printf("Received: %s | ID: %s | Time: %s\n", string(msg.Payload), msg.Id, ts.Format(time.TimeOnly))

			if shouldAck {
				ackErr := stream.Send(&pb.SubscribeRequest{
					Action:       "ACK",
					AckMessageId: msg.Id,
				})
				if ackErr != nil {
					log.Printf("Failed to ACK: %v", ackErr)
				}
			} else {
				log.Printf("IGNORING ACK for %s (Simulating Failure)", msg.Id)
			}
		}
	}()
	<-waitc
}

func runPublisher(ctx context.Context, client pb.BrokerServiceClient, topic, content string, count int) {
	for i := 0; i < count; i++ {
		payload := fmt.Sprintf("%s - Seq %d", content, i+1)

		_, err := client.Publish(ctx, &pb.PublishRequest{
			Topic:   topic,
            Payload: []byte(payload),
		})

		if err != nil {
			log.Printf("Error publishing: %v", err)
		} else {
			log.Printf("Published to [%s]: %s", topic, payload)
		}
		time.Sleep(100 * time.Millisecond)
	}
}