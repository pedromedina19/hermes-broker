package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/pedromedina19/hermes-broker/pb"
)

var (
	grpcAddr = "localhost:50051"
	httpAddr = "http://localhost:8080"
	topic    = "benchmark-topic"

	totalReqs     uint64
	totalErrs     uint64
	totalReceived uint64

	errorCounts = make(map[string]int)
	errorMu     sync.Mutex
)

func recordError(err error) {
	if err == nil {
		return
	}
	atomic.AddUint64(&totalErrs, 1)

	msg := err.Error()
	errorMu.Lock()
	errorCounts[msg]++
	errorMu.Unlock()
}

func recordCustomError(msg string) {
	atomic.AddUint64(&totalErrs, 1)
	errorMu.Lock()
	errorCounts[msg]++
	errorMu.Unlock()
}

func main() {
	workers := flag.Int("workers", 60, "Total number of simultaneous workers")
	duration := flag.Duration("duration", 10*time.Second, "Test duration")
	flag.Parse()

	fmt.Printf("STARTING LOAD TEST (SOFT STOP)\n")
	fmt.Printf("Workers: %d\n", *workers)
	fmt.Printf("Duração: %v\n\n", *duration)

	subCtx, subCancel := context.WithCancel(context.Background())
	defer subCancel()

	readyWg := &sync.WaitGroup{}
	readyWg.Add(1)
	go runInternalSubscriber(subCtx, readyWg)
	readyWg.Wait()

	fmt.Println("Auditor connected. Initiating attack...")
	start := time.Now()

	doneChan := make(chan struct{})

	time.AfterFunc(*duration, func() {
		close(doneChan)
		fmt.Println("\nTime expired. Stopping new shipments (awaiting in-flight)...")
	})

	var wg sync.WaitGroup
	squadSize := *workers / 3

	for i := 0; i < squadSize; i++ {
		wg.Add(1)
		go runGrpcWorker(doneChan, &wg, i)
	}
	for i := 0; i < squadSize; i++ {
		wg.Add(1)
		go runRestWorker(doneChan, &wg, i)
	}
	for i := 0; i < squadSize; i++ {
		wg.Add(1)
		go runGraphqlWorker(doneChan, &wg, i)
	}

	monitorStop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-monitorStop:
				return
			case <-ticker.C:
				currPub := atomic.LoadUint64(&totalReqs)
				currSub := atomic.LoadUint64(&totalReceived)
				elapsed := time.Since(start).Seconds()
				if elapsed > 0 {
					fmt.Printf("\rPub: %d | Sub: %d | errors: %d",
						currPub, currSub, atomic.LoadUint64(&totalErrs))
				}
			}
		}
	}()

	wg.Wait()
	close(monitorStop)

	fmt.Println("\nPublication closed. Awaiting drainage (5s)...")

	drainTimeout := time.After(5 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
drainLoop:
	for {
		select {
		case <-drainTimeout:
			fmt.Println("Drainage timeout reached.")
			break drainLoop
		case <-ticker.C:
			p := atomic.LoadUint64(&totalReqs)
			r := atomic.LoadUint64(&totalReceived)
			if r >= p && p > 0 {
				fmt.Println("All messages have been drained")
				break drainLoop
			}
		}
	}

	elapsed := time.Since(start)
	printReport(elapsed)
}

// WORKERS 

func runGrpcWorker(done <-chan struct{}, wg *sync.WaitGroup, id int) {
	defer wg.Done()
	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		recordError(fmt.Errorf("gRPC Connect: %w", err))
		return
	}
	defer conn.Close()
	client := pb.NewBrokerServiceClient(conn)
	payload := []byte(fmt.Sprintf("gRPC-%d", id))

	for {
		select {
		case <-done:
			return
		default:
			// Creates an INDEPENDENT context for each request (5s timeout)
			reqCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

			_, err := client.Publish(reqCtx, &pb.PublishRequest{Topic: topic, Payload: payload})
			if err != nil {
				recordError(err)
			} else {
				atomic.AddUint64(&totalReqs, 1)
			}
			cancel()
		}
	}
}

func runRestWorker(done <-chan struct{}, wg *sync.WaitGroup, id int) {
	defer wg.Done()
	client := &http.Client{Timeout: 5 * time.Second}
	url := httpAddr + "/publish"
	jsonStr := fmt.Sprintf(`{"topic": "%s", "payload": "REST-%d"}`, topic, id)
	payload := []byte(jsonStr)

	for {
		select {
		case <-done:
			return
		default:
			resp, err := client.Post(url, "application/json", bytes.NewBuffer(payload))
			if err != nil {
				recordError(fmt.Errorf("REST Net: %w", err))
				continue
			}
			if resp.StatusCode != 200 {
				body, _ := io.ReadAll(resp.Body)
				recordCustomError(fmt.Sprintf("REST %d: %s", resp.StatusCode, string(body)))
			} else {
				atomic.AddUint64(&totalReqs, 1)
			}
			resp.Body.Close()
		}
	}
}

func runGraphqlWorker(done <-chan struct{}, wg *sync.WaitGroup, id int) {
	defer wg.Done()
	client := &http.Client{Timeout: 5 * time.Second}
	url := httpAddr + "/query"
	query := fmt.Sprintf(`{"query": "mutation { publish(topic: \"%s\", payload: \"GQL-%d\") { success } }"}`, topic, id)
	payload := []byte(query)

	for {
		select {
		case <-done:
			return
		default:
			resp, err := client.Post(url, "application/json", bytes.NewBuffer(payload))
			if err != nil {
				recordError(fmt.Errorf("GQL Net: %w", err))
				continue
			}
			if resp.StatusCode != 200 {
				body, _ := io.ReadAll(resp.Body)
				recordCustomError(fmt.Sprintf("GQL %d: %s", resp.StatusCode, string(body)))
			} else {
				atomic.AddUint64(&totalReqs, 1)
			}
			resp.Body.Close()
		}
	}
}

//SUBSCRIBER (AUDITOR)
func runInternalSubscriber(ctx context.Context, readyWg *sync.WaitGroup) {
	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Auditor failed to connect: %v", err)
	}
	// Don't close the connection immediately, let the context decide
	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	client := pb.NewBrokerServiceClient(conn)
	stream, err := client.Subscribe(ctx)
	if err != nil {
		// It may fail if the server crashes, but it's not fatal for the test
		return
	}

	stream.Send(&pb.SubscribeRequest{
		Action: "SUBSCRIBE",
		Topic:  topic,
	})

	readyWg.Done()

	for {
		msg, err := stream.Recv()
		if err != nil {
			return
		}
		atomic.AddUint64(&totalReceived, 1)
		stream.Send(&pb.SubscribeRequest{
			Action:       "ACK",
			AckMessageId: msg.Id,
		})
	}
}

func printReport(elapsed time.Duration) {
	pub := atomic.LoadUint64(&totalReqs)
	sub := atomic.LoadUint64(&totalReceived)
	errs := atomic.LoadUint64(&totalErrs)
	rps := float64(pub) / elapsed.Seconds()

	fmt.Println("\n--- FINAL REPORT (SOFT STOP) ---")
	fmt.Printf("Total Time: %.2fs\n", elapsed.Seconds())
	fmt.Printf("Published: %d\n", pub)
	fmt.Printf("Received:  %d\n", sub)

	loss := int64(pub) - int64(sub)
	if loss < 0 {
		loss = 0
	}

	lossRate := (float64(loss) / float64(pub)) * 100
	if pub == 0 {
		lossRate = 0
	}

	fmt.Printf("Loss: %d (%.2f%%)\n", loss, lossRate)
	fmt.Printf("Send errors: %d\n", errs)
	fmt.Printf("THROUGHPUT: %.2f msg/seg ⚡\n", rps)

	if errs > 0 {
		fmt.Println("\nERRORS:")
		errorMu.Lock()
		for msg, count := range errorCounts {
			if len(msg) > 80 {
				msg = msg[:77] + "..."
			}
			fmt.Printf("   [%d] -> %s\n", count, msg)
		}
		errorMu.Unlock()
	}
	fmt.Println("-----------------------------------")
}
