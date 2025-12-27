package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/pedromedina19/hermes-broker/pb"
)

var (
	grpcTargetsStr string
	httpTargetsStr string
	subTarget      string

	grpcPool []string
	httpPool []string

	topic = "benchmark-topic"

	// Global Atomic Metrics
	totalReqs     uint64 // Messages sent successfully
	totalErrs     uint64 // Actual network/logic errors
	totalReceived uint64 // Messages that arrived at the Auditor

	pubA uint64
	pubB uint64
	subA uint64
	subB uint64

	errorCounts = make(map[string]int)
	errorMu     sync.Mutex

	currentScenario string
)

func recordError(err error) {
	if err == nil {
		return
	}
	if err == context.DeadlineExceeded || err == context.Canceled {
		return
	}
	errMsg := err.Error()
	if strings.Contains(errMsg, "context deadline exceeded") ||
		strings.Contains(errMsg, "context canceled") ||
		strings.Contains(errMsg, "canceled") {
		return
	}
	atomic.AddUint64(&totalErrs, 1)
	errorMu.Lock()
	errorCounts[errMsg]++
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
	duration := flag.Duration("duration", 15*time.Second, "Test duration")
	scenario := flag.String("scenario", "live", "Cenários: live, disk, slow, late, crash, isolation")
	protocol := flag.String("protocol", "all", "Protocolo: grpc, rest, graphql, all")

	flag.StringVar(&grpcTargetsStr, "grpc-targets", "localhost:50051,localhost:50052,localhost:50053", "List of gRPC nodes (csv)")
	flag.StringVar(&httpTargetsStr, "http-targets", "localhost:8080,localhost:8081,localhost:8082", "HTTP Node List (csv)")
	flag.StringVar(&subTarget, "sub-target", "localhost:50051", "Specific gRPC node for the Subscriber (Auditor)")

	flag.Parse()
	currentScenario = *scenario

	grpcPool = strings.Split(grpcTargetsStr, ",")
	httpPool = strings.Split(httpTargetsStr, ",")

	fmt.Printf("STARTING TEST: %s \n", strings.ToUpper(*scenario))
	fmt.Printf("Protocol: %s | Workers: %d | Duration: %v\n", strings.ToUpper(*protocol), *workers, *duration)
	fmt.Printf("Publisher Pool: %v\n", grpcPool)
	fmt.Printf("Subscriber Target: %s\n\n", subTarget)

	pubCtx, pubCancel := context.WithTimeout(context.Background(), *duration)
	defer pubCancel()

	subCtx, subCancel := context.WithCancel(context.Background())
	defer subCancel()

	readyWg := &sync.WaitGroup{}
	var wg sync.WaitGroup

	processingDelay := time.Duration(0)
	groupID := ""

	switch *scenario {
	case "live":
		readyWg.Add(1)
		go runInternalSubscriber(subCtx, readyWg, topic, "", 0, &totalReceived)
		readyWg.Wait()
		fmt.Println("Connected auditor (Live Mode)")
	case "slow":
		processingDelay = 2 * time.Millisecond
		groupID = "ha-slow-group"
		readyWg.Add(1)
		go runInternalSubscriber(subCtx, readyWg, topic, groupID, processingDelay, &totalReceived)
		readyWg.Wait()
		fmt.Println("MODO SLOW: Slow subscriber with Consumption Group for Failover")
	case "disk":
		groupID = "__REPLAY__"
		fmt.Println("MODO DISK: The auditor will wait until the publication is complete to read it from scratch")
	case "late":
		groupID = "__REPLAY__"
		fmt.Println("MODO LATE: Subscriber will enter halfway through the test.")
		time.AfterFunc(*duration/2, func() {
			fmt.Println("\n🏃 Late subscriber joining the race!")
			readyWg.Add(1)
			go runInternalSubscriber(subCtx, readyWg, topic, groupID, 0, &totalReceived)
		})
	case "crash":
		groupID = "persistent-group-01"
		fmt.Println("CRASH MODE: Subscriber will die halfway through and then be resurrected.")
		readyWg.Add(1)
		crashCtx, crashCancel := context.WithCancel(context.Background())
		go func() {
			runInternalSubscriber(crashCtx, readyWg, topic, groupID, 0, &totalReceived)
		}()
		time.AfterFunc(*duration/2, func() {
			fmt.Println("\nSubscriber DIED (Simulated Crash)!")
			crashCancel()
			time.AfterFunc(2*time.Second, func() {
				fmt.Println("\n🧟 Subscriber RESURRECTED (Offset Resuming)!")
				go runInternalSubscriber(subCtx, nil, topic, groupID, 0, &totalReceived)
			})
		})
	case "isolation":
		fmt.Println("ISOLATION MODE: Validating message separation between Topic A and B.")
		readyWg.Add(2)
		go runInternalSubscriber(subCtx, readyWg, "topic-A", "", 0, &subA)
		go runInternalSubscriber(subCtx, readyWg, "topic-B", "", 0, &subB)
		readyWg.Wait()
		fmt.Println("Auditors A and B connected.")
	default:
		log.Fatalf("Unknown scenario: %s", *scenario)
	}

	start := time.Now()

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		p := strings.ToLower(*protocol)
		if p == "all" {
			if i%3 == 0 {
				go runGrpcWorker(pubCtx, &wg, i)
			} else if i%3 == 1 {
				go runRestWorker(pubCtx, &wg, i)
			} else {
				go runGraphqlWorker(pubCtx, &wg, i)
			}
		} else {
			switch p {
			case "grpc":
				go runGrpcWorker(pubCtx, &wg, i)
			case "rest":
				go runRestWorker(pubCtx, &wg, i)
			case "graphql":
				go runGraphqlWorker(pubCtx, &wg, i)
			default:
				log.Fatalf("Invalid protocol: %s", *protocol)
			}
		}
	}

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-pubCtx.Done():
				return
			case <-ticker.C:
				p := atomic.LoadUint64(&totalReqs)
				if *scenario == "isolation" {
					pa, pb := atomic.LoadUint64(&pubA), atomic.LoadUint64(&pubB)
					sa, sb := atomic.LoadUint64(&subA), atomic.LoadUint64(&subB)
					fmt.Printf("\rA:[Pub:%d Sub:%d] | B:[Pub:%d Sub:%d]", pa, sa, pb, sb)
				} else {
					r := atomic.LoadUint64(&totalReceived)
					var lag uint64
					if p >= r {
						lag = p - r
					} else {
						lag = 0
					}
					fmt.Printf("\rPub: %d | Sub: %d | Pendente: %d", p, r, lag)
				}
			}
		}
	}()

	wg.Wait()
	fmt.Println("\n\n")

	if *scenario == "disk" {
		fmt.Println("Publication closed. Final drainage initiated...")
		readyWg.Add(1)
		go runInternalSubscriber(subCtx, readyWg, topic, groupID, 0, &totalReceived)
		readyWg.Wait()
	}

	fmt.Println("Awaiting complete drainage (Monitoring cluster stability)...")

	drainTicker := time.NewTicker(1 * time.Second)
	defer drainTicker.Stop()
	lastReceivedCount := atomic.LoadUint64(&totalReceived)
	stuckSeconds := 0

drainLoop:
	for {
		select {
		case <-drainTicker.C:
			p := atomic.LoadUint64(&totalReqs)
			r := atomic.LoadUint64(&totalReceived)
			if *scenario == "isolation" {
				r = atomic.LoadUint64(&subA) + atomic.LoadUint64(&subB)
			}

			fmt.Printf("\rDraining: %d / %d | Cluster stopped for: %ds", r, p, stuckSeconds)

			if r >= p && p > 0 {
				fmt.Println("\nSuccess: All messages processed!")
				break drainLoop
			}

			if r == lastReceivedCount {
				stuckSeconds++
			} else {
				stuckSeconds = 0
			}
			lastReceivedCount = r

			if stuckSeconds >= 120 {
				fmt.Println("\nError: Cluster locked for 120s. Drain aborted.")
				break drainLoop
			}
		}
	}

	elapsed := time.Since(start)
	printReport(elapsed, *scenario)
}

func runInternalSubscriber(ctx context.Context, readyWg *sync.WaitGroup, topicName, groupID string, delay time.Duration, metric *uint64) {
	ackChan := make(chan string, 200000)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		target := subTarget
		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		client := pb.NewBrokerServiceClient(conn)
		stream, err := client.Subscribe(ctx)
		if err != nil {
			conn.Close()
			time.Sleep(500 * time.Millisecond)
			continue
		}

		err = stream.Send(&pb.SubscribeRequest{
			Action:  "SUBSCRIBE",
			Topic:   topicName,
			GroupId: groupID,
		})
		if err != nil {
			conn.Close()
			continue
		}

		if readyWg != nil {
			readyWg.Done()
			readyWg = nil
		}

		ctxSub, cancelSub := context.WithCancel(ctx)
		go func(c pb.BrokerService_SubscribeClient, cancelFunc context.CancelFunc) {
			defer cancelFunc()
			for {
				select {
				case <-ctxSub.Done():
					return
				case id := <-ackChan:
					if sendErr := c.Send(&pb.SubscribeRequest{Action: "ACK", AckMessageId: id}); sendErr != nil {
						return
					}
				}
			}
		}(stream, cancelSub)

		for {
			msg, err := stream.Recv()
			if err != nil {
				conn.Close()
				cancelSub()
				break
			}
			if delay > 0 {
				time.Sleep(delay)
			}
			atomic.AddUint64(metric, 1)
			select {
			case ackChan <- msg.Id:
			default:
			}
		}
		time.Sleep(1 * time.Second)
	}
}

func getTopicForID(id int) string {
	if currentScenario == "isolation" {
		if id%2 == 0 {
			return "topic-A"
		}
		return "topic-B"
	}
	return topic
}

func runGrpcWorker(ctx context.Context, wg *sync.WaitGroup, id int) {
	defer wg.Done()
	targetIndex := id % len(grpcPool)
	var conn *grpc.ClientConn
	var client pb.BrokerServiceClient

	reconnect := func() bool {
		if conn != nil {
			conn.Close()
		}
		addr := grpcPool[targetIndex]
		var err error
		conn, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			targetIndex = (targetIndex + 1) % len(grpcPool)
			return false
		}
		client = pb.NewBrokerServiceClient(conn)
		return true
	}

	if !reconnect() {
		return
	}
	defer conn.Close()

	targetTopic := getTopicForID(id)
	payload := []byte(fmt.Sprintf("gRPC-%d", id))

	for {
		select {
		case <-ctx.Done():
			return
		default:
			_, err := client.Publish(ctx, &pb.PublishRequest{Topic: targetTopic, Payload: payload})
			if err != nil {
				targetIndex = (targetIndex + 1) % len(grpcPool)
				reconnect()
				time.Sleep(100 * time.Millisecond)
			} else {
				atomic.AddUint64(&totalReqs, 1)
				if targetTopic == "topic-A" {
					atomic.AddUint64(&pubA, 1)
				}
				if targetTopic == "topic-B" {
					atomic.AddUint64(&pubB, 1)
				}
			}
		}
	}
}

func runRestWorker(ctx context.Context, wg *sync.WaitGroup, id int) {
	defer wg.Done()
	client := &http.Client{Timeout: 1 * time.Second}
	targetIndex := id % len(httpPool)
	targetTopic := getTopicForID(id)
	jsonStr := fmt.Sprintf(`{"topic": "%s", "payload": "REST-%d"}`, targetTopic, id)
	var buf bytes.Buffer

	for {
		select {
		case <-ctx.Done():
			return
		default:
			buf.Reset()
			buf.WriteString(jsonStr)
			url := "http://" + httpPool[targetIndex] + "/publish"

			resp, err := client.Post(url, "application/json", bytes.NewBuffer(buf.Bytes()))
			if err != nil || (resp != nil && resp.StatusCode != 200) {
				if resp != nil {
					resp.Body.Close()
				}
				targetIndex = (targetIndex + 1) % len(httpPool)
				time.Sleep(100 * time.Millisecond)
				continue
			}
			atomic.AddUint64(&totalReqs, 1)
			if targetTopic == "topic-A" {
				atomic.AddUint64(&pubA, 1)
			}
			if targetTopic == "topic-B" {
				atomic.AddUint64(&pubB, 1)
			}
			resp.Body.Close()
		}
	}
}

func runGraphqlWorker(ctx context.Context, wg *sync.WaitGroup, id int) {
	defer wg.Done()
	client := &http.Client{Timeout: 1 * time.Second}
	targetIndex := id % len(httpPool)
	targetTopic := getTopicForID(id)
	query := fmt.Sprintf(`{"query": "mutation { publish(topic: \"%s\", payload: \"GQL-%d\") { success } }"}`, targetTopic, id)
	var buf bytes.Buffer

	for {
		select {
		case <-ctx.Done():
			return
		default:
			buf.Reset()
			buf.WriteString(query)
			url := "http://" + httpPool[targetIndex] + "/query"

			resp, err := client.Post(url, "application/json", bytes.NewBuffer(buf.Bytes()))
			if err != nil || (resp != nil && resp.StatusCode != 200) {
				if resp != nil {
					resp.Body.Close()
				}
				targetIndex = (targetIndex + 1) % len(httpPool)
				time.Sleep(100 * time.Millisecond)
				continue
			}
			atomic.AddUint64(&totalReqs, 1)
			if targetTopic == "topic-A" {
				atomic.AddUint64(&pubA, 1)
			}
			if targetTopic == "topic-B" {
				atomic.AddUint64(&pubB, 1)
			}
			resp.Body.Close()
		}
	}
}

func printReport(elapsed time.Duration, scenario string) {
	pub := atomic.LoadUint64(&totalReqs)
	sub := atomic.LoadUint64(&totalReceived)
	if scenario == "isolation" {
		sub = atomic.LoadUint64(&subA) + atomic.LoadUint64(&subB)
	}

	errs := atomic.LoadUint64(&totalErrs)
	rps := float64(pub) / elapsed.Seconds()

	fmt.Println("\n--- FINAL REPORT ---")
	fmt.Printf("Total Time: %.2fs\n", elapsed.Seconds())
	fmt.Printf("Published Total: %d\n", pub)

	if scenario == "isolation" {
		fmt.Printf("   ├─ Topic A: %d\n", atomic.LoadUint64(&pubA))
		fmt.Printf("   └─ Topic B: %d\n", atomic.LoadUint64(&pubB))
		fmt.Printf("Total Received:  %d\n", sub)
		fmt.Printf("   ├─ Sub A: %d\n", atomic.LoadUint64(&subA))
		fmt.Printf("   └─ Sub B: %d\n", atomic.LoadUint64(&subB))
	} else {
		fmt.Printf("Received:  %d\n", sub)
	}

	loss := int64(pub) - int64(sub)
	if loss < 0 {
		loss = 0
	}

	lossRate := (float64(loss) / float64(pub)) * 100
	if pub == 0 {
		lossRate = 0
	}

	fmt.Printf("General Loss: %d (%.2f%%)\n", loss, lossRate)
	fmt.Printf("Network/Logic Errors: %d\n", errs)
	fmt.Printf("THROUGHPUT: %.2f msg/seg ⚡\n", rps)

	if errs > 0 {
		fmt.Println("\nERRORS FOUND:")
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