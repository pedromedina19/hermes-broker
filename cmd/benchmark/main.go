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

	grpcPool []string
	httpPool []string

	topic         = "benchmark-topic"
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
	workers := flag.Int("workers", 60, "Workers simultâneos")
	duration := flag.Duration("duration", 15*time.Second, "Duração")

	flag.StringVar(&grpcTargetsStr, "grpc-targets", "localhost:50051,localhost:50052,localhost:50053", "Lista de nós gRPC (csv)")
	flag.StringVar(&httpTargetsStr, "http-targets", "localhost:8080,localhost:8081,localhost:8082", "Lista de nós HTTP (csv)")

	flag.Parse()

	grpcPool = strings.Split(grpcTargetsStr, ",")
	httpPool = strings.Split(httpTargetsStr, ",")

	fmt.Printf("INICIANDO SMART BENCHMARK (CLUSTER AWARE)\n")
	fmt.Printf("Workers: %d\n", *workers)
	fmt.Printf("Alvos gRPC: %v\n", grpcPool)
	fmt.Printf("Alvos HTTP: %v\n", httpPool)

	subCtx, subCancel := context.WithCancel(context.Background())
	defer subCancel()

	readyWg := &sync.WaitGroup{}
	readyWg.Add(1)
	go runInternalSubscriber(subCtx, readyWg, grpcPool[len(grpcPool)-1])
	readyWg.Wait()

	fmt.Println("Auditor conectado. Iniciando ataque distribuído...")
	start := time.Now()

	doneChan := make(chan struct{})
	time.AfterFunc(*duration, func() {
		close(doneChan)
		fmt.Println("\nTempo esgotado. Parando novos envios...")
	})

	var wg sync.WaitGroup
	squadSize := *workers / 3

	for i := 0; i < squadSize; i++ {
		wg.Add(1)
		go runSmartGrpcWorker(doneChan, &wg, i)
	}
	for i := 0; i < squadSize; i++ {
		wg.Add(1)
		go runSmartRestWorker(doneChan, &wg, i)
	}
	for i := 0; i < squadSize; i++ {
		wg.Add(1)
		go runSmartGraphqlWorker(doneChan, &wg, i)
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
				fmt.Printf("\r Pub: %d | Sub: %d | Erros: %d", currPub, currSub, atomic.LoadUint64(&totalErrs))
			}
		}
	}()

	wg.Wait()
	close(monitorStop)

	fmt.Println("\nPublicação encerrada. Aguardando o Auditor processar o backlog...")

	kickerCtx, kickerCancel := context.WithCancel(context.Background())
	runDrainKicker(kickerCtx, httpPool)

	drainTimeout := time.After(120 * time.Second)
	drainTicker := time.NewTicker(1 * time.Second)

	defer drainTicker.Stop()

drainLoop:
	for {
		select {
		case <-drainTimeout:
			kickerCancel()
			fmt.Println("\nTimeout de drenagem atingido (Auditor muito lento).")
			break drainLoop
		case <-drainTicker.C:
			p := atomic.LoadUint64(&totalReqs)
			r := atomic.LoadUint64(&totalReceived)

			fmt.Printf("\r⏳ Drenando... Pendentes: %d (Recebidos: %d / %d)", p-r, r, p)

			if r >= p {
				kickerCancel()
				fmt.Println("\n\nSUCESSO: Todas as mensagens foram drenadas!")
				break drainLoop
			}
		}
	}

	elapsed := time.Since(start)
	printReport(elapsed)
}


func runSmartGrpcWorker(done <-chan struct{}, wg *sync.WaitGroup, id int) {
	defer wg.Done()

	targetIndex := id % len(grpcPool)
	var conn *grpc.ClientConn
	var client pb.BrokerServiceClient
	var err error

	connect := func() bool {
		if conn != nil {
			conn.Close()
		}
		addr := grpcPool[targetIndex]
		conn, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			targetIndex = (targetIndex + 1) % len(grpcPool)
			return false
		}
		client = pb.NewBrokerServiceClient(conn)
		return true
	}

	if !connect() {
		return
	}
	defer conn.Close()

	payload := []byte(fmt.Sprintf("gRPC-%d", id))

	for {
		select {
		case <-done:
			return
		default:
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err := client.Publish(ctx, &pb.PublishRequest{Topic: topic, Payload: payload})
			cancel()

			if err != nil {
				targetIndex = (targetIndex + 1) % len(grpcPool)
				connect()
				time.Sleep(100 * time.Millisecond)
			} else {
				atomic.AddUint64(&totalReqs, 1)
			}
		}
	}
}

func runSmartRestWorker(done <-chan struct{}, wg *sync.WaitGroup, id int) {
	defer wg.Done()
	client := &http.Client{Timeout: 2 * time.Second}
	targetIndex := id % len(httpPool)

	jsonStr := fmt.Sprintf(`{"topic": "%s", "payload": "REST-%d"}`, topic, id)
	payload := []byte(jsonStr)

	for {
		select {
		case <-done:
			return
		default:
			url := "http://" + httpPool[targetIndex] + "/publish"
			resp, err := client.Post(url, "application/json", bytes.NewBuffer(payload))

			// Failover Trigger
			if err != nil || resp.StatusCode != 200 {
				if resp != nil {
					resp.Body.Close()
				}
				targetIndex = (targetIndex + 1) % len(httpPool)
				time.Sleep(100 * time.Millisecond)
				continue
			}

			resp.Body.Close()
			atomic.AddUint64(&totalReqs, 1)
		}
	}
}

func runSmartGraphqlWorker(done <-chan struct{}, wg *sync.WaitGroup, id int) {
	defer wg.Done()
	client := &http.Client{Timeout: 2 * time.Second}
	targetIndex := id % len(httpPool)

	query := fmt.Sprintf(`{"query": "mutation { publish(topic: \"%s\", payload: \"GQL-%d\") { success } }"}`, topic, id)
	payload := []byte(query)

	for {
		select {
		case <-done:
			return
		default:
			url := "http://" + httpPool[targetIndex] + "/query"
			resp, err := client.Post(url, "application/json", bytes.NewBuffer(payload))

			// Failover Trigger
			if err != nil || resp.StatusCode != 200 {
				if resp != nil {
					resp.Body.Close()
				}
				targetIndex = (targetIndex + 1) % len(httpPool)
				time.Sleep(100 * time.Millisecond)
				continue
			}

			resp.Body.Close()
			atomic.AddUint64(&totalReqs, 1)
		}
	}
}

func runInternalSubscriber(ctx context.Context, readyWg *sync.WaitGroup, addr string) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Auditor failed connect: %v", err)
		readyWg.Done()
		return
	}

	go func() { <-ctx.Done(); conn.Close() }()

	client := pb.NewBrokerServiceClient(conn)
	stream, err := client.Subscribe(ctx)
	if err != nil {
		readyWg.Done()
		return
	}

	stream.Send(&pb.SubscribeRequest{Action: "SUBSCRIBE", Topic: topic})
	readyWg.Done()

	for {
		msg, err := stream.Recv()
		if err != nil {
			return
		}
		atomic.AddUint64(&totalReceived, 1)
		stream.Send(&pb.SubscribeRequest{Action: "ACK", AckMessageId: msg.Id})
	}
}

func printReport(elapsed time.Duration) {
	pub := atomic.LoadUint64(&totalReqs)
	sub := atomic.LoadUint64(&totalReceived)
	errs := atomic.LoadUint64(&totalErrs)
	rps := float64(pub) / elapsed.Seconds()

	fmt.Println("\n--- RELATÓRIO FINAL (CLUSTER) 🏁 ---")
	fmt.Printf("Tempo Total: %.2fs\n", elapsed.Seconds())
	fmt.Printf("Publicados: %d\n", pub)
	fmt.Printf("Recebidos:  %d\n", sub)
	fmt.Printf("Erros Totais: %d\n", errs)
	fmt.Printf("⚡ THROUGHPUT: %.2f msg/seg ⚡\n", rps)
	fmt.Println("-----------------------------------")
}

func runDrainKicker(ctx context.Context, pool []string) {
	client := &http.Client{Timeout: 500 * time.Millisecond}
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	dummyPayload := []byte(`{"topic": "heartbeat", "payload": "KEEP_ALIVE"}`)

	go func() {
		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				target := pool[i%len(pool)]
				url := fmt.Sprintf("http://%s/publish", target)
				resp, _ := client.Post(url, "application/json", bytes.NewBuffer(dummyPayload))
				if resp != nil {
					resp.Body.Close()
				}
				i++
			}
		}
	}()
}
