package domain

import "time"

type Message struct {
	ID        string
	Topic     string
	Payload   []byte
	Timestamp time.Time
	DeliveryAttempts int
}

type BrokerStats struct {
	TotalSubscribers int
	BufferedMessages int
}