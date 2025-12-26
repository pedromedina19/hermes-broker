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

// Reset clears the fields for reuse via sync.Pool
func (m *Message) Reset() {
	m.ID = ""
	m.Topic = ""
	m.Payload = m.Payload[:0] // It retains the slice capacity but resets the content
	m.DeliveryAttempts = 0
}