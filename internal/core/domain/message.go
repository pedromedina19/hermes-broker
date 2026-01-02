package domain

import "time"


const (
	LogTypePublish = "PUBLISH"
	LogTypeOffset  = "OFFSET"
	LogTypeReplica = "REPLICA"
)

type RaftCommand struct {
	Type         string    `json:"type"`
	OriginNodeID string    `json:"origin_node_id,omitempty"`
	Messages     []Message `json:"messages,omitempty"`
	OffsetCommit *Offset   `json:"offset,omitempty"`
}

type Offset struct {
	Topic   string `json:"topic"`
	GroupID string `json:"group_id"`
	Value   uint64 `json:"value"`
}
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