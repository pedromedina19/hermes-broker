package storage

import (
	"encoding/binary"
	"encoding/json"
	"sync"
	"time"

	"github.com/pedromedina19/hermes-broker/internal/core/domain"
	bolt "go.etcd.io/bbolt"
)
const (
	OffsetsBucket = "consumer_offsets"
	MetaBucket    = "_meta"            
	RaftIndexKey  = "last_raft_index" 
)
type BoltLogStore struct {
    db *bolt.DB
    mu sync.RWMutex
}

func NewBoltLogStore(path string) (*BoltLogStore, error) {
    options := &bolt.Options{
        Timeout: 1 * time.Second,
        NoSync: true, 
    }
    db, err := bolt.Open(path, 0600, options)
    if err != nil {
        return nil, err
    }
    err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(MetaBucket))
		return err
	})
    return &BoltLogStore{db: db}, nil
}

func itob(v uint64) []byte {
    b := make([]byte, 8)
    binary.BigEndian.PutUint64(b, v)
    return b
}

func (s *BoltLogStore) GetLastRaftIndex() (uint64, error) {
	var index uint64 = 0
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(MetaBucket))
		if b == nil {
			return nil
		}
		val := b.Get([]byte(RaftIndexKey))
		if val != nil {
			index = binary.BigEndian.Uint64(val)
		}
		return nil
	})
	return index, err
}

func (s *BoltLogStore) AppendBatch(msgs []*domain.Message, raftIndex uint64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		for _, msg := range msgs {
			b, err := tx.CreateBucketIfNotExists([]byte(msg.Topic))
			if err != nil {
				return err
			}
			id, _ := b.NextSequence()
			data, err := json.Marshal(msg)
			if err != nil {
				return err
			}
			if err := b.Put(itob(id), data); err != nil {
				return err
			}
		}

		if raftIndex > 0 {
			meta := tx.Bucket([]byte(MetaBucket))
			if err := meta.Put([]byte(RaftIndexKey), itob(raftIndex)); err != nil {
				return err
			}
		}
		
		return nil
	})
}

func (s *BoltLogStore) Append(msg domain.Message) (uint64, error) {
	var id uint64
	err := s.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(msg.Topic))
		if err != nil {
			return err
		}
		id, _ = b.NextSequence()
		data, err := json.Marshal(msg)
		if err != nil {
			return err
		}
		return b.Put(itob(id), data)
	})
	return id, err
}

func (s *BoltLogStore) ReadBatch(topic string, startOffset uint64, limit int) ([]domain.Message, uint64, error) {
    var rawMessages [][]byte
    nextOffset := startOffset

    err := s.db.View(func(tx *bolt.Tx) error {
        b := tx.Bucket([]byte(topic))
        if b == nil {
            return nil
        }

        c := b.Cursor()
        k, v := c.Seek(itob(startOffset))

        count := 0
        for k != nil && count < limit {
            msgData := make([]byte, len(v))
            copy(msgData, v)
            rawMessages = append(rawMessages, msgData)

            currentID := binary.BigEndian.Uint64(k)
            nextOffset = currentID + 1

            count++
            k, v = c.Next()
        }
        return nil
    })

    if err != nil {
        return nil, startOffset, err
    }

    var msgs []domain.Message
    for _, data := range rawMessages {
        var msg domain.Message
        if err := json.Unmarshal(data, &msg); err == nil {
            msgs = append(msgs, msg)
        }
    }

    return msgs, nextOffset, nil
}

func (s *BoltLogStore) LastOffset(topic string) uint64 {
    var last uint64
    s.db.View(func(tx *bolt.Tx) error {
        b := tx.Bucket([]byte(topic))
        if b == nil { return nil }
        last = b.Sequence()
        return nil
    })
    return last
}

func (s *BoltLogStore) Close() error {
    return s.db.Close()
}

// key: "topic:groupID" -> value: offset (uint64)
func (s *BoltLogStore) SaveOffset(topic, groupID string, offset uint64, raftIndex uint64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(OffsetsBucket))
		if err != nil {
			return err
		}
		key := []byte(topic + ":" + groupID)
		if err := b.Put(key, itob(offset)); err != nil {
			return err
		}

		if raftIndex > 0 {
			meta := tx.Bucket([]byte(MetaBucket))
			return meta.Put([]byte(RaftIndexKey), itob(raftIndex))
		}
		return nil
	})
}

// GetOffset: Pick up where the customer left off
func (s *BoltLogStore) GetOffset(topic, groupID string) (uint64, error) {
    var offset uint64 = 0 // If you can't find it, start from scratch (or it could be configurable)
    
    err := s.db.View(func(tx *bolt.Tx) error {
        b := tx.Bucket([]byte(OffsetsBucket))
        if b == nil {
            return nil
        }
        key := []byte(topic + ":" + groupID)
        val := b.Get(key)
        if val != nil {
            offset = binary.BigEndian.Uint64(val)
        }
        return nil
    })
    return offset, err
}