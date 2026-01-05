package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/pedromedina19/hermes-broker/internal/core/domain"
)

const (
	OffsetsBucket = "consumer_offsets"
	MetaBucket    = "_meta"
	RaftIndexKey  = "last_raft_index"
)

// Key layout (lexicographic friendly):
// messages:  "m\x00<topic>\x00<offset:8be>" -> json(message)
// offsets:   "o\x00<topic>\x00<group>\x00"  -> uint64be(offset)
// meta:      "x\x00<name>\x00"              -> uint64be(value)

var (
	prefixMsg    = []byte{'m', 0x00}
	prefixOffset = []byte{'o', 0x00}
	prefixMeta   = []byte{'x', 0x00}
)

type PebbleLogStore struct {
	db    *pebble.DB
	cache *pebble.Cache

	// per-topic sequence (last offset). Not persisted; rebuilt from DB on demand
	seq sync.Map // map[string]*atomic.Uint64

	initMu sync.Mutex
}

func NewPebbleLogStore(dir string) (*PebbleLogStore, error) {
	cache := pebble.NewCache(64 << 20) // 64MB

	opts := &pebble.Options{
		Cache:        cache,
		MaxOpenFiles: 512,
	}

	db, err := pebble.Open(dir, opts)
	if err != nil {
		cache.Unref()
		return nil, err
	}

	return &PebbleLogStore{
		db:    db,
		cache: cache,
	}, nil
}

func (s *PebbleLogStore) Close() error {
	if s.db != nil {
		_ = s.db.Close()
	}
	if s.cache != nil {
		s.cache.Unref()
	}
	return nil
}

func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func btoi(b []byte) (uint64, error) {
	if len(b) != 8 {
		return 0, fmt.Errorf("invalid uint64 bytes len=%d", len(b))
	}
	return binary.BigEndian.Uint64(b), nil
}

func metaKey(name string) []byte {
	// "x\0<name>\0"
	k := make([]byte, 0, len(prefixMeta)+len(name)+1)
	k = append(k, prefixMeta...)
	k = append(k, name...)
	k = append(k, 0x00)
	return k
}

func msgPrefix(topic string) []byte {
	// "m\0<topic>\0"
	p := make([]byte, 0, len(prefixMsg)+len(topic)+1)
	p = append(p, prefixMsg...)
	p = append(p, topic...)
	p = append(p, 0x00)
	return p
}

func msgKey(topic string, offset uint64) []byte {
	p := msgPrefix(topic)
	k := make([]byte, len(p)+8)
	copy(k, p)
	binary.BigEndian.PutUint64(k[len(p):], offset)
	return k
}

func offsetKey(topic, groupID string) []byte {
	// "o\0<topic>\0<group>\0"
	k := make([]byte, 0, len(prefixOffset)+len(topic)+1+len(groupID)+1)
	k = append(k, prefixOffset...)
	k = append(k, topic...)
	k = append(k, 0x00)
	k = append(k, groupID...)
	k = append(k, 0x00)
	return k
}

// prefixUpperBound returns the smallest key that is > all keys with the given prefix
// If it cannot compute (all 0xFF), it returns nil (no upper bound)
func prefixUpperBound(prefix []byte) []byte {
	ub := append([]byte(nil), prefix...)
	for i := len(ub) - 1; i >= 0; i-- {
		if ub[i] != 0xFF {
			ub[i]++
			return ub[:i+1]
		}
	}
	return nil
}

func (s *PebbleLogStore) findLastOffset(topic string) (uint64, error) {
	p := msgPrefix(topic)
	ub := prefixUpperBound(p)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: p,
		UpperBound: ub,
	})
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	if !iter.Last() {
		return 0, nil
	}

	k := iter.Key()
	if !bytes.HasPrefix(k, p) || len(k) < len(p)+8 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(k[len(p):]), nil
}

func (s *PebbleLogStore) seqForTopic(topic string) (*atomic.Uint64, error) {
	if v, ok := s.seq.Load(topic); ok {
		return v.(*atomic.Uint64), nil
	}

	s.initMu.Lock()
	defer s.initMu.Unlock()

	if v, ok := s.seq.Load(topic); ok {
		return v.(*atomic.Uint64), nil
	}

	last, err := s.findLastOffset(topic)
	if err != nil {
		return nil, err
	}

	a := new(atomic.Uint64)
	a.Store(last)
	s.seq.Store(topic, a)
	return a, nil
}

func (s *PebbleLogStore) GetLastRaftIndex() (uint64, error) {
	val, closer, err := s.db.Get(metaKey(RaftIndexKey))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	defer closer.Close()

	return btoi(val)
}

func (s *PebbleLogStore) Append(msg domain.Message) (uint64, error) {
	seq, err := s.seqForTopic(msg.Topic)
	if err != nil {
		return 0, err
	}

	offset := seq.Add(1)

	data, err := json.Marshal(msg)
	if err != nil {
		return 0, err
	}

	if err := s.db.Set(msgKey(msg.Topic, offset), data, pebble.NoSync); err != nil {
		return 0, err
	}
	return offset, nil
}

func (s *PebbleLogStore) AppendBatch(msgs []*domain.Message, raftIndex uint64) error {
	b := s.db.NewBatch()
	defer b.Close()

	for _, msg := range msgs {
		seq, err := s.seqForTopic(msg.Topic)
		if err != nil {
			return err
		}

		offset := seq.Add(1)

		data, err := json.Marshal(msg)
		if err != nil {
			return err
		}
		if err := b.Set(msgKey(msg.Topic, offset), data, nil); err != nil {
			return err
		}
	}

	if raftIndex > 0 {
		if err := b.Set(metaKey(RaftIndexKey), itob(raftIndex), nil); err != nil {
			return err
		}
	}

	return b.Commit(pebble.NoSync)
}

func (s *PebbleLogStore) ReadBatch(topic string, startOffset uint64, limit int) ([]domain.Message, uint64, error) {
	p := msgPrefix(topic)
	ub := prefixUpperBound(p)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: p,
		UpperBound: ub,
	})
	if err != nil {
		return nil, startOffset, err
	}
	defer iter.Close()

	startKey := msgKey(topic, startOffset)

	var out []domain.Message
	nextOffset := startOffset

	for valid := iter.SeekGE(startKey); valid && len(out) < limit; valid = iter.Next() {
		k := iter.Key()
		if len(k) < len(p)+8 {
			continue
		}

		var m domain.Message
		if err := json.Unmarshal(iter.Value(), &m); err == nil {
			out = append(out, m)
		}

		cur := binary.BigEndian.Uint64(k[len(p):])
		nextOffset = cur + 1
	}

	return out, nextOffset, nil
}

func (s *PebbleLogStore) LastOffset(topic string) uint64 {
	seq, err := s.seqForTopic(topic)
	if err != nil {
		return 0
	}
	return seq.Load()
}

func (s *PebbleLogStore) SaveOffset(topic, groupID string, offset uint64, raftIndex uint64) error {
	b := s.db.NewBatch()
	defer b.Close()

	if err := b.Set(offsetKey(topic, groupID), itob(offset), nil); err != nil {
		return err
	}

	if raftIndex > 0 {
		if err := b.Set(metaKey(RaftIndexKey), itob(raftIndex), nil); err != nil {
			return err
		}
	}

	return b.Commit(pebble.NoSync)
}

func (s *PebbleLogStore) GetOffset(topic, groupID string) (uint64, error) {
	val, closer, err := s.db.Get(offsetKey(topic, groupID))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	defer closer.Close()

	return btoi(val)
}
