package broker

import (
	"errors"
	"fmt"
	"hash/fnv"
	"my-kafka/internals/storage"
	"path/filepath"
	"sync"
)

type Broker struct {
	DataDir         string
	MaxSegmentBytes int64
	Topics          map[string]*Topic
	mu              sync.RWMutex
}

func NewBroker(dataDir string, maxSegmentBytes int64) *Broker {
	return &Broker{
		DataDir:         dataDir,
		MaxSegmentBytes: maxSegmentBytes,
		Topics:          make(map[string]*Topic),
	}
}

func (b *Broker) CreateTopic(name string, partitions int, replication int) error {
	if partitions <= 0 {
		partitions = 1
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.Topics[name]; ok {
		return errors.New("Topic already exists")
	}

	t := NewTopic(name)
	for p := 0; p < partitions; p++ {
		partDir := filepath.Join(b.DataDir, "topics", name, fmt.Sprintf("partition-%d", p))
		l, err := storage.NewLog(partDir, b.MaxSegmentBytes)
		if err != nil {
			return err
		}
		t.AddPartition(&Partition{
			ID:  p,
			Log: l,
		})
	}

	b.Topics[name] = t
	return nil

}

func (b *Broker) GetTopic(name string) (*Topic, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	t, ok := b.Topics[name]
	return t, ok
}

func (b *Broker) PartitionForKey(topic string, key string) (int, error) {
	t, ok := b.GetTopic(topic)
	if !ok {
		return -1, errors.New("Topic not found")
	}

	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % len(t.Partitions), nil
}
