package broker

type Topic struct {
	Name       string
	Partitions []*Partition
}

func NewTopic(name string) *Topic {
	return &Topic{
		Name:       name,
		Partitions: make([]*Partition, 0),
	}
}

func (t *Topic) AddPartition(partition *Partition) {
	t.Partitions = append(t.Partitions, partition)
}

func (t *Topic) GetPartition(partitionId int) *Partition {
	if partitionId < 0 || partitionId >= len(t.Partitions) {
		return nil
	}

	return t.Partitions[partitionId]
}
