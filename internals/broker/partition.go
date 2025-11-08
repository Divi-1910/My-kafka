package broker

import "my-kafka/internals/storage"

type Partition struct {
	ID  int
	Log *storage.Log
}
