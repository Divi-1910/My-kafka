package main

import (
	"fmt"
	"my-kafka/internals/storage"
)

// Records - the event messages
// Segments - the files in which event messages are stored
// Log - our continuous stream of data

func main() {
	log, _ := storage.NewLog("data/orders-0", 1024*1024)
	defer log.Active.File.Close()

	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("order-%d", i))
		val := []byte(fmt.Sprintf("Divi-%d bought protein powder", i))

		offset, _ := log.Append(key, val)
		fmt.Printf("Appended offset %d \n", offset)
	}

	recs, _ := log.Read(0, 10)
	for _, rec := range recs {
		fmt.Printf("[%d] %s = %s \n", rec.Offset, rec.Key, rec.Value)
	}

}
