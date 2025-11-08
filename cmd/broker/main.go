package main

import (
	"encoding/json"
	"fmt"
	"log"
	"my-kafka/internals/broker"
	"net/http"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// Records - the event messages
// Segments - the files in which event messages are stored
// Log - our continuous stream of data

type produceReq struct {
	Topic string `json:"topic"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

type produceResp struct {
	Partition int `json:"partition"`
	Offset    int `json:"offset"`
}

type consumeReq struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int    `json:"offset"`
	MaxMsgs   int    `json:"max_messages"`
}

type adminCreateReq struct {
	Topic      string `json:"topic"`
	Partitions int    `json:"partitions"`
}

func main() {
	godotenv.Load()
	port := os.Getenv("PORT")
	if port == "" {
		port = "9092"
	}

	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = "data"
	}

	maxSegBytes := int64(50 * 1024 * 1024)

	br := broker.NewBroker(dataDir, maxSegBytes)
	log.Println(os.Getenv("TOPIC_BOOTSTRAP"))
	if tp := os.Getenv("TOPIC_BOOTSTRAP"); tp != "" {
		pr, _ := strconv.Atoi(os.Getenv("BOOT_PARTITIONS"))
		if pr == 0 {
			pr = 3
		}

		if err := br.CreateTopic(tp, pr, 3); err != nil {
			log.Printf("bootstrap create topic err : %v", err)
		} else {
			log.Printf("bootstrapped create topic success")
			log.Printf("Topic = %s", tp)
			log.Printf("Partitions = %d", pr)
		}
	}

	http.HandleFunc("/admin/create", func(w http.ResponseWriter, r *http.Request) {
		var req adminCreateReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if req.Partitions <= 0 {
			req.Partitions = 3
		}

		if err := br.CreateTopic(req.Topic, req.Partitions, 3); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		w.WriteHeader(200)
		w.Write([]byte("created"))
	})

	http.HandleFunc("/produce", func(w http.ResponseWriter, r *http.Request) {
		var req produceReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}

		part, err := br.PartitionForKey(req.Topic, req.Key)
		if err != nil {
			http.Error(w, err.Error(), 404)
			return
		}

		tp, _ := br.GetTopic(req.Topic)
		pr := tp.GetPartition(part)

		offset, err := pr.Log.Append([]byte(req.Key), []byte(req.Value))
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		json.NewEncoder(w).Encode(produceResp{
			Partition: part,
			Offset:    int(offset),
		})

	})

	http.HandleFunc("/consume", func(w http.ResponseWriter, r *http.Request) {
		var req consumeReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		t, ok := br.GetTopic(req.Topic)
		if !ok {
			http.Error(w, "unknown topic", 404)
			return
		}
		p := t.GetPartition(req.Partition)
		if p == nil {
			http.Error(w, "unknown partition", 404)
			return
		}
		records, err := p.Log.Read(uint64(req.Offset), req.MaxMsgs)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"messages": records})
	})

	addr := fmt.Sprintf(":%s", port)
	log.Printf("starting broker on %s , data = %s", addr, dataDir)
	log.Fatal(http.ListenAndServe(addr, nil))

}
