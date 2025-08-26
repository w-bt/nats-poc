package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"nats-poc/internal/common"
	"nats-poc/internal/hash"
)

type Payload struct {
	IdempotencyKey string         `json:"idempotency_key"`
	PartitionKey   string         `json:"partition_key"`
	PID            int            `json:"pid"`
	Type           string         `json:"type"`
	ReceivedAt     string         `json:"received_at"`
	Data           map[string]any `json:"data"`
}

func main() {
	// Load .env file
	err := godotenv.Load()
	if err != nil {
		log.Println("No .env file found, using default values")
	}

	app := common.MustEnv("APP_NAME", "publisher")
	natsURL := common.MustEnv("NATS_URL", "nats://localhost:4222")
	partitions := common.MustEnvInt("PARTITIONS", 8)
	prefix := common.MustEnv("SUBJECT_PREFIX", "subject.events")
	intervalMs := common.MustEnvInt("PUBLISH_INTERVAL_MS", 200)
	streamName := common.MustEnv("STREAM_NAME", "stream_name") // Use existing stream

	// Connect using the new jetstream package
	nc, err := nats.Connect(natsURL, nats.Name(app))
	if err != nil {
		log.Fatal("Failed to connect to NATS:", err)
	}
	defer nc.Drain()

	// Create jetstream context using the new API
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal("Failed to create JetStream context:", err)
	}
	fmt.Println("JetStream context created successfully")

	// Ensure stream exists using the new API
	ctx := context.Background()
	_, err = js.Stream(ctx, streamName)
	if err != nil {
		// Stream doesn't exist, create it
		_, err = js.CreateStream(ctx, jetstream.StreamConfig{
			Name:       streamName,
			Subjects:   []string{fmt.Sprintf("%s.p.*", prefix)},
			Retention:  jetstream.LimitsPolicy,
			MaxAge:     24 * time.Hour * 7,
			Storage:    jetstream.MemoryStorage,
			Replicas:   1,
			Duplicates: 24 * time.Hour,
		})
		if err != nil {
			log.Fatal("Failed to create stream:", err)
		}
		fmt.Printf("Created stream: %s\n", streamName)
	} else {
		fmt.Printf("Using existing stream: %s\n", streamName)
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	fmt.Printf("%s started partitions=%d url=%s\n", app, partitions, natsURL)
	seq := 0
	for {
		seq++
		tenant := fmt.Sprintf("tenant-%d", r.Intn(5)+1)
		phone := fmt.Sprintf("phone-%d", r.Intn(3)+1)
		pkey := tenant + ":" + phone
		h := hash.HashString64(pkey)
		pid := int(h % uint64(partitions))
		subject := fmt.Sprintf("%s.p.%d", prefix, pid)

		idKey := fmt.Sprintf("%s:%d:%d", app, time.Now().UnixNano(), seq)
		pl := Payload{
			IdempotencyKey: idKey,
			PartitionKey:   pkey,
			PID:            pid,
			Type:           "type.message",
			ReceivedAt:     time.Now().UTC().Format(time.RFC3339),
			Data:           map[string]any{"tenant": tenant, "phone": phone, "seq": seq, "from": app},
		}
		b, err := json.Marshal(pl)
		if err != nil {
			fmt.Fprintf(os.Stderr, "marshal_failed err=%v\n", err)
			continue
		}

		// Use JetStream publish with message ID for deduplication and timeout
		pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		ack, err := js.Publish(pubCtx, subject, b, jetstream.WithMsgID(idKey))
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "publish_failed subject=%s pid=%d err=%v\n", subject, pid, err)
		} else {
			fmt.Printf("publish_ok subject=%s pid=%d id=%s partition_key=%s seq=%d\n", subject, pid, idKey, pkey, ack.Sequence)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(intervalMs) * time.Millisecond):
			// Continue publishing
		}
	}
}
