package common


import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"


	"github.com/nats-io/nats.go"
)


func MustEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}


func MustEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}


func ConnectJS(appName string, url string) (*nats.Conn, nats.JetStreamContext) {
	nc, err := nats.Connect(url, nats.Name(appName))
	if err != nil {
		panic(err)
	}
	js, err := nc.JetStream()
	if err != nil {
		panic(err)
	}
	return nc, js
}


func EnsureStream(js nats.JetStreamContext, streamName, subjectPrefix string) {
	cfg := &nats.StreamConfig{
		Name: streamName,
		Subjects: []string{fmt.Sprintf("%s.p*", subjectPrefix)},
		Retention: nats.LimitsPolicy,
		MaxAge: 24 * time.Hour * 7,
		Storage: nats.MemoryStorage, // POC: in-memory. For prod, FileStorage.
		Replicas: 1,
		Duplicates: 24 * time.Hour,
	}
	if _, err := js.AddStream(cfg); err != nil {
		// If already exists, try update
		_, _ = js.UpdateStream(cfg)
	}
}


func EnsureKV(js nats.JetStreamContext, bucket string) nats.KeyValue {
	kv, err := js.KeyValue(bucket)
	if err == nil {
		return kv
	}
	kv, err = js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: bucket,
		// TTL 0 means no auto-expire at bucket level. We store expires_at in value.
	})
	if err != nil {
		panic(err)
	}
	return kv
}


// Helper to create or ensure durable consumer per partition
func EnsureDurable(js nats.JetStreamContext, stream, durable, filterSubject string) {
	cfg := &nats.ConsumerConfig{
		Durable: durable,
		FilterSubject: filterSubject,
		AckPolicy: nats.AckExplicitPolicy,
		AckWait: 2 * time.Minute,
		MaxAckPending: 512,
		ReplayPolicy: nats.ReplayInstantPolicy,
	}
	_, err := js.AddConsumer(stream, cfg)
	if err != nil {
		// If exists, try update to ensure settings
		_, _ = js.UpdateConsumer(stream, cfg)
	}
}


// Small utility to poll with backoff
func WaitUntil(ctx context.Context, interval time.Duration, fn func() bool) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if fn() { return }
		}
	}
}