package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"nats-poc/internal/common"
	"nats-poc/internal/lease"
)

func main() {
	// Load .env file
	err := godotenv.Load()
	if err != nil {
		log.Println("No .env file found, using default values")
	}

	baseApp := common.MustEnv("APP_NAME", "consumer")
	// Create unique worker ID using process ID
	app := baseApp + "-" + strconv.Itoa(os.Getpid())
	natsURL := common.MustEnv("NATS_URL", "nats://localhost:4222")
	partitions := common.MustEnvInt("PARTITIONS", 8)
	prefix := common.MustEnv("SUBJECT_PREFIX", "subject.events")
	streamName := common.MustEnv("STREAM_NAME", "stream_name")
	leaseBucket := common.MustEnv("LEASE_BUCKET", "leases")
	leaseTTL := time.Duration(common.MustEnvInt("LEASE_TTL_SEC", 15)) * time.Second
	hb := time.Duration(common.MustEnvInt("HEARTBEAT_SEC", 5)) * time.Second
	batch := common.MustEnvInt("FETCH_BATCH", 10)

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

	// Ensure stream exists using the new API
	ensureStream(js, streamName, prefix)

	// Ensure KV bucket exists
	kv, err := js.KeyValue(context.Background(), leaseBucket)
	if err != nil {
		// Create KV bucket if it doesn't exist
		kv, err = js.CreateKeyValue(context.Background(), jetstream.KeyValueConfig{
			Bucket: leaseBucket,
		})
		if err != nil {
			log.Fatal("Failed to create KV bucket:", err)
		}
	}

	ls := &lease.JetStreamKVLeaseStore{KV: kv}

	// Register this worker
	err = ls.RegisterWorker(app, leaseTTL)
	if err != nil {
		log.Fatal("Failed to register worker:", err)
	}

	fmt.Printf("%s started partitions=%d url=%s ttl=%s hb=%s\n", app, partitions, natsURL, leaseTTL, hb)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	claimed := make(map[int]context.CancelFunc)
	var claimedMutex sync.RWMutex
	lastRebalanceCheck := time.Now()
	rebalanceInterval := 2 * time.Second
	lastAggressiveClaimCheck := time.Now()
	aggressiveClaimInterval := 3 * time.Second
	lastCleanupCheck := time.Now()
	cleanupInterval := 30 * time.Second

	for {
		select {
		case <-ctx.Done():
			fmt.Println("shutting down gracefully...")
			claimedMutex.Lock()
			for pid, cancel := range claimed {
				fmt.Println("stop pid", pid)
				// Release the lease before stopping
				if err := ls.Release(pid, app); err != nil {
					fmt.Printf("failed to release lease for pid %d: %v\n", pid, err)
				} else {
					fmt.Printf("released lease for pid %d\n", pid)
				}
				cancel()
			}
			claimedMutex.Unlock()
			
			// Also remove the worker from the KV store
			workerKey := fmt.Sprintf("worker/%s", app)
			if err := kv.Delete(ctx, workerKey); err != nil {
				fmt.Printf("failed to remove worker from KV store: %v\n", err)
			} else {
				fmt.Printf("removed worker %s from KV store\n", app)
			}
			
			// Small delay to ensure all cleanup operations complete
			time.Sleep(100 * time.Millisecond)
			fmt.Println("graceful shutdown completed")
			return
		case <-time.After(1 * time.Second):
		}

		// Update worker heartbeat with current partitions
		claimedMutex.RLock()
		ownedPids := make([]int, 0, len(claimed))
		for pid := range claimed {
			ownedPids = append(ownedPids, pid)
		}
		claimedMutex.RUnlock()
		err = ls.UpdateWorkerHeartbeat(app, ownedPids)
		if err != nil {
			return
		}

		// Check if we should rebalance
		if time.Since(lastRebalanceCheck) >= rebalanceInterval {
			rebalanceInfo, err := ls.CheckRebalance(app, partitions, leaseTTL)
			if err != nil {
				fmt.Println("rebalance_check_error", err)
			} else {
				// Release partitions not assigned to us
				releasedAny := false
				claimedMutex.Lock()
				for pid, cancel := range claimed {
					shouldOwn := false
					for _, myPid := range rebalanceInfo.MyPartitions {
						if myPid == pid {
							shouldOwn = true
							break
						}
					}
					if !shouldOwn {
						fmt.Println("releasing pid", pid, "for rebalance")
						cancel()
						delete(claimed, pid)
						ls.Release(pid, app)
						releasedAny = true
					}
				}
				claimedMutex.Unlock()

				// If we released any partitions, add a small delay to allow other workers to claim them
				if releasedAny {
					time.Sleep(200 * time.Millisecond)
				}

				// First, try to claim partitions assigned to us
				for _, pid := range rebalanceInfo.MyPartitions {
					// Check if we already have a consumer for this partition
					claimedMutex.RLock()
					_, alreadyClaimed := claimed[pid]
					claimedMutex.RUnlock()
					if alreadyClaimed {
						continue // already claimed
					}

					info, err := ls.TryClaim(pid, app, leaseTTL)
					if err != nil {
						fmt.Println("claim_error pid", pid, err)
						continue
					}
					if !info.Owned {
						continue
					}

					// Double-check we don't already have a consumer for this partition
					claimedMutex.Lock()
					_, alreadyExists := claimed[pid]
					if alreadyExists {
						claimedMutex.Unlock()
						// We already have a consumer for this partition, release the lease we just claimed
						ls.Release(pid, app)
						continue
					}

					// start consumer for this partition
					subject := fmt.Sprintf("%s.p.%d", prefix, pid)
					durable := fmt.Sprintf("core-meta-p%d", pid)

					// Ensure durable consumer using the new API
					ensureDurable(js, streamName, durable, subject)

					cctx, cancel := context.WithCancel(ctx)
					claimed[pid] = cancel
					claimedMutex.Unlock()
					// Wrap the cleanup function to ensure it's always called
					go func(pid int, cctx context.Context, cancel context.CancelFunc) {
						// Ensure cleanup is called even if consumePartitionSubject panics
						defer func() {
							claimedMutex.Lock()
							delete(claimed, pid)
							claimedMutex.Unlock()
						}()
						consumePartitionSubject(cctx, js, ls, pid, subject, durable, app, leaseTTL, hb, batch, streamName, nil)
					}(pid, cctx, cancel)
				}
			}
			lastRebalanceCheck = time.Now()
		}

		// Separate aggressive claiming for expired partitions (less frequent)
		if time.Since(lastAggressiveClaimCheck) >= aggressiveClaimInterval {
			// Aggressively try to claim any available partitions (fast failover)
			for pid := 0; pid < partitions; pid++ {
				claimedMutex.RLock()
				_, alreadyClaimed := claimed[pid]
				claimedMutex.RUnlock()
				if alreadyClaimed {
					continue // already claimed by us
				}

				// First check if we already have a consumer for this partition
				claimedMutex.RLock()
				_, alreadyClaimed = claimed[pid]
				claimedMutex.RUnlock()
				if alreadyClaimed {
					continue // already claimed by us
				}

				// Try to claim this partition (will only succeed if expired/available)
				info, err := ls.TryClaim(pid, app, leaseTTL)
				if err != nil {
					continue // likely claimed by someone else or other error
				}
				if !info.Owned {
					continue // not available
				}

				fmt.Printf("claimed expired partition %d (previous owner: %s)\n", pid, info.Value.WorkerID)

				// Double-check we don't already have a consumer for this partition
				claimedMutex.Lock()
				_, alreadyExists := claimed[pid]
				if alreadyExists {
					claimedMutex.Unlock()
					// We already have a consumer for this partition, release the lease we just claimed
					ls.Release(pid, app)
					continue
				}

				// start consumer for this partition
				subject := fmt.Sprintf("%s.p%d", prefix, pid)
				durable := fmt.Sprintf("core-meta-p%d", pid)

				// Ensure durable consumer using the new API
				ensureDurable(js, streamName, durable, subject)

				cctx, cancel := context.WithCancel(ctx)
				claimed[pid] = cancel
				claimedMutex.Unlock()
				// Wrap the cleanup function to ensure it's always called
				go func(pid int, cctx context.Context, cancel context.CancelFunc) {
					// Ensure cleanup is called even if consumePartitionSubject panics
					defer func() {
						claimedMutex.Lock()
						delete(claimed, pid)
						claimedMutex.Unlock()
					}()
					consumePartitionSubject(cctx, js, ls, pid, subject, durable, app, leaseTTL, hb, batch, streamName, nil)
				}(pid, cctx, cancel)
			}
			lastAggressiveClaimCheck = time.Now()
		}

		// Periodic cleanup to prevent memory leaks
		if time.Since(lastCleanupCheck) >= cleanupInterval {
			performCleanup(claimed, &claimedMutex, ls, app, partitions, leaseTTL)
			lastCleanupCheck = time.Now()
		}
	}
}

func ensureStream(js jetstream.JetStream, streamName, subjectPrefix string) {
	ctx := context.Background()

	// Try to get the stream first
	_, err := js.Stream(ctx, streamName)
	if err != nil {
		// Stream doesn't exist, create it
		_, err = js.CreateStream(ctx, jetstream.StreamConfig{
			Name:       streamName,
			Subjects:   []string{fmt.Sprintf("%s.p*", subjectPrefix)},
			Retention:  jetstream.LimitsPolicy,
			MaxAge:     24 * time.Hour * 7,
			Storage:    jetstream.MemoryStorage,
			Replicas:   1,
			Duplicates: 24 * time.Hour,
		})
		if err != nil {
			log.Fatal("Failed to create stream:", err)
		}
	}
}

func ensureDurable(js jetstream.JetStream, streamName, durable, filterSubject string) {
	ctx := context.Background()

	// Try to get the consumer first
	_, err := js.Consumer(ctx, streamName, durable)
	if err != nil {
		// Consumer doesn't exist, create it
		_, err = js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
			Durable:       durable,
			FilterSubject: filterSubject,
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       2 * time.Minute,
			MaxAckPending: 512,
			ReplayPolicy:  jetstream.ReplayInstantPolicy,
		})
		if err != nil {
			log.Fatal("Failed to create consumer:", err)
		}
	}
}

func consumePartitionSubject(ctx context.Context, js jetstream.JetStream, ls *lease.JetStreamKVLeaseStore,
	pid int, subject, durable, app string, leaseTTL, hb time.Duration, batch int, streamName string, onStop func()) {
	// Always clean up claimed map entry when this function exits
	defer func() {
		if onStop != nil {
			onStop()
		}
	}()

	// Get the consumer using the new API
	consumer, err := js.Consumer(ctx, streamName, durable)
	if err != nil {
		fmt.Println("consumer_error pid", pid, err)
		return
	}

	fmt.Println(app, "own pid", pid, "subject", subject)

	// Start lease renewal in separate goroutine to prevent blocking
	renewCtx, renewCancel := context.WithCancel(ctx)
	defer renewCancel()

	go func() {
		ticker := time.NewTicker(hb)
		defer ticker.Stop()

		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				_, err := ls.Renew(pid, app, 0, leaseTTL)
				if err != nil {
					fmt.Println("lease_renew_failed pid", pid, err)
					renewCancel() // Signal main loop to stop
					return
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-renewCtx.Done():
			return // Lease renewal failed, stop consuming
		default:
		}

		// Fetch messages using the new API
		msgs, err := consumer.Fetch(batch, jetstream.FetchMaxWait(2*time.Second))
		if err != nil {
			if err == context.DeadlineExceeded {
				continue
			}
			fmt.Println("fetch_error pid", pid, err)
			continue
		}

		// Iterate through the messages using the correct API
		for msg := range msgs.Messages() {
			if msg == nil {
				continue
			}

			id := ""
			if headers := msg.Headers(); headers != nil {
				if idVals := headers.Values("Nats-Msg-Id"); len(idVals) > 0 {
					id = idVals[0]
				}
			}

			// Process the message
			metadata, metaErr := msg.Metadata()
			if metaErr != nil {
				fmt.Println("metadata_error pid", pid, metaErr)
				continue
			}
			fmt.Println("consume pid", pid, "stream_seq", metadata.Sequence.Stream, "id", id)

			// Acknowledge the message
			err = msg.Ack()
			if err != nil {
				fmt.Println("ack_error pid", pid, err)
			}
		}

		// Check for errors after the loop
		if msgs.Error() != nil {
			fmt.Println("msgs_error pid", pid, msgs.Error())
		}
	}
}

// performCleanup checks for any leaked consumers and cleans them up
func performCleanup(claimed map[int]context.CancelFunc, claimedMutex *sync.RWMutex, ls *lease.JetStreamKVLeaseStore, app string, totalPartitions int, leaseTTL time.Duration) {
	claimedMutex.Lock()
	defer claimedMutex.Unlock()
	
	fmt.Printf("Performing periodic cleanup - currently managing %d partitions\n", len(claimed))
	printMemStats()
	
	// Get active workers
	activeWorkers, err := ls.GetActiveWorkers(leaseTTL)
	if err != nil {
		fmt.Printf("cleanup: failed to get active workers: %v\n", err)
		return
	}
	
	// Get our fair share of partitions
	var allWorkers []string
	for _, w := range activeWorkers {
		allWorkers = append(allWorkers, w.WorkerID)
	}
	
	fmt.Printf("cleanup: active workers: %v\n", allWorkers)
	
	// Calculate what partitions we should own
	expectedPartitions := ls.CalculateFairPartitions(app, allWorkers, totalPartitions)
	expectedMap := make(map[int]bool)
	for _, pid := range expectedPartitions {
		expectedMap[pid] = true
	}
	
	fmt.Printf("cleanup: expected partitions: %v\n", expectedPartitions)
	
	// This is a simplified check - in a real implementation, you might want to do more thorough validation
	// For now, we're just logging the information
}

// printMemStats prints current memory statistics
func printMemStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	
	// Convert to MB for easier reading
	alloc := bToMb(m.Alloc)
	totalAlloc := bToMb(m.TotalAlloc)
	sys := bToMb(m.Sys)
	numGC := m.NumGC
	
	fmt.Printf("Memory Stats - Alloc: %d MB, TotalAlloc: %d MB, Sys: %d MB, NumGC: %d\n", 
		alloc, totalAlloc, sys, numGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}