package lease

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type JetStreamKVLeaseStore struct {
	KV jetstream.KeyValue
}

type jetStreamLeaseValue struct {
	WorkerID  string    `json:"worker_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

type JetStreamLeaseInfo struct {
	Owned    bool
	Revision uint64
	Value    jetStreamLeaseValue
}

func jetStreamKey(pid int) string { return fmt.Sprintf("slot/%d", pid) }
func workerKey(workerID string) string { return fmt.Sprintf("worker/%s", workerID) }

type WorkerInfo struct {
	WorkerID    string    `json:"worker_id"`
	LastSeen    time.Time `json:"last_seen"`
	Partitions  []int     `json:"partitions"`
}

type RebalanceInfo struct {
	ShouldRebalance bool
	MyPartitions    []int
	AllWorkers      []string
}

func (s *JetStreamKVLeaseStore) TryClaim(pid int, workerID string, ttl time.Duration) (JetStreamLeaseInfo, error) {
	k := jetStreamKey(pid)
	now := time.Now().UTC()
	expires := now.Add(ttl)
	v := jetStreamLeaseValue{WorkerID: workerID, ExpiresAt: expires}
	b, _ := json.Marshal(v)

	ctx := context.Background()

	// Try Create first (NX semantics)
	rev, err := s.KV.Create(ctx, k, b)
	if err == nil {
		return JetStreamLeaseInfo{Owned: true, Revision: rev, Value: v}, nil
	}
	
	// If exists, load and check expiration
	entry, gerr := s.KV.Get(ctx, k)
	if gerr != nil {
		return JetStreamLeaseInfo{}, gerr
	}
	
	var cur jetStreamLeaseValue
	_ = json.Unmarshal(entry.Value(), &cur)
	if now.Before(cur.ExpiresAt) && cur.WorkerID != workerID {
		// Not expired and owned by someone else
		return JetStreamLeaseInfo{Owned: false, Revision: entry.Revision(), Value: cur}, nil
	}
	
	// Grace period: don't allow same worker to immediately reclaim recently expired lease
	gracePeriod := 1 * time.Second
	if cur.WorkerID == workerID && now.After(cur.ExpiresAt) && now.Before(cur.ExpiresAt.Add(gracePeriod)) {
		// Same worker trying to reclaim within grace period
		return JetStreamLeaseInfo{Owned: false, Revision: entry.Revision(), Value: cur}, nil
	}
	
	// Expired or we are the same owner: try optimistic update
	rev, uerr := s.KV.Update(ctx, k, b, entry.Revision())
	if uerr != nil {
		return JetStreamLeaseInfo{}, uerr
	}
	
	return JetStreamLeaseInfo{Owned: true, Revision: rev, Value: v}, nil
}

func (s *JetStreamKVLeaseStore) Renew(pid int, workerID string, prevRev uint64, ttl time.Duration) (JetStreamLeaseInfo, error) {
	k := jetStreamKey(pid)
	
	ctx := context.Background()
	
	entry, err := s.KV.Get(ctx, k)
	if err != nil {
		return JetStreamLeaseInfo{}, err
	}
	
	var cur jetStreamLeaseValue
	_ = json.Unmarshal(entry.Value(), &cur)
	if cur.WorkerID != workerID {
		return JetStreamLeaseInfo{Owned: false, Revision: entry.Revision(), Value: cur}, errors.New("lost_ownership")
	}
	
	now := time.Now().UTC()
	if now.After(cur.ExpiresAt) {
		return JetStreamLeaseInfo{Owned: false, Revision: entry.Revision(), Value: cur}, errors.New("lease_expired")
	}
	
	v := jetStreamLeaseValue{WorkerID: workerID, ExpiresAt: now.Add(ttl)}
	b, _ := json.Marshal(v)
	rev, uerr := s.KV.Update(ctx, k, b, entry.Revision())
	if uerr != nil {
		return JetStreamLeaseInfo{}, uerr
	}
	
	return JetStreamLeaseInfo{Owned: true, Revision: rev, Value: v}, nil
}

func (s *JetStreamKVLeaseStore) Release(pid int, workerID string) error {
	k := jetStreamKey(pid)
	
	ctx := context.Background()
	
	entry, err := s.KV.Get(ctx, k)
	if err != nil {
		return err
	}
	
	var cur jetStreamLeaseValue
	_ = json.Unmarshal(entry.Value(), &cur)
	if cur.WorkerID != workerID {
		return errors.New("not_owner")
	}
	
	return s.KV.Delete(ctx, k)
}

func (s *JetStreamKVLeaseStore) RegisterWorker(workerID string, ttl time.Duration) error {
	k := workerKey(workerID)
	ctx := context.Background()
	
	worker := WorkerInfo{
		WorkerID: workerID,
		LastSeen: time.Now().UTC(),
		Partitions: []int{},
	}
	
	b, _ := json.Marshal(worker)
	_, err := s.KV.Put(ctx, k, b)
	return err
}

func (s *JetStreamKVLeaseStore) UpdateWorkerHeartbeat(workerID string, ownedPartitions []int) error {
	k := workerKey(workerID)
	ctx := context.Background()
	
	worker := WorkerInfo{
		WorkerID: workerID,
		LastSeen: time.Now().UTC(),
		Partitions: ownedPartitions,
	}
	
	b, _ := json.Marshal(worker)
	_, err := s.KV.Put(ctx, k, b)
	return err
}

func (s *JetStreamKVLeaseStore) GetActiveWorkers(ttl time.Duration) ([]WorkerInfo, error) {
	ctx := context.Background()
	
	keys, err := s.KV.Keys(ctx)
	if err != nil {
		return nil, err
	}
	
	var workers []WorkerInfo
	now := time.Now().UTC()
	
	for _, key := range keys {
		if !strings.HasPrefix(key, "worker/") {
			continue
		}
		
		entry, err := s.KV.Get(ctx, key)
		if err != nil {
			continue
		}
		
		var worker WorkerInfo
		if err := json.Unmarshal(entry.Value(), &worker); err != nil {
			continue
		}
		
		if now.Sub(worker.LastSeen) <= ttl*2 {
			workers = append(workers, worker)
		}
	}
	
	sort.Slice(workers, func(i, j int) bool {
		return workers[i].WorkerID < workers[j].WorkerID
	})
	
	return workers, nil
}

func (s *JetStreamKVLeaseStore) CheckRebalance(workerID string, totalPartitions int, ttl time.Duration) (RebalanceInfo, error) {
	workers, err := s.GetActiveWorkers(ttl)
	if err != nil {
		return RebalanceInfo{}, err
	}
	
	if len(workers) == 0 {
		return RebalanceInfo{}, errors.New("no active workers found")
	}
	
	allWorkers := make([]string, len(workers))
	for i, w := range workers {
		allWorkers[i] = w.WorkerID
	}
	
	myPartitions := s.calculateFairPartitions(workerID, allWorkers, totalPartitions)
	
	return RebalanceInfo{
		ShouldRebalance: true,
		MyPartitions:    myPartitions,
		AllWorkers:      allWorkers,
	}, nil
}

func (s *JetStreamKVLeaseStore) calculateFairPartitions(workerID string, allWorkers []string, totalPartitions int) []int {
	workerCount := len(allWorkers)
	partitionsPerWorker := totalPartitions / workerCount
	remainder := totalPartitions % workerCount
	
	workerIndex := -1
	for i, w := range allWorkers {
		if w == workerID {
			workerIndex = i
			break
		}
	}
	
	if workerIndex == -1 {
		return []int{}
	}
	
	start := workerIndex * partitionsPerWorker
	if workerIndex < remainder {
		start += workerIndex
	} else {
		start += remainder
	}
	
	count := partitionsPerWorker
	if workerIndex < remainder {
		count++
	}
	
	var partitions []int
	for i := 0; i < count; i++ {
		partitions = append(partitions, start+i)
	}
	
	return partitions
}

// CalculateFairPartitions calculates the fair share of partitions for a worker
func (s *JetStreamKVLeaseStore) CalculateFairPartitions(workerID string, allWorkers []string, totalPartitions int) []int {
	return s.calculateFairPartitions(workerID, allWorkers, totalPartitions)
}