package lease


import (
	"encoding/json"
	"errors"
	"fmt"
	"time"


	"github.com/nats-io/nats.go"
)


type KVLeaseStore struct {
	KV nats.KeyValue
}


type leaseValue struct {
	WorkerID string `json:"worker_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

type LeaseInfo struct {
	Owned bool
	Revision uint64
	Value leaseValue
}


func key(pid int) string { return fmt.Sprintf("slot/%d", pid) }


func (s *KVLeaseStore) TryClaim(pid int, workerID string, ttl time.Duration) (LeaseInfo, error) {
	k := key(pid)
	now := time.Now().UTC()
	expires := now.Add(ttl)
	v := leaseValue{WorkerID: workerID, ExpiresAt: expires}
	b, _ := json.Marshal(v)


	// Try Create first (NX semantics)
	rev, err := s.KV.Create(k, b)
	if err == nil {
		return LeaseInfo{Owned: true, Revision: rev, Value: v}, nil
	}
	// If exists, load and check expiration
	entry, gerr := s.KV.Get(k)
	if gerr != nil {
		return LeaseInfo{}, gerr
	}
	var cur leaseValue
	_ = json.Unmarshal(entry.Value(), &cur)
	if now.Before(cur.ExpiresAt) && cur.WorkerID != workerID {
		// Not expired and owned by someone else
		return LeaseInfo{Owned: false, Revision: entry.Revision(), Value: cur}, nil
	}
	// Expired or we are the same owner: try optimistic update
	rev, uerr := s.KV.Update(k, b, entry.Revision())
	if uerr != nil {
		return LeaseInfo{}, uerr
	}
	return LeaseInfo{Owned: true, Revision: rev, Value: v}, nil
}


func (s *KVLeaseStore) Renew(pid int, workerID string, prevRev uint64, ttl time.Duration) (LeaseInfo, error) {
	k := key(pid)
	entry, err := s.KV.Get(k)
	if err != nil {
		return LeaseInfo{}, err
	}
	var cur leaseValue
	_ = json.Unmarshal(entry.Value(), &cur)
	if cur.WorkerID != workerID {
		return LeaseInfo{Owned: false, Revision: entry.Revision(), Value: cur}, errors.New("lost_ownership")
	}
	now := time.Now().UTC()
	v := leaseValue{WorkerID: workerID, ExpiresAt: now.Add(ttl)}
	b, _ := json.Marshal(v)
	rev, uerr := s.KV.Update(k, b, entry.Revision())
	if uerr != nil {
		return LeaseInfo{}, uerr
	}
	return LeaseInfo{Owned: true, Revision: rev, Value: v}, nil
}


func (s *KVLeaseStore) Release(pid int, workerID string) error {
	k := key(pid)
	entry, err := s.KV.Get(k)
	if err != nil {
		return err
	}
	var cur leaseValue
	_ = json.Unmarshal(entry.Value(), &cur)
	if cur.WorkerID != workerID {
		return errors.New("not_owner")
	}
	return s.KV.Delete(k)
}