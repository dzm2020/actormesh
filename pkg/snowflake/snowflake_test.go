package snowflake

import (
	"sync"
	"testing"
	"time"
)

func TestSnowFlakeGeneratesAndParsesID(t *testing.T) {
	start := time.Now().Add(-time.Second)
	s, err := New(Config{
		TimestampBits: 41,
		MachineIDBits: 12,
		SequenceBits:  10,
		MachineID:     7,
		StartTime:     start,
	})
	if err != nil {
		t.Fatalf("new snowflake: %v", err)
	}

	id := s.GenId()
	if id <= 0 {
		t.Fatalf("generated id must be positive: %d", id)
	}
	if got := s.ParseIdToMachineId(id); got != 7 {
		t.Fatalf("machine id = %d, want 7", got)
	}
	if got := s.ParseIdToSequence(id); got < 0 || int64(got) > s.maxSequence {
		t.Fatalf("sequence out of range: %d", got)
	}
	if got := s.ParseIdToTimestamp(id); got < start.UnixMilli() {
		t.Fatalf("timestamp = %d, before start %d", got, start.UnixMilli())
	}
}

func TestSnowFlakeInstancesAreIndependent(t *testing.T) {
	start := time.Now().Add(-time.Second)
	first, err := New(Config{TimestampBits: 41, MachineIDBits: 12, SequenceBits: 10, MachineID: 1, StartTime: start})
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(Config{TimestampBits: 41, MachineIDBits: 12, SequenceBits: 10, MachineID: 2, StartTime: start})
	if err != nil {
		t.Fatal(err)
	}

	if got := first.ParseIdToMachineId(first.GenId()); got != 1 {
		t.Fatalf("first machine id = %d, want 1", got)
	}
	if got := second.ParseIdToMachineId(second.GenId()); got != 2 {
		t.Fatalf("second machine id = %d, want 2", got)
	}
}

func TestSnowFlakeConcurrentIDsAreUnique(t *testing.T) {
	s := NewDefault()
	const workers = 8
	const perWorker = 200
	ids := make(chan int64, workers*perWorker)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perWorker {
				ids <- s.GenId()
			}
		}()
	}
	wg.Wait()
	close(ids)

	seen := make(map[int64]struct{}, workers*perWorker)
	for id := range ids {
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate id: %d", id)
		}
		seen[id] = struct{}{}
	}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	if _, err := New(Config{TimestampBits: 1, MachineIDBits: 1, SequenceBits: 1, StartTime: time.Now()}); err == nil {
		t.Fatal("expected invalid bit length error")
	}
	if _, err := New(Config{TimestampBits: 41, MachineIDBits: 12, SequenceBits: 10, StartTime: time.Now().Add(time.Second)}); err == nil {
		t.Fatal("expected future start time error")
	}
}
