package collector

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/valhalla/mrt-middleware-monitoring/internal/config"
	"github.com/valhalla/mrt-middleware-monitoring/internal/model"
	"github.com/valhalla/mrt-middleware-monitoring/internal/queue"
)

type fakeSink struct {
	fail      bool
	committed map[string]bool
}

type countingReader struct{ calls atomic.Int32 }

func (r *countingReader) Read(context.Context, byte, uint16, uint16) ([]byte, error) {
	r.calls.Add(1)
	return nil, errors.New("offline meter")
}

func TestPollingPausesWhenQueueFullAndResumesAfterDrain(t *testing.T) {
	ctx := context.Background()
	q, e := queue.Open(filepath.Join(t.TempDir(), "full.db"), 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	for i := 0; i < 2000; i++ {
		at := time.Now().UTC()
		m := model.Measurement{ID: uuid.NewString(), MeterID: "m1", ObservedAt: at, CompletedAt: at, Quality: "failed"}
		e = q.Enqueue(ctx, m)
		if errors.Is(e, queue.ErrFull) {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
	}
	if !errors.Is(q.Ready(ctx), queue.ErrFull) {
		t.Fatal("queue was not filled")
	}
	r := &countingReader{}
	run, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		pollConnection(run, r, []config.Meter{{ID: "m1", UnitID: 1, Interval: time.Hour}}, q)
		close(done)
	}()
	defer func() { cancel(); <-done }()
	time.Sleep(100 * time.Millisecond)
	if r.calls.Load() != 0 {
		t.Fatal("meter polled despite full buffer")
	}
	for {
		items, e := q.Peek(ctx, 100)
		if e != nil {
			t.Fatal(e)
		}
		if len(items) == 0 {
			break
		}
		if e = q.Ack(ctx, items); e != nil {
			t.Fatal(e)
		}
	}
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if r.calls.Load() > 0 {
			n, e := q.Count(ctx)
			if e == nil && n > 0 {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("polling failed to resume after buffer recovery")
}

func (s *fakeSink) Write(_ context.Context, _ []config.Meter, ms []model.Measurement) error {
	for _, m := range ms {
		s.committed[m.ID] = true
	}
	if s.fail {
		return errors.New("commit result unknown")
	}
	return nil
}
func TestOutboxUnknownCommitRestartAndReplay(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "q.db")
	q, e := queue.Open(path, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	m := model.Measurement{ID: uuid.NewString(), MeterID: "m1", ObservedAt: time.Now(), CompletedAt: time.Now(), Quality: "failed"}
	if e = q.Enqueue(ctx, m); e != nil {
		t.Fatal(e)
	}
	s := &fakeSink{true, map[string]bool{}}
	if _, e = DrainOnce(ctx, q, s, nil); e == nil {
		t.Fatal("expected uncertain commit")
	}
	if n, _ := q.Count(ctx); n != 1 {
		t.Fatal("acknowledged unconfirmed write")
	}
	q.Close()
	q, e = queue.Open(path, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	s.fail = false
	if n, e := DrainOnce(ctx, q, s, nil); e != nil || n != 1 {
		t.Fatalf("recovery: %d %v", n, e)
	}
	if len(s.committed) != 1 {
		t.Fatal("duplicate identity")
	}
	if n, _ := q.Count(ctx); n != 0 {
		t.Fatal("not acknowledged")
	}
}
