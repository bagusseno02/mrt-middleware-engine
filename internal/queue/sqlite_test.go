package queue

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/valhalla/mrt-middleware-monitoring/internal/model"
)

func sample() model.Measurement {
	return model.Measurement{ID: uuid.NewString(), MeterID: "m1", ObservedAt: time.Now().UTC(), CompletedAt: time.Now().UTC(), Quality: "failed", Errors: map[string]string{"connection": "offline"}}
}
func TestDurabilityReplayAndCapacityRecovery(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "queue.db")
	q, e := Open(path, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	m := sample()
	if e = q.Enqueue(ctx, m); e != nil {
		t.Fatal(e)
	}
	if e = q.Enqueue(ctx, m); e != nil {
		t.Fatal(e)
	}
	q.Close()
	q, e = Open(path, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	items, e := q.Peek(ctx, 100)
	if e != nil || len(items) != 1 || items[0].Measurement.ID != m.ID {
		t.Fatalf("restart: %v %+v", e, items)
	}
	full := false
	for i := 0; i < 2000; i++ {
		e = q.Enqueue(ctx, sample())
		if errors.Is(e, ErrFull) {
			full = true
			break
		}
		if e != nil {
			t.Fatal(e)
		}
	}
	if !full || !errors.Is(q.Ready(ctx), ErrFull) {
		t.Fatal("capacity did not pause writes")
	}
	if e = q.Enqueue(ctx, m); e != nil {
		t.Fatalf("retry of already persisted record must succeed even when full: %v", e)
	}
	count, _ := q.Count(ctx)
	if count < 1 {
		t.Fatal("lost queued records")
	}
	for {
		items, e = q.Peek(ctx, 100)
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
	if e = q.Ready(ctx); e != nil {
		t.Fatalf("queue did not resume: %v", e)
	}
	if e = q.Enqueue(ctx, sample()); e != nil {
		t.Fatal(e)
	}
}

func TestReadOnlyDiskFailureAndRecovery(t *testing.T) {
	q, e := Open(filepath.Join(t.TempDir(), "q.db"), 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	ctx := context.Background()
	if e = q.Enqueue(ctx, sample()); e != nil {
		t.Fatal(e)
	}
	if _, e = q.db.Exec("PRAGMA query_only=ON"); e != nil {
		t.Fatal(e)
	}
	if q.Ready(ctx) == nil || q.Enqueue(ctx, sample()) == nil {
		t.Fatal("write failure did not block acceptance")
	}
	if n, _ := q.Count(ctx); n != 1 {
		t.Fatal("disk failure changed existing data")
	}
	if _, e = q.db.Exec("PRAGMA query_only=OFF"); e != nil {
		t.Fatal(e)
	}
	if e = q.Enqueue(ctx, sample()); e != nil {
		t.Fatal(e)
	}
	if e = q.Ready(ctx); e != nil {
		t.Fatal(e)
	}
}

func TestFailedInsertKeepsReadinessFalseUntilPendingWriteRecovers(t *testing.T) {
	q, e := Open(filepath.Join(t.TempDir(), "q.db"), 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	ctx := context.Background()
	if _, e = q.db.Exec(`CREATE TRIGGER reject_write BEFORE INSERT ON outbox BEGIN SELECT RAISE(FAIL,'write unavailable'); END`); e != nil {
		t.Fatal(e)
	}
	m := sample()
	if q.Enqueue(ctx, m) == nil {
		t.Fatal("injected write failure missing")
	}
	if q.Ready(ctx) == nil {
		t.Fatal("small health update concealed failed data writes")
	}
	if _, e = q.db.Exec("DROP TRIGGER reject_write"); e != nil {
		t.Fatal(e)
	}
	if e = q.Enqueue(ctx, m); e != nil {
		t.Fatal(e)
	}
	if e = q.Ready(ctx); e != nil {
		t.Fatal(e)
	}
}
func TestClosedQueueNotReady(t *testing.T) {
	q, e := Open(filepath.Join(t.TempDir(), "q.db"), 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	q.Close()
	if q.Ready(context.Background()) == nil {
		t.Fatal("closed queue ready")
	}
}
