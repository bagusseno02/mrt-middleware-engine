package collector

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/valhalla/mrt-middleware-monitoring/internal/config"
	"github.com/valhalla/mrt-middleware-monitoring/internal/meter"
	"github.com/valhalla/mrt-middleware-monitoring/internal/model"
	"github.com/valhalla/mrt-middleware-monitoring/internal/queue"
	"github.com/valhalla/mrt-middleware-monitoring/internal/transport"
)

type Sink interface {
	Write(context.Context, []config.Meter, []model.Measurement) error
}

// DrainOnce acknowledges only after the remote transaction commits. A crash
// between commit and acknowledgement replays the same UUIDs safely.
func DrainOnce(ctx context.Context, q *queue.Queue, s Sink, meters []config.Meter) (int, error) {
	items, e := q.Peek(ctx, 100)
	if e != nil {
		return 0, e
	}
	values := make([]model.Measurement, len(items))
	for i, x := range items {
		values[i] = x.Measurement
	}
	if e = s.Write(ctx, meters, values); e != nil {
		return 0, e
	}
	if e = q.Ack(ctx, items); e != nil {
		return 0, e
	}
	return len(items), nil
}
func wait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
func Run(ctx context.Context, c config.Config, q *queue.Queue, s Sink) {
	var wg sync.WaitGroup
	for _, conn := range c.Connections {
		meters := []config.Meter{}
		for _, m := range c.Meters {
			if m.Connection == conn.ID {
				meters = append(meters, m)
			}
		}
		if len(meters) == 0 {
			continue
		}
		wg.Add(1)
		go func() { defer wg.Done(); r := transport.New(conn); defer r.Close(); pollConnection(ctx, r, meters, q) }()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		delay := time.Second
		for ctx.Err() == nil {
			op, cancel := context.WithTimeout(ctx, 10*time.Second)
			n, e := DrainOnce(op, q, s, c.Meters)
			cancel()
			if e != nil {
				slog.Warn("postgres delivery failed; data retained in queue", "retry_in", delay.String())
				if !wait(ctx, delay) {
					return
				}
				delay = min(delay*2, 30*time.Second)
				continue
			}
			delay = time.Second
			if n > 0 {
				count, _ := q.Count(ctx)
				slog.Info("measurements delivered", "count", n, "queue_pending", count)
			}
			if n < 100 && !wait(ctx, time.Second) {
				return
			}
		}
	}()
	wg.Wait()
}
func pollConnection(ctx context.Context, r meter.Reader, meters []config.Meter, q *queue.Queue) {
	due := make([]time.Time, len(meters))
	for ctx.Err() == nil {
		soon := time.Now().Add(time.Hour)
		for i, m := range meters {
			if ctx.Err() != nil {
				return
			}
			if time.Now().Before(due[i]) {
				if due[i].Before(soon) {
					soon = due[i]
				}
				continue
			}
			probe, cancel := context.WithTimeout(ctx, 2*time.Second)
			e := q.Ready(probe)
			cancel()
			if e != nil {
				slog.Error("polling paused; local queue unavailable", "meter", m.ID)
				if !wait(ctx, time.Second) {
					return
				}
				break
			}
			value := meter.Poll(ctx, r, m)
			// Finish persisting an in-flight sample even if shutdown interrupts polling.
			for {
				save, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				e = q.Enqueue(save, value)
				cancel()
				if e == nil {
					break
				}
				slog.Error("sample pending; local queue write failed", "meter", m.ID, "measurement", value.ID)
				if !wait(ctx, time.Second) {
					slog.Error("shutdown with unpersisted sample", "meter", m.ID, "measurement", value.ID)
					return
				}
			}
			slog.Info("meter polled", "meter", m.ID, "quality", value.Quality, "measurement", value.ID)
			// Fixed delay: never accumulate missed ticks or overlap samples on a bus.
			due[i] = time.Now().Add(m.Interval)
			if due[i].Before(soon) {
				soon = due[i]
			}
		}
		if !wait(ctx, min(time.Until(soon), time.Second)) {
			return
		}
	}
}
