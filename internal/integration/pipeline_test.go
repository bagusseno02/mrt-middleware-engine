package integration_test

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/valhalla/mrt-middleware-monitoring/internal/api"
	"github.com/valhalla/mrt-middleware-monitoring/internal/collector"
	"github.com/valhalla/mrt-middleware-monitoring/internal/config"
	"github.com/valhalla/mrt-middleware-monitoring/internal/postgres"
	"github.com/valhalla/mrt-middleware-monitoring/internal/queue"
)

func eventually(t *testing.T, f func() bool) {
	t.Helper()
	until := time.Now().Add(8 * time.Second)
	for time.Now().Before(until) {
		if f() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}

func TestMeterToDatabaseAndAPIAcrossOutageAndRestart(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL for end-to-end integration")
	}
	ctx := context.Background()
	admin, e := pgxpool.New(ctx, url)
	if e != nil {
		t.Fatal(e)
	}
	defer admin.Close()
	schema := "pme2e_" + uuid.NewString()[:8]
	if _, e = admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); e != nil {
		t.Fatal(e)
	}
	defer admin.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
	pc, e := pgxpool.ParseConfig(url)
	if e != nil {
		t.Fatal(e)
	}
	pc.ConnConfig.RuntimeParams["search_path"] = schema
	pc.ConnConfig.ConnectTimeout = 200 * time.Millisecond
	var offline atomic.Bool
	var mu sync.Mutex
	connections := []net.Conn{}
	pc.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		if offline.Load() {
			return nil, errors.New("simulated database network outage")
		}
		c, e := (&net.Dialer{}).DialContext(ctx, network, address)
		if e == nil {
			mu.Lock()
			if offline.Load() {
				c.Close()
				mu.Unlock()
				return nil, errors.New("network outage")
			}
			connections = append(connections, c)
			mu.Unlock()
		}
		return c, e
	}
	pool, e := pgxpool.NewWithConfig(ctx, pc)
	if e != nil {
		t.Fatal(e)
	}
	defer pool.Close()
	s := &postgres.Store{Pool: pool}
	if e = s.Migrate(ctx); e != nil {
		t.Fatal(e)
	}
	address := meterSimulator(t)
	c := config.Config{Connections: []config.Connection{{ID: "bus", Transport: "tcp", Address: address, Timeout: 100 * time.Millisecond, Retries: 1}}, Meters: []config.Meter{{ID: "m1", Name: "Simulated PM5110", Connection: "bus", UnitID: 1, Interval: 50 * time.Millisecond, Energy: true}}}
	path := filepath.Join(t.TempDir(), "queue.db")
	q, e := queue.Open(path, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { q.Close() }()
	run, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { collector.Run(run, c, q, s); close(done) }()
	defer func() { cancel(); <-done }()
	eventually(t, func() bool {
		m, e := s.Latest(ctx, "m1")
		return e == nil && m.Quality == "good" && m.Values.EnergyImport != nil && *m.Values.EnergyImport == "89550842"
	})
	handler := api.New(s, q, c.Meters, "01234567890123456789012345678901")
	req := httptest.NewRequest("GET", "/v1/meters/m1/latest", nil)
	req.Header.Set("Authorization", "Bearer 01234567890123456789012345678901")
	result := httptest.NewRecorder()
	handler.ServeHTTP(result, req)
	if result.Code != 200 {
		t.Fatal(result.Body.String())
	}
	offline.Store(true)
	mu.Lock()
	for _, conn := range connections {
		conn.Close()
	}
	mu.Unlock()
	eventually(t, func() bool { n, e := q.Count(ctx); return e == nil && n >= 3 })
	ready := httptest.NewRecorder()
	handler.ServeHTTP(ready, httptest.NewRequest("GET", "/health/ready", nil))
	if ready.Code != 503 {
		t.Fatal("database outage not reflected in readiness")
	}
	cancel()
	<-done
	backlog, e := q.Peek(ctx, 1000)
	if e != nil || len(backlog) == 0 {
		t.Fatal("no persisted backlog", e)
	}
	if e = q.Close(); e != nil {
		t.Fatal(e)
	}
	q, e = queue.Open(path, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	offline.Store(false)
	resume, stopResume := context.WithCancel(ctx)
	resumedDone := make(chan struct{})
	go func() { collector.Run(resume, c, q, s); close(resumedDone) }()
	defer func() { stopResume(); <-resumedDone }()
	eventually(t, func() bool {
		for _, item := range backlog {
			var n int
			if e := pool.QueryRow(ctx, "SELECT count(*) FROM measurements WHERE id=$1", item.Measurement.ID).Scan(&n); e != nil || n != 1 {
				return false
			}
		}
		return true
	})
	var total, distinct int
	if e = pool.QueryRow(ctx, "SELECT count(*),count(DISTINCT id) FROM measurements").Scan(&total, &distinct); e != nil || total != distinct {
		t.Fatal("duplicates after recovery", e)
	}
}

func meterSimulator(t *testing.T) string {
	ln, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, e := ln.Accept()
			if e != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conn.Close()
				for {
					conn.SetDeadline(time.Now().Add(time.Second))
					h := make([]byte, 7)
					if _, e := io.ReadFull(conn, h); e != nil {
						return
					}
					n := int(binary.BigEndian.Uint16(h[4:6])) - 1
					if n != 5 {
						return
					}
					p := make([]byte, n)
					if _, e := io.ReadFull(conn, p); e != nil {
						return
					}
					if p[0] != 3 {
						return
					}
					addr := binary.BigEndian.Uint16(p[1:3])
					words := binary.BigEndian.Uint16(p[3:5])
					data := make([]byte, int(words)*2)
					switch {
					case addr == 89:
						binary.BigEndian.PutUint16(data, 15270)
					case words == 4:
						// PM5110 INT64 energy uses low UINT32 followed by high UINT32.
						binary.BigEndian.PutUint32(data[0:4], 89550842)
						binary.BigEndian.PutUint32(data[4:8], 0)
					case words == 2:
						binary.BigEndian.PutUint32(data, math.Float32bits(.8))
					default:
						return
					}
					body := append([]byte{3, byte(len(data))}, data...)
					binary.BigEndian.PutUint16(h[4:6], uint16(len(body)+1))
					if _, e = conn.Write(append(h, body...)); e != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() { ln.Close(); wg.Wait() })
	return ln.Addr().String()
}
