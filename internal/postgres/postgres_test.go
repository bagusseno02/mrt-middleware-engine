package postgres_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/valhalla/mrt-middleware-monitoring/internal/collector"
	"github.com/valhalla/mrt-middleware-monitoring/internal/config"
	"github.com/valhalla/mrt-middleware-monitoring/internal/model"
	"github.com/valhalla/mrt-middleware-monitoring/internal/postgres"
	"github.com/valhalla/mrt-middleware-monitoring/internal/queue"
)

func database(t *testing.T) *postgres.Store {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL to run real PostgreSQL integration tests")
	}
	ctx := context.Background()
	admin, e := pgxpool.New(ctx, url)
	if e != nil {
		t.Fatal(e)
	}
	schema := "pmtest_" + uuid.New().String()[:8]
	if _, e = admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); e != nil {
		admin.Close()
		t.Fatal(e)
	}
	c, e := pgxpool.ParseConfig(url)
	if e != nil {
		t.Fatal(e)
	}
	c.ConnConfig.RuntimeParams["search_path"] = schema
	pool, e := pgxpool.NewWithConfig(ctx, c)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		admin.Close()
	})
	return &postgres.Store{Pool: pool}
}
func TestPostgresMigrationsMeasurementsAndPagination(t *testing.T) {
	s := database(t)
	ctx := context.Background()
	if s.Ready(ctx) == nil {
		t.Fatal("missing schema reported ready")
	}
	if e := s.Migrate(ctx); e != nil {
		t.Fatal(e)
	}
	if e := s.Migrate(ctx); e != nil {
		t.Fatal("migration not idempotent", e)
	}
	if e := s.Ready(ctx); e != nil {
		t.Fatal(e)
	}
	at := time.Now().UTC().Truncate(time.Microsecond)
	energy := "9007199254740993"
	current := 12.5
	meters := []config.Meter{{ID: "m1", Name: "Panel", Interval: 10 * time.Second}}
	ms := []model.Measurement{
		{ID: uuid.NewString(), MeterID: "m1", ObservedAt: at, CompletedAt: at, Quality: "partial", Errors: map[string]string{"voltage": "N/A"}, Values: model.Values{CurrentA: &current, EnergyImport: &energy}},
		{ID: uuid.NewString(), MeterID: "m1", ObservedAt: at, CompletedAt: at, Quality: "failed", Errors: map[string]string{"connection": "timeout"}},
		{ID: uuid.NewString(), MeterID: "m1", ObservedAt: at.Add(-time.Second), CompletedAt: at, Quality: "good", Errors: map[string]string{}},
	}
	for i := 0; i < 2; i++ {
		if e := s.Write(ctx, meters, ms); e != nil {
			t.Fatal(e)
		}
	}
	var count int
	if e := s.Pool.QueryRow(ctx, "SELECT count(*) FROM measurements").Scan(&count); e != nil || count != 3 {
		t.Fatalf("dedup %d %v", count, e)
	}
	q := model.Query{From: at.Add(-time.Hour), To: at.Add(time.Second), Limit: 1}
	seen := map[string]bool{}
	for i := 0; i < 3; i++ {
		rows, e := s.History(ctx, "m1", q)
		if e != nil || len(rows) != 1 {
			t.Fatalf("page %d: %v %v", i, rows, e)
		}
		m := rows[0]
		if seen[m.ID] {
			t.Fatal("duplicate page")
		}
		seen[m.ID] = true
		q.Cursor = &model.Cursor{Time: m.ObservedAt, ID: m.ID}
	}
	if rows, e := s.History(ctx, "m1", q); e != nil || len(rows) != 0 {
		t.Fatal("unexpected extra page", e)
	}
	latest, e := s.Latest(ctx, "m1")
	if e != nil || !latest.ObservedAt.Equal(at) {
		t.Fatal("latest", e)
	}
	rows, e := s.History(ctx, "m1", model.Query{From: at.Add(-time.Hour), To: at.Add(time.Second), Limit: 100})
	if e != nil {
		t.Fatal(e)
	}
	for _, m := range rows {
		if m.ID == ms[0].ID && (m.Values.EnergyImport == nil || *m.Values.EnergyImport != energy || m.Values.VoltageAB != nil || *m.Values.CurrentA != current) {
			t.Fatalf("numeric/null roundtrip: %+v", m)
		}
	}
	if _, e = s.Latest(ctx, "unknown"); e != pgx.ErrNoRows {
		t.Fatalf("missing latest %v", e)
	}
	// The exclusive upper bound must not include measurements at exactly 'to'.
	rows, e = s.History(ctx, "m1", model.Query{From: at.Add(-time.Hour), To: at, Limit: 100})
	if e != nil || len(rows) != 1 {
		t.Fatal("exclusive upper bound", e)
	}
}
func TestPostgresOutageRestartAndCommittedReplay(t *testing.T) {
	s := database(t)
	ctx := context.Background()
	if e := s.Migrate(ctx); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(t.TempDir(), "queue.db")
	q, e := queue.Open(path, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	at := time.Now().UTC()
	m := model.Measurement{ID: uuid.NewString(), MeterID: "removed-meter", ObservedAt: at, CompletedAt: at, Quality: "failed", Errors: map[string]string{"communication": "offline"}}
	if e = q.Enqueue(ctx, m); e != nil {
		t.Fatal(e)
	}
	ln, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	address := ln.Addr().String()
	ln.Close()
	offline, e := postgres.Open(ctx, "postgres://test:test@"+address+"/test?sslmode=disable")
	if e != nil {
		t.Fatal(e)
	}
	timeout, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	_, e = collector.DrainOnce(timeout, q, offline, nil)
	cancel()
	offline.Close()
	if e == nil {
		t.Fatal("expected unreachable database")
	}
	if n, _ := q.Count(ctx); n != 1 {
		t.Fatal("outage lost sample")
	}
	q.Close()
	q, e = queue.Open(path, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	// Model a server commit followed by process death before local acknowledgement.
	if e = s.Write(ctx, nil, []model.Measurement{m}); e != nil {
		t.Fatal(e)
	}
	if n, e := collector.DrainOnce(ctx, q, s, nil); e != nil || n != 1 {
		t.Fatalf("replay %d %v", n, e)
	}
	var count int
	if e = s.Pool.QueryRow(ctx, "SELECT count(*) FROM measurements").Scan(&count); e != nil || count != 1 {
		t.Fatalf("duplicate committed record %d %v", count, e)
	}
	if n, _ := q.Count(ctx); n != 0 {
		t.Fatal("successful delivery unacknowledged")
	}
}
