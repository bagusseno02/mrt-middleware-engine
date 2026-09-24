// Package queue implements a durable, bounded local outbox.
package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/valhalla/mrt-middleware-monitoring/internal/model"
	_ "modernc.org/sqlite"
)

var ErrFull = errors.New("local queue capacity reached")

type Queue struct {
	db          *sql.DB
	mu          sync.Mutex
	maxPages    int64
	writeFailed bool
}
type Item struct {
	Sequence    int64
	Measurement model.Measurement
}

func Open(path string, maxBytes int64) (*Queue, error) {
	if maxBytes < 1<<20 {
		return nil, errors.New("queue budget must be at least 1 MiB")
	}
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return nil, e
	}
	// Half the disk budget is reserved for the rollback journal and its headers.
	pages := (maxBytes/2 - 16384) / 4096
	db, e := sql.Open("sqlite", path)
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(1)
	q := &Queue{db: db, maxPages: pages}
	for _, stmt := range []string{"PRAGMA busy_timeout=1000", "PRAGMA page_size=4096", "PRAGMA journal_mode=DELETE", "PRAGMA synchronous=FULL", "PRAGMA locking_mode=EXCLUSIVE", fmt.Sprintf("PRAGMA max_page_count=%d", pages), `CREATE TABLE IF NOT EXISTS outbox (sequence INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, payload BLOB NOT NULL)`, `CREATE TABLE IF NOT EXISTS health (id INTEGER PRIMARY KEY CHECK(id=1), nonce INTEGER NOT NULL)`, `INSERT OR IGNORE INTO health VALUES(1,0)`} {
		if _, e = db.Exec(stmt); e != nil {
			db.Close()
			return nil, e
		}
	}
	var size, count int64
	if e = db.QueryRow("PRAGMA page_size").Scan(&size); e == nil {
		e = db.QueryRow("PRAGMA page_count").Scan(&count)
	}
	if e != nil || size != 4096 || count > pages {
		db.Close()
		return nil, errors.New("existing queue exceeds configured capacity or has unsupported page size")
	}
	return q, nil
}
func (q *Queue) Close() error { return q.db.Close() }
func (q *Queue) capacity(ctx context.Context) error {
	var used, free int64
	if e := q.db.QueryRowContext(ctx, "PRAGMA page_count").Scan(&used); e != nil {
		return e
	}
	if e := q.db.QueryRowContext(ctx, "PRAGMA freelist_count").Scan(&free); e != nil {
		return e
	}
	// Reserve four pages for the next sample and B-tree changes.
	if q.maxPages-used+free < 4 {
		return ErrFull
	}
	return nil
}
func (q *Queue) Ready(ctx context.Context) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if e := q.capacity(ctx); e != nil {
		return e
	}
	if q.writeFailed {
		return errors.New("local queue write has not recovered")
	}
	_, e := q.db.ExecContext(ctx, "UPDATE health SET nonce=1-nonce WHERE id=1")
	return e
}
func (q *Queue) Enqueue(ctx context.Context, m model.Measurement) error {
	payload, e := json.Marshal(m)
	if e != nil {
		return e
	}
	if len(payload) > 8192 {
		return errors.New("measurement exceeds 8 KiB queue record limit")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	var exists bool
	if e = q.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM outbox WHERE id=?)", m.ID).Scan(&exists); e != nil {
		q.writeFailed = true
		return e
	}
	if exists {
		q.writeFailed = false
		return nil
	}
	if e = q.capacity(ctx); e != nil {
		return e
	}
	_, e = q.db.ExecContext(ctx, "INSERT INTO outbox(id,payload) VALUES(?,?) ON CONFLICT(id) DO NOTHING", m.ID, payload)
	q.writeFailed = e != nil
	return e
}
func (q *Queue) Peek(ctx context.Context, limit int) ([]Item, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	rows, e := q.db.QueryContext(ctx, "SELECT sequence,payload FROM outbox ORDER BY sequence LIMIT ?", limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Item{}
	for rows.Next() {
		var x Item
		var b []byte
		if e = rows.Scan(&x.Sequence, &b); e != nil {
			return nil, e
		}
		if e = json.Unmarshal(b, &x.Measurement); e != nil {
			return nil, e
		}
		out = append(out, x)
	}
	return out, rows.Err()
}
func (q *Queue) Ack(ctx context.Context, items []Item) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	tx, e := q.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	for _, x := range items {
		if _, e = tx.ExecContext(ctx, "DELETE FROM outbox WHERE sequence=? AND id=?", x.Sequence, x.Measurement.ID); e != nil {
			return e
		}
	}
	return tx.Commit()
}
func (q *Queue) Count(ctx context.Context) (int64, error) {
	var n int64
	e := q.db.QueryRowContext(ctx, "SELECT count(*) FROM outbox").Scan(&n)
	return n, e
}
