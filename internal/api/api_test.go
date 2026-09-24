package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/valhalla/mrt-middleware-monitoring/internal/config"
	"github.com/valhalla/mrt-middleware-monitoring/internal/model"
)

const key = "01234567890123456789012345678901"

type fakeStore struct {
	err    error
	latest model.Measurement
	items  []model.Measurement
}

func (f *fakeStore) Ready(context.Context) error { return f.err }
func (f *fakeStore) Latest(context.Context, string) (model.Measurement, error) {
	return f.latest, f.err
}
func (f *fakeStore) History(context.Context, string, model.Query) ([]model.Measurement, error) {
	return f.items, f.err
}
func call(h http.Handler, path, token string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("GET", path, nil)
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func TestAuthStatusAndStale(t *testing.T) {
	f := &fakeStore{latest: model.Measurement{ObservedAt: time.Now().Add(-time.Minute), Quality: "failed"}}
	q := &fakeStore{}
	h := New(f, q, []config.Meter{{ID: "m1", Interval: 10 * time.Second}}, key)
	for _, tt := range []struct {
		path, token string
		want        int
	}{{"/health/live", "", 200}, {"/health/ready", "", 200}, {"/v1/meters", "", 401}, {"/v1/meters", "wrong", 401}, {"/v1/meters", key, 200}, {"/v1/meters/no/latest", key, 404}, {"/v1/meters/m1/latest", key, 200}} {
		if w := call(h, tt.path, tt.token); w.Code != tt.want {
			t.Fatalf("%s: %d", tt.path, w.Code)
		}
	}
	w := call(h, "/v1/meters/m1/latest", key)
	if !strings.Contains(w.Body.String(), `"stale":true`) {
		t.Fatal(w.Body.String())
	}
	f.err = pgx.ErrNoRows
	if w = call(h, "/v1/meters/m1/latest", key); w.Code != 404 {
		t.Fatal(w.Code)
	}
	f.err = errors.New("secret connection details")
	if w = call(h, "/v1/meters/m1/latest", key); w.Code != 503 || strings.Contains(w.Body.String(), "secret") {
		t.Fatal(w.Body.String())
	}
	if w = call(h, "/health/ready", ""); w.Code != 503 {
		t.Fatal(w.Code)
	}
	f.err = nil
	q.err = errors.New("disk full")
	if w = call(h, "/health/ready", ""); w.Code != 503 {
		t.Fatal(w.Code)
	}
}
func TestHistoryValidationAndCursor(t *testing.T) {
	at := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	f := &fakeStore{items: []model.Measurement{{ID: uuid.NewString(), ObservedAt: at}, {ID: uuid.NewString(), ObservedAt: at.Add(-time.Second)}}}
	h := New(f, &fakeStore{}, []config.Meter{{ID: "m1", Interval: time.Second}}, key)
	base := "/v1/meters/m1/measurements"
	valid := "?from=2026-01-01T00:00:00Z&to=2026-01-03T00:00:00Z&limit=1"
	for _, query := range []string{"", "?from=no&to=no", valid + "&limit=2", valid + "&extra=1", valid + "&cursor=bad", "?from=2026-01-01T00:00:00Z&to=2026-03-01T00:00:00Z", "?from=2026-01-01T00:00:00Z&to=2026-01-02T00:00:00Z&limit=1001"} {
		if w := call(h, base+query, key); w.Code != 400 {
			t.Fatalf("%s: %d", query, w.Code)
		}
	}
	w := call(h, base+valid, key)
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	var page struct {
		Items []model.Measurement `json:"items"`
		Next  string              `json:"next_cursor"`
	}
	if e := json.Unmarshal(w.Body.Bytes(), &page); e != nil {
		t.Fatal(e)
	}
	if len(page.Items) != 1 || page.Next == "" {
		t.Fatal(w.Body.String())
	}
	if w = call(h, base+valid+"&cursor="+page.Next, key); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if w = call(h, base+strings.Replace(valid, "01-03", "01-04", 1)+"&cursor="+page.Next, key); w.Code != 400 {
		t.Fatal("cross-query cursor accepted")
	}
}
