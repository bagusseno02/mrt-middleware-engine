package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/valhalla/mrt-middleware-monitoring/internal/config"
	"github.com/valhalla/mrt-middleware-monitoring/internal/model"
)

type Store interface {
	Ready(context.Context) error
	Latest(context.Context, string) (model.Measurement, error)
	History(context.Context, string, model.Query) ([]model.Measurement, error)
}
type LocalQueue interface{ Ready(context.Context) error }
type API struct {
	store  Store
	queue  LocalQueue
	meters map[string]config.Meter
	list   []config.Meter
	key    [32]byte
}

func New(s Store, q LocalQueue, meters []config.Meter, key string) http.Handler {
	a := &API{s, q, map[string]config.Meter{}, meters, sha256.Sum256([]byte(key))}
	for _, m := range meters {
		a.meters[m.ID] = m
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /health/ready", a.ready)
	data := http.NewServeMux()
	data.HandleFunc("GET /v1/meters", a.meterList)
	data.HandleFunc("GET /v1/meters/{id}/latest", a.latest)
	data.HandleFunc("GET /v1/meters/{id}/measurements", a.history)
	mux.Handle("/v1/", a.auth(data))
	return mux
}
func respond(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter, status int, msg string) {
	respond(w, status, map[string]string{"error": msg})
}
func (a *API) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		hash := sha256.Sum256([]byte(token))
		if !ok || subtle.ConstantTimeCompare(hash[:], a.key[:]) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			fail(w, 401, "unauthorized")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
func (a *API) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if a.store.Ready(ctx) != nil || a.queue.Ready(ctx) != nil {
		respond(w, 503, map[string]string{"status": "not_ready"})
		return
	}
	respond(w, 200, map[string]string{"status": "ok"})
}
func (a *API) meterList(w http.ResponseWriter, r *http.Request) {
	if a.store.Ready(r.Context()) != nil {
		fail(w, 503, "database unavailable")
		return
	}
	type entry struct {
		config.Meter
		Interval string `json:"poll_interval"`
	}
	out := make([]entry, 0, len(a.list))
	for _, m := range a.list {
		out = append(out, entry{m, m.Interval.String()})
	}
	respond(w, 200, map[string]any{"items": out})
}
func (a *API) latest(w http.ResponseWriter, r *http.Request) {
	m, ok := a.meters[r.PathValue("id")]
	if !ok {
		fail(w, 404, "meter not found")
		return
	}
	value, e := a.store.Latest(r.Context(), m.ID)
	if errors.Is(e, pgx.ErrNoRows) {
		fail(w, 404, "measurement not found")
		return
	}
	if e != nil {
		fail(w, 503, "database unavailable")
		return
	}
	respond(w, 200, struct {
		model.Measurement
		Stale bool `json:"stale"`
	}{value, time.Since(value.ObservedAt) > 3*m.Interval})
}

type pageCursor struct {
	model.Cursor
	Meter string    `json:"meter"`
	From  time.Time `json:"from"`
	To    time.Time `json:"to"`
}

func parseQuery(r *http.Request) (model.Query, error) {
	v := r.URL.Query()
	q := model.Query{Limit: 100}
	for k, values := range v {
		if len(values) != 1 {
			return q, errors.New("duplicate query parameter")
		}
		switch k {
		case "from", "to", "limit", "cursor":
		default:
			return q, errors.New("unknown query parameter")
		}
	}
	var e error
	q.From, e = time.Parse(time.RFC3339Nano, v.Get("from"))
	if e != nil {
		return q, errors.New("from must be RFC3339")
	}
	q.To, e = time.Parse(time.RFC3339Nano, v.Get("to"))
	if e != nil {
		return q, errors.New("to must be RFC3339")
	}
	if !q.To.After(q.From) || q.To.Sub(q.From) > 31*24*time.Hour {
		return q, errors.New("time range must be positive and at most 31 days")
	}
	if v.Has("limit") {
		q.Limit, e = strconv.Atoi(v.Get("limit"))
		if e != nil || q.Limit < 1 || q.Limit > 1000 {
			return q, errors.New("limit must be between 1 and 1000")
		}
	}
	if v.Has("cursor") {
		raw := v.Get("cursor")
		if len(raw) > 1024 {
			return q, errors.New("invalid cursor")
		}
		b, e := base64.RawURLEncoding.DecodeString(raw)
		var c pageCursor
		if e != nil || json.Unmarshal(b, &c) != nil {
			return q, errors.New("invalid cursor")
		}
		if _, e = uuid.Parse(c.ID); e != nil || c.Meter != r.PathValue("id") || !c.From.Equal(q.From) || !c.To.Equal(q.To) || c.Time.Before(q.From) || !c.Time.Before(q.To) {
			return q, errors.New("cursor does not match query")
		}
		q.Cursor = &c.Cursor
	}
	return q, nil
}
func (a *API) history(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := a.meters[id]; !ok {
		fail(w, 404, "meter not found")
		return
	}
	q, e := parseQuery(r)
	if e != nil {
		fail(w, 400, e.Error())
		return
	}
	limit := q.Limit
	q.Limit++
	values, e := a.store.History(r.Context(), id, q)
	if e != nil {
		fail(w, 503, "database unavailable")
		return
	}
	var next *string
	if len(values) > limit {
		values = values[:limit]
		last := values[len(values)-1]
		b, _ := json.Marshal(pageCursor{model.Cursor{Time: last.ObservedAt, ID: last.ID}, id, q.From, q.To})
		s := base64.RawURLEncoding.EncodeToString(b)
		next = &s
	}
	if values == nil {
		values = []model.Measurement{}
	}
	respond(w, 200, struct {
		Items []model.Measurement `json:"items"`
		Next  *string             `json:"next_cursor"`
	}{values, next})
}
