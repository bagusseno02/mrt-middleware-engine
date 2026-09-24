package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/valhalla/mrt-middleware-monitoring/internal/config"
	"github.com/valhalla/mrt-middleware-monitoring/internal/model"
	"github.com/valhalla/mrt-middleware-monitoring/migrations"
)

type Store struct{ Pool *pgxpool.Pool }

func Open(ctx context.Context, url string) (*Store, error) {
	c, e := pgxpool.ParseConfig(url)
	if e != nil {
		return nil, errors.New("invalid DATABASE_URL")
	}
	c.ConnConfig.ConnectTimeout = 3 * time.Second
	c.MaxConns = 5
	p, e := pgxpool.NewWithConfig(ctx, c)
	if e != nil {
		return nil, errors.New("cannot initialize PostgreSQL pool")
	}
	return &Store{p}, nil
}
func (s *Store) Close() { s.Pool.Close() }
func (s *Store) Ready(ctx context.Context) error {
	var ok bool
	e := s.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=1) AND to_regclass('measurements') IS NOT NULL`).Scan(&ok)
	if e != nil {
		return e
	}
	if !ok {
		return errors.New("database migrations missing")
	}
	return nil
}
func (s *Store) Migrate(ctx context.Context) error {
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(5110001)"); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); e != nil {
		return e
	}
	var done bool
	if e = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=1)").Scan(&done); e != nil {
		return e
	}
	if !done {
		if _, e = tx.Exec(ctx, migrations.Initial); e != nil {
			return e
		}
		if _, e = tx.Exec(ctx, "INSERT INTO schema_migrations(version) VALUES(1)"); e != nil {
			return e
		}
	}
	return tx.Commit(ctx)
}

const columns = `id,meter_id,observed_at,completed_at,quality,errors,current_a_a,current_b_a,current_c_a,voltage_ab_v,voltage_bc_v,voltage_ca_v,voltage_an_v,voltage_bn_v,voltage_cn_v,active_power_kw,reactive_power_kvar,apparent_power_kva,power_factor,frequency_hz,energy_import_wh,energy_export_wh`
const selectColumns = `id::text,meter_id,observed_at,completed_at,quality,errors,current_a_a,current_b_a,current_c_a,voltage_ab_v,voltage_bc_v,voltage_ca_v,voltage_an_v,voltage_bn_v,voltage_cn_v,active_power_kw,reactive_power_kvar,apparent_power_kva,power_factor,frequency_hz,energy_import_wh::text,energy_export_wh::text`

func (s *Store) Write(ctx context.Context, meters []config.Meter, items []model.Measurement) error {
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	for _, m := range meters {
		_, e = tx.Exec(ctx, `INSERT INTO meters(id,name,location,poll_interval_ms) VALUES($1,$2,$3,$4) ON CONFLICT(id) DO UPDATE SET name=excluded.name,location=excluded.location,poll_interval_ms=excluded.poll_interval_ms,updated_at=now()`, m.ID, m.Name, m.Location, m.Interval.Milliseconds())
		if e != nil {
			return e
		}
	}
	placeholders := make([]string, 22)
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	placeholders[20] = "$21::text::bigint"
	placeholders[21] = "$22::text::bigint"
	for _, m := range items {
		// Preserve queued data from meters removed from the current configuration.
		if _, e = tx.Exec(ctx, `INSERT INTO meters(id,name,poll_interval_ms) VALUES($1,$1,10000) ON CONFLICT(id) DO NOTHING`, m.MeterID); e != nil {
			return e
		}
		b, e := json.Marshal(m.Errors)
		if e != nil {
			return e
		}
		v := m.Values
		_, e = tx.Exec(ctx, "INSERT INTO measurements("+columns+") VALUES("+strings.Join(placeholders, ",")+") ON CONFLICT(id) DO NOTHING", m.ID, m.MeterID, m.ObservedAt, m.CompletedAt, m.Quality, b, v.CurrentA, v.CurrentB, v.CurrentC, v.VoltageAB, v.VoltageBC, v.VoltageCA, v.VoltageAN, v.VoltageBN, v.VoltageCN, v.ActivePower, v.ReactivePower, v.ApparentPower, v.PowerFactor, v.Frequency, v.EnergyImport, v.EnergyExport)
		if e != nil {
			return e
		}
	}
	return tx.Commit(ctx)
}
func scan(row pgx.Row) (model.Measurement, error) {
	var m model.Measurement
	var b []byte
	v := &m.Values
	e := row.Scan(&m.ID, &m.MeterID, &m.ObservedAt, &m.CompletedAt, &m.Quality, &b, &v.CurrentA, &v.CurrentB, &v.CurrentC, &v.VoltageAB, &v.VoltageBC, &v.VoltageCA, &v.VoltageAN, &v.VoltageBN, &v.VoltageCN, &v.ActivePower, &v.ReactivePower, &v.ApparentPower, &v.PowerFactor, &v.Frequency, &v.EnergyImport, &v.EnergyExport)
	if e == nil {
		e = json.Unmarshal(b, &m.Errors)
	}
	m.ObservedAt = m.ObservedAt.UTC()
	m.CompletedAt = m.CompletedAt.UTC()
	return m, e
}
func (s *Store) Latest(ctx context.Context, id string) (model.Measurement, error) {
	return scan(s.Pool.QueryRow(ctx, "SELECT "+selectColumns+" FROM measurements WHERE meter_id=$1 ORDER BY observed_at DESC,id DESC LIMIT 1", id))
}
func (s *Store) History(ctx context.Context, id string, q model.Query) ([]model.Measurement, error) {
	args := []any{id, q.From, q.To}
	where := "meter_id=$1 AND observed_at >= $2 AND observed_at < $3"
	if q.Cursor != nil {
		where += " AND (observed_at,id)<($4,$5::uuid)"
		args = append(args, q.Cursor.Time, q.Cursor.ID)
	}
	args = append(args, q.Limit)
	rows, e := s.Pool.Query(ctx, "SELECT "+selectColumns+" FROM measurements WHERE "+where+fmt.Sprintf(" ORDER BY observed_at DESC,id DESC LIMIT $%d", len(args)), args...)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []model.Measurement{}
	for rows.Next() {
		m, e := scan(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
