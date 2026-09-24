// Package meter contains the read-only PM5110 register profile.
package meter

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/valhalla/mrt-middleware-monitoring/internal/config"
	"github.com/valhalla/mrt-middleware-monitoring/internal/model"
)

type Reader interface {
	Read(context.Context, byte, uint16, uint16) ([]byte, error)
}
type Register struct {
	Name         string
	Number       uint16 // Schneider one-based register; wire address = Number - 1.
	Words        uint16
	Type         string
	Unit         string
	WordOrder    string
	Scale        float64
	FloatTarget  func(*model.Values) **float64
	StringTarget func(*model.Values) **string
}

var Profile = []Register{
	{"current_a_a", 3000, 2, "float32", "A", "msw_first", 1, func(v *model.Values) **float64 { return &v.CurrentA }, nil},
	{"current_b_a", 3002, 2, "float32", "A", "msw_first", 1, func(v *model.Values) **float64 { return &v.CurrentB }, nil},
	{"current_c_a", 3004, 2, "float32", "A", "msw_first", 1, func(v *model.Values) **float64 { return &v.CurrentC }, nil},
	{"voltage_ab_v", 3020, 2, "float32", "V", "msw_first", 1, func(v *model.Values) **float64 { return &v.VoltageAB }, nil},
	{"voltage_bc_v", 3022, 2, "float32", "V", "msw_first", 1, func(v *model.Values) **float64 { return &v.VoltageBC }, nil},
	{"voltage_ca_v", 3024, 2, "float32", "V", "msw_first", 1, func(v *model.Values) **float64 { return &v.VoltageCA }, nil},
	{"voltage_an_v", 3028, 2, "float32", "V", "msw_first", 1, func(v *model.Values) **float64 { return &v.VoltageAN }, nil},
	{"voltage_bn_v", 3030, 2, "float32", "V", "msw_first", 1, func(v *model.Values) **float64 { return &v.VoltageBN }, nil},
	{"voltage_cn_v", 3032, 2, "float32", "V", "msw_first", 1, func(v *model.Values) **float64 { return &v.VoltageCN }, nil},
	{"active_power_kw", 3060, 2, "float32", "kW", "msw_first", 1, func(v *model.Values) **float64 { return &v.ActivePower }, nil},
	{"reactive_power_kvar", 3068, 2, "float32", "kvar", "msw_first", 1, func(v *model.Values) **float64 { return &v.ReactivePower }, nil},
	{"apparent_power_kva", 3076, 2, "float32", "kVA", "msw_first", 1, func(v *model.Values) **float64 { return &v.ApparentPower }, nil},
	{"power_factor", 3084, 2, "pf4q", "1", "msw_first", 1, func(v *model.Values) **float64 { return &v.PowerFactor }, nil},
	{"frequency_hz", 3110, 2, "float32", "Hz", "msw_first", 1, func(v *model.Values) **float64 { return &v.Frequency }, nil},
	{"energy_import_wh", 3204, 4, "int64", "Wh", "low_dword_first", 1, nil, func(v *model.Values) **string { return &v.EnergyImport }},
	{"energy_export_wh", 3208, 4, "int64", "Wh", "low_dword_first", 1, nil, func(v *model.Values) **string { return &v.EnergyExport }},
}

func Decode(r Register, b []byte, v *model.Values) error {
	if len(b) != int(r.Words)*2 {
		return errors.New("unexpected register response length")
	}
	switch r.Type {
	case "float32", "pf4q":
		if r.WordOrder != "msw_first" {
			return errors.New("unsupported float word order")
		}
		if len(b) != 4 {
			return errors.New("float32 requires two registers")
		}
		x := float64(math.Float32frombits(binary.BigEndian.Uint32(b)))
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return errors.New("meter returned unavailable/non-finite value")
		}
		if r.Type == "pf4q" {
			if math.Abs(x) > 2 {
				return errors.New("invalid four-quadrant power factor")
			}
			if x > 1 {
				x = 2 - x
			} else if x < -1 {
				x = -2 - x
			}
		}
		x *= r.Scale
		*r.FloatTarget(v) = &x
	case "int64":
		if len(b) != 8 {
			return errors.New("int64 requires four registers")
		}
		if r.WordOrder != "low_dword_first" {
			return errors.New("unsupported int64 word order")
		}
		// Schneider stores the lower UINT32 in registers 3204-3205 and
		// the higher UINT32 in 3206-3207 (similarly 3208-3211).
		low := uint64(binary.BigEndian.Uint32(b[0:4]))
		high := uint64(binary.BigEndian.Uint32(b[4:8]))
		raw := low | high<<32
		if raw >= 1<<63 {
			return errors.New("unavailable or invalid cumulative energy")
		}
		s := strconv.FormatUint(raw, 10)
		*r.StringTarget(v) = &s
	default:
		return fmt.Errorf("unsupported type %s", r.Type)
	}
	return nil
}

func Poll(ctx context.Context, r Reader, c config.Meter) model.Measurement {
	m := model.Measurement{ID: uuid.NewString(), MeterID: c.ID, ObservedAt: time.Now().UTC(), Quality: "failed", Errors: map[string]string{}}
	// Product ID is read for each sample: never apply this profile to another device.
	b, e := r.Read(ctx, c.UnitID, 89, 1)
	if e != nil || len(b) != 2 {
		m.Errors["model"] = "cannot read PM5110 product ID"
		m.CompletedAt = time.Now().UTC()
		return m
	}
	if binary.BigEndian.Uint16(b) != 15270 {
		m.Errors["model"] = "unexpected product ID; PM5110 requires 15270"
		m.CompletedAt = time.Now().UTC()
		return m
	}
	good, total := 0, 0
	for _, p := range Profile {
		if p.Type == "int64" && !c.Energy {
			continue
		}
		total++
		b, e = r.Read(ctx, c.UnitID, p.Number-1, p.Words)
		if e == nil {
			e = Decode(p, b, &m.Values)
		}
		if e != nil {
			m.Errors[p.Name] = e.Error()
		} else {
			good++
		}
	}
	if good == total {
		m.Quality = "good"
	} else if good > 0 {
		m.Quality = "partial"
	}
	m.CompletedAt = time.Now().UTC()
	return m
}
