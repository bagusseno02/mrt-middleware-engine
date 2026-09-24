package meter

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/valhalla/mrt-middleware-monitoring/internal/config"
	"github.com/valhalla/mrt-middleware-monitoring/internal/model"
)

func bytes32(f float32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, math.Float32bits(f))
	return b
}
func TestDecode(t *testing.T) {
	for _, tt := range []struct{ raw, want float32 }{{.85, .85}, {1.2, .8}, {-1.2, -.8}, {-.5, -.5}, {1, 1}, {-1, -1}, {2, 0}} {
		var v model.Values
		if e := Decode(Profile[12], bytes32(tt.raw), &v); e != nil {
			t.Fatal(e)
		}
		if math.Abs(*v.PowerFactor-float64(tt.want)) > 1e-6 {
			t.Fatalf("PF %v => %v", tt.raw, *v.PowerFactor)
		}
	}
	var v model.Values
	if e := Decode(Profile[0], []byte{0x41, 0x48, 0, 0}, &v); e != nil || *v.CurrentA != 12.5 {
		t.Fatalf("float decode: %+v %v", v, e)
	}
	for _, b := range [][]byte{{1}, bytes32(float32(math.NaN())), bytes32(float32(math.Inf(1)))} {
		if Decode(Profile[0], b, &v) == nil {
			t.Fatal("invalid float accepted")
		}
	}
	b := make([]byte, 8)
	// 0x0020000000000001 is transferred as low DWORD, then high DWORD.
	binary.BigEndian.PutUint32(b[0:4], 1)
	binary.BigEndian.PutUint32(b[4:8], 0x00200000)
	if e := Decode(Profile[14], b, &v); e != nil || *v.EnergyImport != "9007199254740993" {
		t.Fatalf("energy precision: %v %v", v.EnergyImport, e)
	}
	binary.BigEndian.PutUint32(b[0:4], 0)
	binary.BigEndian.PutUint32(b[4:8], 0x80000000)
	if Decode(Profile[14], b, &v) == nil {
		t.Fatal("INT64 N/A accepted")
	}
}

type fakeReader struct {
	fail       uint16
	wrongModel bool
	calls      []uint16
}

func (f *fakeReader) Read(_ context.Context, _ byte, addr, words uint16) ([]byte, error) {
	f.calls = append(f.calls, addr)
	if addr == f.fail {
		return nil, errors.New("simulated failure")
	}
	if addr == 89 {
		if f.wrongModel {
			return []byte{0, 1}, nil
		}
		return []byte{0x3b, 0xa6}, nil
	}
	if words == 4 {
		return []byte{0, 0, 0, 100, 0, 0, 0, 0}, nil
	}
	return bytes32(.8), nil
}
func TestPollQualityAddressAndIdentity(t *testing.T) {
	c := config.Meter{ID: "m1", UnitID: 1, Energy: true}
	r := &fakeReader{}
	m := Poll(context.Background(), r, c)
	if m.Quality != "good" || len(r.calls) != 17 || r.calls[1] != 2999 || m.Values.EnergyImport == nil {
		t.Fatalf("bad profile: %+v", m)
	}
	r = &fakeReader{fail: 2999}
	m = Poll(context.Background(), r, c)
	if m.Quality != "partial" || m.Values.CurrentA != nil || m.Values.CurrentB == nil {
		t.Fatalf("partial: %+v", m)
	}
	r = &fakeReader{wrongModel: true}
	m = Poll(context.Background(), r, c)
	if m.Quality != "failed" || len(r.calls) != 1 || m.Values.Frequency != nil {
		t.Fatal("wrong model must not read measurements")
	}
	r = &fakeReader{fail: 89}
	m = Poll(context.Background(), r, c)
	if m.Quality != "failed" {
		t.Fatal("missing model")
	}
	c.Energy = false
	r = &fakeReader{}
	m = Poll(context.Background(), r, c)
	if m.Quality != "good" || m.Values.EnergyImport != nil || len(r.calls) != 15 {
		t.Fatal("disabled energy polled")
	}
}
