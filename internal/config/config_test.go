package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDefaultsAndValidation(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/test")
	t.Setenv("API_KEY", strings.Repeat("x", 32))
	source := `connections:
  - id: bus
    transport: tcp
    address: localhost:502
meters:
  - id: m1
    connection: bus
    unit_id: 1
`
	path := filepath.Join(t.TempDir(), "config.yaml")
	if e := os.WriteFile(path, []byte(source), 0600); e != nil {
		t.Fatal(e)
	}
	c, e := Load(path)
	if e != nil {
		t.Fatal(e)
	}
	if c.Meters[0].Interval != 10*time.Second || c.Connections[0].Timeout != 2*time.Second || c.Connections[0].Retries != 1 {
		t.Fatal("missing defaults")
	}
	c.Meters = append(c.Meters, c.Meters[0])
	if c.Validate() == nil {
		t.Fatal("duplicate meter accepted")
	}
	if e = os.WriteFile(path, []byte(source+"unknown: true\n"), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = Load(path); e == nil {
		t.Fatal("unknown field accepted")
	}
}
