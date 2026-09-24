package config

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	HTTPAddress   string       `yaml:"http_address"`
	QueuePath     string       `yaml:"queue_path"`
	QueueMaxBytes int64        `yaml:"queue_max_bytes"`
	Connections   []Connection `yaml:"connections"`
	Meters        []Meter      `yaml:"meters"`
	DatabaseURL   string       `yaml:"-"`
	APIKey        string       `yaml:"-"`
}

var validID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)

type Connection struct {
	ID         string        `yaml:"id"`
	Transport  string        `yaml:"transport"`
	Address    string        `yaml:"address"`
	SerialPort string        `yaml:"serial_port"`
	Baud       int           `yaml:"baud"`
	Parity     string        `yaml:"parity"`
	StopBits   int           `yaml:"stop_bits"`
	Timeout    time.Duration `yaml:"timeout"`
	Retries    int           `yaml:"retries"`
}
type Meter struct {
	ID         string        `yaml:"id" json:"id"`
	Name       string        `yaml:"name" json:"name"`
	Location   string        `yaml:"location" json:"location"`
	Connection string        `yaml:"connection" json:"connection"`
	UnitID     byte          `yaml:"unit_id" json:"unit_id"`
	Interval   time.Duration `yaml:"interval" json:"-"`
	Energy     bool          `yaml:"energy" json:"energy_enabled"`
}

// UnmarshalYAML preserves useful defaults while allowing an explicit zero retry count.
func (c *Connection) UnmarshalYAML(n *yaml.Node) error {
	allowed := map[string]bool{"id": true, "transport": true, "address": true, "serial_port": true, "baud": true, "parity": true, "stop_bits": true, "timeout": true, "retries": true}
	for i := 0; i+1 < len(n.Content); i += 2 {
		if !allowed[n.Content[i].Value] {
			return fmt.Errorf("unknown connection field %q", n.Content[i].Value)
		}
	}
	type plain Connection
	v := plain{Baud: 19200, Parity: "E", StopBits: 1, Timeout: 2 * time.Second, Retries: 1}
	if err := n.Decode(&v); err != nil {
		return err
	}
	*c = Connection(v)
	return nil
}
func Load(path string) (Config, error) {
	c := Config{HTTPAddress: "127.0.0.1:8080", QueuePath: "data/queue.db", QueueMaxBytes: 1 << 30}
	f, e := os.Open(path)
	if e != nil {
		return c, e
	}
	defer f.Close()
	d := yaml.NewDecoder(f)
	d.KnownFields(true)
	if e = d.Decode(&c); e != nil {
		return c, e
	}
	var extra any
	if e = d.Decode(&extra); e != io.EOF {
		return c, errors.New("configuration must contain one YAML document")
	}
	c.DatabaseURL = os.Getenv("DATABASE_URL")
	c.APIKey = os.Getenv("API_KEY")
	return c, c.Validate()
}
func (c *Config) Validate() error {
	if c.DatabaseURL == "" {
		return errors.New("DATABASE_URL is required")
	}
	if len(c.APIKey) < 32 {
		return errors.New("API_KEY must contain at least 32 characters")
	}
	if _, _, e := net.SplitHostPort(c.HTTPAddress); e != nil {
		return errors.New("invalid http_address")
	}
	if c.QueuePath == "" || c.QueueMaxBytes < 1<<20 {
		return errors.New("queue_path required; queue_max_bytes must be at least 1 MiB")
	}
	if len(c.Connections) == 0 || len(c.Meters) == 0 {
		return errors.New("at least one connection and meter required")
	}
	ids := map[string]bool{}
	endpoints := map[string]bool{}
	for _, v := range c.Connections {
		if !validID.MatchString(v.ID) || ids[v.ID] {
			return errors.New("connection IDs must be nonempty and unique")
		}
		ids[v.ID] = true
		if v.Timeout <= 0 || v.Timeout > 30*time.Second || v.Retries < 0 || v.Retries > 3 {
			return fmt.Errorf("connection %s: invalid timeout/retries", v.ID)
		}
		endpoint := v.Transport + ":" + v.Address
		switch v.Transport {
		case "tcp":
			if _, _, e := net.SplitHostPort(v.Address); e != nil {
				return fmt.Errorf("connection %s: invalid TCP address", v.ID)
			}
		case "rtu":
			endpoint = "rtu:" + v.SerialPort
			if v.SerialPort == "" || (v.Baud != 9600 && v.Baud != 19200 && v.Baud != 38400) || (v.Parity != "N" && v.Parity != "E" && v.Parity != "O") || (v.StopBits != 1 && v.StopBits != 2) {
				return fmt.Errorf("connection %s: invalid serial settings", v.ID)
			}
		default:
			return fmt.Errorf("connection %s: transport must be rtu or tcp", v.ID)
		}
		if endpoints[endpoint] {
			return errors.New("duplicate physical connection; share a connection ID between meters")
		}
		endpoints[endpoint] = true
	}
	meters := map[string]bool{}
	units := map[string]bool{}
	for i := range c.Meters {
		m := &c.Meters[i]
		if m.Interval == 0 {
			m.Interval = 10 * time.Second
		}
		key := fmt.Sprintf("%s/%d", m.Connection, m.UnitID)
		if !validID.MatchString(m.ID) || meters[m.ID] || units[key] || !ids[m.Connection] || m.UnitID < 1 || m.UnitID > 247 || m.Interval < time.Second {
			return errors.New("invalid meter ID, connection, unit_id or interval")
		}
		meters[m.ID] = true
		units[key] = true
	}
	return nil
}
