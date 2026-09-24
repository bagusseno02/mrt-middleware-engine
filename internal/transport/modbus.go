package transport

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/goburrow/modbus"
	"github.com/valhalla/mrt-middleware-monitoring/internal/config"
)

var ErrBackoff = errors.New("connection is waiting for reconnect")

type handler interface {
	modbus.ClientHandler
	Connect() error
	Close() error
}

// Connection serializes all requests, including changes to the slave address.
type Connection struct {
	mu      sync.Mutex
	cfg     config.Connection
	handler handler
	setUnit func(byte)
	client  modbus.Client
	next    time.Time
	delay   time.Duration
}

func New(c config.Connection) *Connection {
	x := &Connection{cfg: c, delay: time.Second}
	if c.Transport == "rtu" {
		h := modbus.NewRTUClientHandler(c.SerialPort)
		h.BaudRate = c.Baud
		h.DataBits = 8
		h.Parity = c.Parity
		h.StopBits = c.StopBits
		h.Timeout = c.Timeout
		h.IdleTimeout = 0
		x.handler = h
		x.setUnit = func(u byte) { h.SlaveId = u }
	} else {
		h := modbus.NewTCPClientHandler(c.Address)
		h.Timeout = c.Timeout
		h.IdleTimeout = 0
		x.handler = h
		x.setUnit = func(u byte) { h.SlaveId = u }
	}
	x.client = modbus.NewClient(x.handler)
	return x
}
func (x *Connection) Close() error { x.mu.Lock(); defer x.mu.Unlock(); return x.handler.Close() }
func (x *Connection) Read(ctx context.Context, unit byte, address, words uint16) ([]byte, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	if time.Now().Before(x.next) {
		return nil, ErrBackoff
	}
	x.setUnit(unit)
	var err error
	for attempt := 0; attempt <= x.cfg.Retries; attempt++ {
		if e := ctx.Err(); e != nil {
			return nil, e
		}
		err = x.handler.Connect()
		if err == nil {
			var b []byte
			b, err = x.client.ReadHoldingRegisters(address, words)
			if err == nil && len(b) != int(words)*2 {
				err = errors.New("short Modbus response")
			}
			if err == nil {
				x.delay = time.Second
				x.next = time.Time{}
				return b, nil
			}
			var exception *modbus.ModbusError
			if errors.As(err, &exception) {
				return nil, fmt.Errorf("Modbus exception %d", exception.ExceptionCode)
			}
		}
		_ = x.handler.Close()
		if attempt < x.cfg.Retries {
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
	}
	x.next = time.Now().Add(x.delay)
	slog.Warn("modbus reconnect scheduled", "connection", x.cfg.ID, "backoff", x.delay.String())
	x.delay = min(30*time.Second, x.delay*2)
	// Keep endpoint details out of stored failure messages.
	return nil, errors.New("Modbus communication failed after retries")
}
