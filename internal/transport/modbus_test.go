package transport

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goburrow/modbus"
	"github.com/valhalla/mrt-middleware-monitoring/internal/config"
)

func tcpSimulator(t *testing.T, reply func(int, []byte) []byte) string {
	t.Helper()
	ln, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	var sequence atomic.Int32
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, e := ln.Accept()
			if e != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer c.Close()
				for {
					_ = c.SetDeadline(time.Now().Add(time.Second))
					h := make([]byte, 7)
					if _, e := io.ReadFull(c, h); e != nil {
						return
					}
					n := int(binary.BigEndian.Uint16(h[4:6])) - 1
					if n < 1 || n > 253 {
						return
					}
					p := make([]byte, n)
					if _, e := io.ReadFull(c, p); e != nil {
						return
					}
					body := reply(int(sequence.Add(1)), p)
					if body == nil {
						return
					}
					binary.BigEndian.PutUint16(h[4:6], uint16(len(body)+1))
					if _, e := c.Write(append(h, body...)); e != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() { ln.Close(); wg.Wait() })
	return ln.Addr().String()
}
func TestTCPReadRetryAndException(t *testing.T) {
	addr := tcpSimulator(t, func(n int, p []byte) []byte {
		if p[0] != 3 || binary.BigEndian.Uint16(p[1:3]) != 2999 {
			t.Error("not read-only or incorrect wire address")
		}
		if n == 1 {
			return nil
		}
		if n == 3 {
			return []byte{0x83, 2}
		}
		return []byte{3, 4, 0x41, 0x48, 0, 0}
	})
	x := New(config.Connection{ID: "test", Transport: "tcp", Address: addr, Timeout: 100 * time.Millisecond, Retries: 1})
	defer x.Close()
	b, e := x.Read(context.Background(), 1, 2999, 2)
	if e != nil || len(b) != 4 {
		t.Fatalf("retry failed: %x %v", b, e)
	}
	if _, e = x.Read(context.Background(), 1, 2999, 2); e == nil {
		t.Fatal("exception ignored")
	}
	if _, e = x.Read(context.Background(), 1, 2999, 2); e != nil {
		t.Fatal("exception should not put healthy transport into backoff")
	}
}
func TestTCPTimeoutShortResponseAndRecovery(t *testing.T) {
	for _, mode := range []string{"timeout", "short"} {
		t.Run(mode, func(t *testing.T) {
			addr := tcpSimulator(t, func(n int, p []byte) []byte {
				if n == 1 {
					if mode == "timeout" {
						time.Sleep(100 * time.Millisecond)
					} else {
						return []byte{3, 2, 0, 1}
					}
				}
				return []byte{3, 4, 0, 0, 0, 1}
			})
			x := New(config.Connection{ID: "test", Transport: "tcp", Address: addr, Timeout: 25 * time.Millisecond})
			defer x.Close()
			if _, e := x.Read(context.Background(), 1, 2999, 2); e == nil {
				t.Fatal("bad response accepted")
			}
			if _, e := x.Read(context.Background(), 1, 2999, 2); e != ErrBackoff {
				t.Fatalf("no backoff: %v", e)
			}
			x.next = time.Time{}
			if _, e := x.Read(context.Background(), 1, 2999, 2); e != nil {
				t.Fatal(e)
			}
			if x.delay != time.Second {
				t.Fatal("backoff did not reset")
			}
		})
	}
}

type rtuWire struct {
	handler *modbus.RTUClientHandler
	mode    string
}

func (w rtuWire) Send(req []byte) ([]byte, error) {
	if req[0] != 1 || req[1] != 3 || binary.BigEndian.Uint16(req[2:4]) != 2999 {
		panic("invalid RTU request")
	}
	if w.mode == "short" {
		return []byte{1, 3}, nil
	}
	p := &modbus.ProtocolDataUnit{FunctionCode: 3, Data: []byte{4, 0x41, 0x48, 0, 0}}
	if w.mode == "exception" {
		p.FunctionCode = 0x83
		p.Data = []byte{2}
	}
	response, e := w.handler.Encode(p)
	if w.mode == "crc" {
		response[len(response)-1] ^= 1
	}
	return response, e
}
func TestRTUFrames(t *testing.T) {
	for _, mode := range []string{"normal", "exception", "short", "crc"} {
		t.Run(mode, func(t *testing.T) {
			h := modbus.NewRTUClientHandler("unused")
			h.SlaveId = 1
			client := modbus.NewClient2(h, rtuWire{h, mode})
			b, e := client.ReadHoldingRegisters(2999, 2)
			if mode == "normal" {
				if e != nil || len(b) != 4 {
					t.Fatalf("%x %v", b, e)
				}
			} else if e == nil {
				t.Fatal("invalid RTU frame accepted")
			}
		})
	}
}
