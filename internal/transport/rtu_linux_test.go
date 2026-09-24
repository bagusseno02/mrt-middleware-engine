//go:build linux

package transport

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/goburrow/modbus"
	"github.com/valhalla/mrt-middleware-monitoring/internal/config"
	"golang.org/x/sys/unix"
)

// Exercise the actual serial driver against a Linux pseudo-terminal. No meter,
// root privileges, or physical RS-485 adapter is required.
func TestRTUSerialPseudoTerminal(t *testing.T) {
	fd, e := unix.Open("/dev/ptmx", unix.O_RDWR|unix.O_NOCTTY|unix.O_NONBLOCK, 0)
	if e != nil {
		t.Fatal(e)
	}
	master := os.NewFile(uintptr(fd), "ptmx")
	defer master.Close()
	if e = unix.IoctlSetPointerInt(fd, unix.TIOCSPTLCK, 0); e != nil {
		t.Fatal(e)
	}
	n, e := unix.IoctlGetInt(fd, unix.TIOCGPTN)
	if e != nil {
		t.Fatal(e)
	}
	path := fmt.Sprintf("/dev/pts/%d", n)
	x := New(config.Connection{ID: "rtu", Transport: "rtu", SerialPort: path, Baud: 19200, Parity: "N", StopBits: 1, Timeout: 100 * time.Millisecond, Retries: 1})
	defer x.Close()
	// Open the slave before reading the master, otherwise Linux returns EIO.
	if e = x.handler.Connect(); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() {
		req := make([]byte, 8)
		if _, e := io.ReadFull(master, req); e != nil {
			done <- e
			return
		}
		if req[1] != 3 || binary.BigEndian.Uint16(req[2:4]) != 2999 {
			done <- fmt.Errorf("unexpected frame %x", req)
			return
		}
		h := modbus.NewRTUClientHandler("unused")
		h.SlaveId = 1
		b, e := h.Encode(&modbus.ProtocolDataUnit{FunctionCode: 3, Data: []byte{4, 0x41, 0x48, 0, 0}})
		if e == nil {
			_, e = master.Write(b)
		}
		done <- e
	}()
	b, e := x.Read(context.Background(), 1, 2999, 2)
	if e != nil || len(b) != 4 {
		t.Fatalf("serial read %x: %v", b, e)
	}
	if e = <-done; e != nil {
		t.Fatal(e)
	}
}
