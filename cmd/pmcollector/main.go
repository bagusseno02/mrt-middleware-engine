package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/valhalla/mrt-middleware-monitoring/internal/api"
	"github.com/valhalla/mrt-middleware-monitoring/internal/collector"
	"github.com/valhalla/mrt-middleware-monitoring/internal/config"
	"github.com/valhalla/mrt-middleware-monitoring/internal/postgres"
	"github.com/valhalla/mrt-middleware-monitoring/internal/queue"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if e := run(); e != nil {
		slog.Error("service stopped", "error", e.Error())
		os.Exit(1)
	}
}
func run() error {
	command := "serve"
	args := os.Args[1:]
	if len(args) > 0 && (args[0] == "serve" || args[0] == "migrate" || args[0] == "check") {
		command = args[0]
		args = args[1:]
	}
	flags := flag.NewFlagSet("pmcollector", flag.ContinueOnError)
	path := flags.String("config", "config.yaml", "YAML configuration path")
	if e := flags.Parse(args); e != nil {
		return e
	}
	if flags.NArg() != 0 {
		return errors.New("usage: pmcollector [serve|migrate|check] -config config.yaml")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// Migration only needs the database secret, never hardware or API configuration.
	if command == "migrate" {
		url := os.Getenv("DATABASE_URL")
		if url == "" {
			return errors.New("DATABASE_URL required")
		}
		s, e := postgres.Open(ctx, url)
		if e != nil {
			return e
		}
		defer s.Close()
		op, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if e = s.Migrate(op); e != nil {
			return errors.New("migration failed; verify PostgreSQL connectivity, permissions and schema")
		}
		slog.Info("migrations applied")
		return nil
	}
	c, e := config.Load(*path)
	if e != nil {
		return fmt.Errorf("configuration: %w", e)
	}
	if command == "check" {
		slog.Info("configuration valid", "meters", len(c.Meters))
		return nil
	}
	s, e := postgres.Open(ctx, c.DatabaseURL)
	if e != nil {
		return e
	}
	defer s.Close()
	q, e := queue.Open(c.QueuePath, c.QueueMaxBytes)
	if e != nil {
		return fmt.Errorf("open local queue: %w", e)
	}
	defer q.Close()
	server := &http.Server{Addr: c.HTTPAddress, Handler: api.New(s, q, c.Meters, c.APIKey), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	serverErr := make(chan error, 1)
	go func() { serverErr <- server.ListenAndServe() }()
	done := make(chan struct{})
	go func() { collector.Run(ctx, c, q, s); close(done) }()
	slog.Info("collector started", "meters", len(c.Meters), "http_address", c.HTTPAddress)
	select {
	case <-ctx.Done():
	case e = <-serverErr:
		if errors.Is(e, http.ErrServerClosed) {
			e = nil
		}
		stop()
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdown)
	<-done
	slog.Info("collector stopped; queued data retained")
	return e
}
