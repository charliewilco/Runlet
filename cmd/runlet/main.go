package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/charliewilco/runlet"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) > 0 && args[0] == "serve" {
		args = args[1:]
	}

	fs := flag.NewFlagSet("runlet", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:8787", "")
	maxEventsPerJob := fs.Int("max-events-per-job", 10_000, "")
	maxLineBytes := fs.Int("max-line-bytes", 16_384, "")
	_ = fs.String("log-level", "info", "")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("runlet: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	r := runlet.New(runlet.Config{
		MaxEventsPerJob: *maxEventsPerJob,
		MaxLineBytes:    *maxLineBytes,
	})
	server := runlet.NewServer(r)

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Run(ctx, *addr)
	}()

	r.Start(ctx)

	if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
