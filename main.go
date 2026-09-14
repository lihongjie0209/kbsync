package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"kbsync/internal/cli"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := cli.New().ExecuteContext(ctx); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "错误:", err)
		os.Exit(1)
	}
}
