package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "migrate failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("migrate completed")
}

func run() error {
	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}

	schemaBytes, err := os.ReadFile("internal/adapter/postgres/schema.sql")
	if err != nil {
		return fmt.Errorf("read schema.sql: %w", err)
	}

	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		return fmt.Errorf("connect db: %w", err)
	}
	defer pool.Close()

	if _, err := pool.Exec(context.Background(), string(schemaBytes)); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}

	return nil
}
