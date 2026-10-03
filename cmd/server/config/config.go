package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	Port            string
	StorageDriver   string
	DatabaseURL     string
	RedisAddr       string
	TenantID        string
	NamespaceID     string
	ShutdownTimeout time.Duration
}

func Load() (*Config, error) {
	if err := loadDotEnv(); err != nil {
		return nil, err
	}

	cfg := &Config{
		Port:          getEnv("PORT", "50051"),
		StorageDriver: getEnv("STORAGE_DRIVER", "postgres"),
		DatabaseURL:   os.Getenv("DATABASE_URL"),
		RedisAddr:     os.Getenv("REDIS_ADDR"),
		TenantID:      getEnv("TENANT_ID", "default"),
		NamespaceID:   getEnv("NAMESPACE_ID", "default"),
	}

	timeout, err := time.ParseDuration(getEnv("SHUTDOWN_TIMEOUT", "10s"))
	if err != nil || timeout <= 0 {
		return nil, fmt.Errorf("SHUTDOWN_TIMEOUT must be a positive duration")
	}
	cfg.ShutdownTimeout = timeout
	switch cfg.StorageDriver {
	case "postgres":
		if cfg.DatabaseURL == "" {
			return nil, fmt.Errorf("DATABASE_URL is required when STORAGE_DRIVER is postgres")
		}
	case "redis":
		if cfg.RedisAddr == "" {
			return nil, fmt.Errorf("REDIS_ADDR is required when STORAGE_DRIVER is redis")
		}
	case "memory":
	default:
		return nil, fmt.Errorf("unsupported STORAGE_DRIVER: %s", cfg.StorageDriver)
	}
	return cfg, nil
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func loadDotEnv() error {
	for _, path := range dotenvCandidates() {
		if _, err := os.Stat(path); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("failed to access %s: %w", path, err)
		}

		if err := godotenv.Load(path); err != nil {
			return fmt.Errorf("failed to load %s: %w", path, err)
		}
		return nil
	}
	return nil
}

func dotenvCandidates() []string {
	wd, err := os.Getwd()
	if err != nil {
		return []string{".env"}
	}

	candidates := make([]string, 0, 8)
	for dir := wd; ; dir = filepath.Dir(dir) {
		candidates = append(candidates, filepath.Join(dir, ".env"))

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
	}
	return candidates
}
