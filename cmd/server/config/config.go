package config
import (
	"fmt"
	"os"
)
type Config struct {
	Port          string
	StorageDriver string 
	DatabaseURL   string 
	RedisAddr     string 
}

func Load() (*Config, error) {
	cfg := &Config{
		Port:          getEnv("PORT", "50051"),
		StorageDriver: getEnv("STORAGE_DRIVER", "memory"),
		DatabaseURL:   os.Getenv("DATABASE_URL"),
		RedisAddr:     os.Getenv("REDIS_ADDR"),
	}

	switch cfg.StorageDriver {
	case "postgres":
		if cfg.DatabaseURL == "" {
			return nil, fmt.Errorf("DATABASE_URL is required when STORAGE_DRIVER is postgres")
		}
	case "redis":
		if cfg.RedisAddr == "" {
			return nil, fmt.Errorf("REDIS_ADDR is required when STORAGE_DRIVER is redis")
		}
	}
	return cfg, nil
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}