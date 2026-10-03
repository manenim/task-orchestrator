package config

import "testing"

func TestLoadRejectsUnknownStorage(t *testing.T) {
	t.Setenv("STORAGE_DRIVER", "typo")
	if _, err := Load(); err == nil {
		t.Fatal("unknown driver silently falls back to ephemeral memory")
	}
}
