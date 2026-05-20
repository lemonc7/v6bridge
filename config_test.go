package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestJoinRemoteAddr(t *testing.T) {
	tests := []struct {
		name string
		host string
		port int
		want string
	}{
		{name: "domain", host: "example.com", port: 25565, want: "example.com:25565"},
		{name: "ipv4", host: "192.168.100.1", port: 8000, want: "192.168.100.1:8000"},
		{name: "bracketed ipv6", host: "[240e::1]", port: 9000, want: "[240e::1]:9000"},
		{name: "plain ipv6", host: "240e::1", port: 9000, want: "[240e::1]:9000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := JoinRemoteAddr(tt.host, tt.port); got != tt.want {
				t.Fatalf("JoinRemoteAddr() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestConfigValidate(t *testing.T) {
	cfg := Config{
		Network: NetworkConfig{SessionTimeout: 120 * time.Second, BufferSize: 65536},
		Tunnels: []Tunnel{{
			Host:     "example.com",
			Services: []Service{{Name: "mc", Remote: 25565, Local: 25565, Proto: ProtocolTCP}},
		}},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}

	cfg.Tunnels[0].Services[0].Local = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid port error")
	}

	cfg.Tunnels[0].Services[0].Local = 70000
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected out-of-range port error")
	}

	cfg.Tunnels[0].Services[0].Local = 25565
	cfg.Tunnels[0].Services[0].Proto = Protocol("icmp")
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid protocol error")
	}
}

func TestConfigValidateNetwork(t *testing.T) {
	cfg := Config{
		Network: NetworkConfig{SessionTimeout: 0, BufferSize: 65536},
		Tunnels: []Tunnel{{
			Host:     "example.com",
			Services: []Service{{Name: "mc", Remote: 25565, Local: 25565, Proto: ProtocolTCP}},
		}},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid session timeout error")
	}

	cfg.Network = NetworkConfig{SessionTimeout: 120 * time.Second, BufferSize: 512}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid buffer size error")
	}
}

func TestLoadConfigUsesCleanenvDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yml")
	content := []byte(`
tunnels:
  - host: example.com
    services:
      - name: mc
        remote: 25565
        local: 25565
        proto: tcp
`)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if cfg.Network.SessionTimeout != 120*time.Second {
		t.Fatalf("session timeout = %s, want 120s", cfg.Network.SessionTimeout)
	}
	if cfg.Network.BufferSize != 65536 {
		t.Fatalf("buffer size = %d, want 65536", cfg.Network.BufferSize)
	}
}
