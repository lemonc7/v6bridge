package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Network NetworkConfig `yaml:"network"`
	Tunnels []Tunnel      `yaml:"tunnels"`
}

type NetworkConfig struct {
	SessionTimeout time.Duration `yaml:"session_timeout" env:"SESSION_TIMEOUT" env-default:"120s"`
	BufferSize     int           `yaml:"buffer_size" env:"BUFFER_SIZE" env-default:"65536"`
}

type Tunnel struct {
	Host     string    `yaml:"host"`
	Services []Service `yaml:"services"`
}

type Service struct {
	Name   string   `yaml:"name"`
	Remote int      `yaml:"remote"`
	Local  int      `yaml:"local"`
	Proto  Protocol `yaml:"proto"`
}

type Protocol string

const (
	ProtocolTCP  Protocol = "tcp"
	ProtocolUDP  Protocol = "udp"
	ProtocolBoth Protocol = "both"
)

func (p Protocol) String() string {
	switch p {
	case ProtocolTCP:
		return "TCP"
	case ProtocolUDP:
		return "UDP"
	case ProtocolBoth:
		return "TCP & UDP"
	default:
		return string(p)
	}
}

func (p Protocol) Valid() bool {
	switch p {
	case ProtocolTCP, ProtocolUDP, ProtocolBoth:
		return true
	default:
		return false
	}
}

func LoadConfig(path string) (Config, error) {
	var cfg Config
	if err := cleanenv.ReadConfig(path, &cfg); err != nil {
		return Config{}, fmt.Errorf("读取配置文件 %q 失败: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if c.Network.SessionTimeout <= 0 {
		return fmt.Errorf("network.session_timeout 必须大于 0")
	}
	if c.Network.BufferSize < 1024 || c.Network.BufferSize > 4*1024*1024 {
		return fmt.Errorf("network.buffer_size 必须在 1024-4194304 字节范围内")
	}

	for i, tunnel := range c.Tunnels {
		if strings.TrimSpace(tunnel.Host) == "" {
			return fmt.Errorf("tunnels[%d].host 不能为空", i)
		}
		if len(tunnel.Services) == 0 {
			return fmt.Errorf("tunnels[%d].services 不能为空", i)
		}
		for j, svc := range tunnel.Services {
			if strings.TrimSpace(svc.Name) == "" {
				return fmt.Errorf("tunnels[%d].services[%d].name 不能为空", i, j)
			}
			if !svc.Proto.Valid() {
				return fmt.Errorf("tunnels[%d].services[%d].proto 必须是 tcp、udp 或 both", i, j)
			}
			if !validPort(svc.Local) || !validPort(svc.Remote) {
				return fmt.Errorf("tunnels[%d].services[%d] 端口必须在 1-65535 范围内", i, j)
			}
		}
	}
	return nil
}

func validPort(port int) bool {
	return port >= 1 && port <= 65535
}
