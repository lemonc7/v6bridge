package main

import (
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

// --- 配置结构体 ---
type Config struct {
	Setting Setting       `yaml:"setting"`
	Tunnels []TunnelGroup `yaml:"tunnels"`
}

type Setting struct {
	SessionTimeout   int `yaml:"session_timeout"`
	SocketBufSize    int `yaml:"socket_buf_size"`
	PacketBufferSize int `yaml:"packet_buffer_size"`
	ReportInterval   int `yaml:"report_interval"`
}

type TunnelGroup struct {
	Host     string        `yaml:"host"`
	Services []ServiceItem `yaml:"services"`
}

type ServiceItem struct {
	Name   string `yaml:"name"`
	Remote int    `yaml:"remote"`
	Local  int    `yaml:"local"`
	Proto  string `yaml:"proto"`
}

// loadConfig 读取并解析配置文件，设置默认值
func loadConfig(path string) Config {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("[ERROR] 无法读取配置文件: %v", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("[ERROR] 配置文件解析错误: %v", err)
	}

	s := &cfg.Setting
	if s.SessionTimeout <= 0 {
		s.SessionTimeout = 120
	}
	if s.SocketBufSize <= 0 {
		s.SocketBufSize = 4
	}
	if s.PacketBufferSize <= 0 || s.PacketBufferSize > 64 {
		s.PacketBufferSize = 64
	}
	if s.ReportInterval < 0 {
		s.ReportInterval = 0
	}

	return cfg
}
