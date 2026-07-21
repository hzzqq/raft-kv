package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// GatewayConfig 是网关可配置项。代码常量作为默认值，配置文件可覆盖其中任意项。
// 注：本项目零外部依赖（沙箱不可联网装 yaml 库），故用极简 YAML 子集解析器
// （ParseGatewayConfig），仅支持本项目所需的 top-level `key: value` 与 `[a, b]` 列表；
// 生产环境建议改用 gopkg.in/yaml.v3 等成熟库，这里以自包含换取可构建性。
type GatewayConfig struct {
	ListenAddr     string   `yaml:"listen_addr"`
	RequestTimeout int      `yaml:"request_timeout_sec"`
	MaxConcurrent  int      `yaml:"max_concurrent"`
	ClientRate     float64  `yaml:"client_rate"`
	ClientBurst    int      `yaml:"client_burst"`
	CORSOrigins    []string `yaml:"cors_origins"`
}

// DefaultGatewayConfig 返回代码内默认值。
func DefaultGatewayConfig() GatewayConfig {
	return GatewayConfig{
		ListenAddr:     ":8080",
		RequestTimeout: 30,
		MaxConcurrent:  64,
		ClientRate:     200,
		ClientBurst:    40,
		CORSOrigins:    nil,
	}
}

// ParseGatewayConfig 解析极简 YAML 子集：扫描每一行，跳过空行/注释，按首个 ':'
// 切分 top-level key/value；字符串可加引号或裸写；`[a, b]` 解析为列表。未知键忽略。
func ParseGatewayConfig(data []byte) (GatewayConfig, error) {
	cfg := DefaultGatewayConfig()
	sc := bufio.NewScanner(strings.NewReader(string(data)))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		idx := strings.Index(line, ":")
		if idx < 0 {
			continue
		}
		key := strings.TrimSpace(line[:idx])
		val := strings.TrimSpace(line[idx+1:])
		val = strings.Trim(val, `"'`)
		switch key {
		case "listen_addr":
			cfg.ListenAddr = val
		case "request_timeout_sec":
			if n, err := strconv.Atoi(val); err == nil {
				cfg.RequestTimeout = n
			}
		case "max_concurrent":
			if n, err := strconv.Atoi(val); err == nil {
				cfg.MaxConcurrent = n
			}
		case "client_rate":
			if f, err := strconv.ParseFloat(val, 64); err == nil {
				cfg.ClientRate = f
			}
		case "client_burst":
			if n, err := strconv.Atoi(val); err == nil {
				cfg.ClientBurst = n
			}
		case "cors_origins":
			cfg.CORSOrigins = parseList(val)
		}
	}
	if err := sc.Err(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// parseList 把 `[a, b, c]` 或 `a, b, c` 形式解析为字符串切片（去空白与引号，忽略空项）。
func parseList(val string) []string {
	val = strings.TrimSpace(val)
	if strings.HasPrefix(val, "[") {
		val = strings.Trim(val, "[]")
	}
	if val == "" {
		return nil
	}
	parts := strings.Split(val, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		p = strings.Trim(p, `"'`)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// LoadGatewayConfig 从文件读取并解析配置；文件不存在时返回默认值（不报错）。
func LoadGatewayConfig(path string) (GatewayConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return DefaultGatewayConfig(), nil
		}
		return GatewayConfig{}, fmt.Errorf("read config %s: %w", path, err)
	}
	return ParseGatewayConfig(data)
}

// Apply 把配置应用到 Server（仅在显式非零时覆盖，避免把默认值反向清零）。
// 在 NewServer 之后、Handler() 之前调用。
func (c GatewayConfig) Apply(s *Server) {
	if c.RequestTimeout > 0 {
		s.SetRequestTimeout(time.Duration(c.RequestTimeout) * time.Second)
	}
	if c.MaxConcurrent > 0 {
		s.sem = make(chan struct{}, c.MaxConcurrent)
	}
	if c.ClientBurst > 0 {
		s.SetClientRateLimit(c.ClientRate, c.ClientBurst)
	}
	s.SetCORS(c.CORSOrigins)
}

// RequestTimeout 暴露当前单请求超时配置，供测试/排障读取。
func (s *Server) RequestTimeout() time.Duration { return s.requestTimeout }
