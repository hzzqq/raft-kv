package main

import (
	"bufio"
	"fmt"
	"net"
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
	ListenAddr      string   `yaml:"listen_addr"`
	RequestTimeout  int      `yaml:"request_timeout_sec"`
	MaxConcurrent   int      `yaml:"max_concurrent"`
	ClientRate      float64  `yaml:"client_rate"`
	ClientBurst     int      `yaml:"client_burst"`
	CORSOrigins     []string `yaml:"cors_origins"`
	MaxBodySize     int      `yaml:"max_body_size"`
	Compress        bool     `yaml:"compress"`
	SecurityHeaders bool     `yaml:"security_headers"`
	AllowCIDRs      []string `yaml:"allow_cidrs"`
}

// DefaultGatewayConfig 返回代码内默认值。
func DefaultGatewayConfig() GatewayConfig {
	return GatewayConfig{
		ListenAddr:      ":8080",
		RequestTimeout:  30,
		MaxConcurrent:   64,
		ClientRate:      200,
		ClientBurst:     40,
		CORSOrigins:     nil,
		MaxBodySize:     1 << 20,
		Compress:        true,
		SecurityHeaders: true,
		AllowCIDRs:      nil,
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
		case "max_body_size":
			if n, err := strconv.Atoi(val); err == nil {
				cfg.MaxBodySize = n
			}
		case "compress":
			cfg.Compress = (val == "true" || val == "1" || val == "yes")
		case "security_headers":
			cfg.SecurityHeaders = (val == "true" || val == "1" || val == "yes")
		case "allow_cidrs":
			cfg.AllowCIDRs = parseList(val)
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

// Validate 校验网关配置的自洽性，返回问题列表（空切片=通过）。纯函数、cluster-free，
// 风格对齐 shardmaster.ValidateConfig：在 Apply 前做硬守卫，把结构性错误（非法监听地址、
// 非法端口、超时/并发/限流/体量为非正、非法 CIDR）集中暴露，便于调用方在真正应用前拦截。
// 注意：与本方法互补，Apply 内仍保留「越界但可用」的软告警（levelWarn，如超时过大、并发过低）；
// 本方法只报告硬错误（levelError 级别，真正会让监听/限流失效的取值）。
func (c GatewayConfig) Validate() []string {
	var problems []string
	// listen_addr 必须非空且为合法 host:port，端口落在 1..65535。
	if c.ListenAddr == "" {
		problems = append(problems, "listen_addr empty")
	} else {
		_, portStr, err := net.SplitHostPort(c.ListenAddr)
		if err != nil {
			problems = append(problems, fmt.Sprintf("listen_addr %q not host:port: %v", c.ListenAddr, err))
		} else if p, err := strconv.Atoi(portStr); err != nil || p < 1 || p > 65535 {
			problems = append(problems, fmt.Sprintf("listen_addr port %q out of range 1..65535", portStr))
		}
	}
	if c.RequestTimeout <= 0 {
		problems = append(problems, "request_timeout_sec must be > 0")
	} else if c.RequestTimeout > 600 {
		problems = append(problems, "request_timeout_sec > 600s (unusually large)")
	}
	if c.MaxConcurrent < 1 {
		problems = append(problems, "max_concurrent must be >= 1")
	}
	if c.ClientRate < 0 {
		problems = append(problems, "client_rate must be >= 0")
	}
	if c.ClientBurst < 0 {
		problems = append(problems, "client_burst must be >= 0")
	}
	if c.ClientBurst > 0 && c.ClientRate <= 0 {
		problems = append(problems, "client_burst > 0 but client_rate <= 0 (rate limiting ineffective)")
	}
	if c.MaxBodySize <= 0 {
		problems = append(problems, "max_body_size must be > 0")
	}
	for _, cidr := range c.AllowCIDRs {
		if _, _, err := net.ParseCIDR(cidr); err != nil {
			problems = append(problems, fmt.Sprintf("allow_cidrs %q invalid CIDR: %v", cidr, err))
		}
	}
	return problems
}

// Apply 把配置应用到 Server（仅在显式非零时覆盖，避免把默认值反向清零）。
// 在 NewServer 之后、Handler() 之前调用。越界/可疑值会记 warn 日志（经 s.logf），
// 便于在 /debug/log 中回看配置加载时的问题，但不阻断启动。应用前先跑 Validate 做硬守卫，
// 任何结构性错误以 levelError 记录（不阻断，便于后续 Listen 失败时能回看原因）。
func (c GatewayConfig) Apply(s *Server) {
	if problems := c.Validate(); len(problems) > 0 {
		for _, p := range problems {
			s.logf(levelError, "config: validation problem", map[string]string{"problem": p})
		}
	}
	if c.RequestTimeout > 0 {
		if c.RequestTimeout > 600 {
			s.logf(levelWarn, "config: request_timeout_sec unusually large", map[string]string{"value": strconv.Itoa(c.RequestTimeout)})
		}
		s.SetRequestTimeout(time.Duration(c.RequestTimeout) * time.Second)
	} else if c.RequestTimeout < 0 {
		s.logf(levelWarn, "config: negative request_timeout_sec ignored", map[string]string{"value": strconv.Itoa(c.RequestTimeout)})
	}
	if c.MaxConcurrent > 0 {
		if c.MaxConcurrent < 4 {
			s.logf(levelWarn, "config: max_concurrent very low (frequent 429 risk)", map[string]string{"value": strconv.Itoa(c.MaxConcurrent)})
		}
		s.sem = make(chan struct{}, c.MaxConcurrent)
	} else if c.MaxConcurrent < 0 {
		s.logf(levelWarn, "config: negative max_concurrent ignored", map[string]string{"value": strconv.Itoa(c.MaxConcurrent)})
	}
	if c.MaxBodySize > 0 {
		if c.MaxBodySize < 1024 {
			s.logf(levelWarn, "config: max_body_size very small", map[string]string{"value": strconv.Itoa(c.MaxBodySize)})
		} else if c.MaxBodySize > 100*1024*1024 {
			s.logf(levelWarn, "config: max_body_size very large", map[string]string{"value": strconv.Itoa(c.MaxBodySize)})
		}
		s.maxBodySize = int64(c.MaxBodySize)
	}
	s.compress = c.Compress
	s.secHeaders = c.SecurityHeaders
	if c.ClientBurst > 0 {
		if c.ClientRate <= 0 {
			s.logf(levelWarn, "config: client_rate <=0 with client_burst set (rate limiting ineffective)", map[string]string{"client_burst": strconv.Itoa(c.ClientBurst)})
		}
		s.SetClientRateLimit(c.ClientRate, c.ClientBurst)
	}
	s.SetCORS(c.CORSOrigins)
	if len(c.AllowCIDRs) > 0 {
		s.SetIPAllow(c.AllowCIDRs)
	}
	s.curCfg = c
}

// RequestTimeout 暴露当前单请求超时配置，供测试/排障读取。
func (s *Server) RequestTimeout() time.Duration { return s.requestTimeout }
