package shardmaster

import "testing"

// 构造一个内部一致的合法配置。
func validCfg() *Config {
	c := &Config{Num: 1, Groups: map[int][]string{}}
	c.Groups[1] = []string{"s1", "s2"}
	c.Groups[2] = []string{"s3"}
	for i := 0; i < NShards; i++ {
		c.Shards[i] = 1 + i%2 // 交替归 gid 1/2
	}
	return c
}

// TestValidateConfigOK 验证：内部一致的配置通过校验（无违规）。
func TestValidateConfigOK(t *testing.T) {
	if got := ValidateConfig(validCfg()); len(got) != 0 {
		t.Fatalf("合法配置不应有违规，却得到 %v", got)
	}
}

// TestValidateConfigNil 验证：nil 配置被报告。
func TestValidateConfigNil(t *testing.T) {
	got := ValidateConfig(nil)
	if len(got) == 0 {
		t.Fatal("nil 配置应被报告")
	}
}

// TestValidateConfigNegativeNum 验证：负配置号被报告。
func TestValidateConfigNegativeNum(t *testing.T) {
	c := &Config{Num: -1, Groups: map[int][]string{1: {"s1"}}}
	if got := ValidateConfig(c); len(got) == 0 {
		t.Fatal("负配置号应被报告")
	}
}

// TestValidateConfigUnknownGid 验证：分片指向不存在的 gid 被报告。
func TestValidateConfigUnknownGid(t *testing.T) {
	c := validCfg()
	c.Shards[0] = 99 // 指向未定义的 gid
	got := ValidateConfig(c)
	found := false
	for _, p := range got {
		if contains(p, "unknown gid 99") {
			found = true
		}
	}
	if !found {
		t.Fatalf("应报告分片指向未知 gid 99，实际 %v", got)
	}
}

// TestValidateConfigBadServers 验证：group 空 server 列表 / 空地址被报告。
func TestValidateConfigBadServers(t *testing.T) {
	c := &Config{Num: 1, Groups: map[int][]string{1: {""}}} // 空地址
	got := ValidateConfig(c)
	found := false
	for _, p := range got {
		if contains(p, "empty server address") {
			found = true
		}
	}
	if !found {
		t.Fatalf("应报告空 server 地址，实际 %v", got)
	}
}

// TestValidateConfigNoGroups 验证：已编号配置却无 group 被报告。
func TestValidateConfigNoGroups(t *testing.T) {
	c := &Config{Num: 3} // 编号>0 但无 group
	got := ValidateConfig(c)
	found := false
	for _, p := range got {
		if contains(p, "has no groups") {
			found = true
		}
	}
	if !found {
		t.Fatalf("应报告编号配置无 group，实际 %v", got)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
