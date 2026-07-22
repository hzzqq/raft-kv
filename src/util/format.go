package util

import (
	"fmt"
	"time"
)

// FormatDuration 把时长格式化为紧凑可读字符串，便于日志/可观测输出：
//   - >=1h： "1h2m3s"
//   - >=1m： "2m3s"
//   - >=1s： "2.3s"（保留一位小数）
//   - >=1ms： "500ms"
//   - 否则：  "45µs"
//   - <=0：   "0s"
func FormatDuration(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	switch {
	case d >= time.Hour:
		h := int(d / time.Hour)
		rem := d % time.Hour
		return fmt.Sprintf("%dh%dm%ds", h, int(rem/time.Minute), int(rem%time.Minute/time.Second))
	case d >= time.Minute:
		m := int(d / time.Minute)
		s := int((d % time.Minute) / time.Second)
		return fmt.Sprintf("%dm%ds", m, s)
	case d >= time.Second:
		return fmt.Sprintf("%.1fs", d.Seconds())
	case d >= time.Millisecond:
		return fmt.Sprintf("%dms", int(d/time.Millisecond))
	default:
		return fmt.Sprintf("%dµs", int(d/time.Microsecond))
	}
}
