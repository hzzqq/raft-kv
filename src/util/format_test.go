package util

import (
	"testing"
	"time"
)

func TestFormatDuration(t *testing.T) {
	cases := []struct {
		in   time.Duration
		want string
	}{
		{0, "0s"},
		{-5 * time.Second, "0s"},
		{500 * time.Microsecond, "500µs"},
		{500 * time.Millisecond, "500ms"},
		{2*time.Second + 500*time.Millisecond, "2.5s"},
		{65 * time.Second, "1m5s"},
		{90 * time.Minute, "1h30m0s"},
		{2*time.Hour + 3*time.Minute + 4*time.Second, "2h3m4s"},
	}
	for _, c := range cases {
		if got := FormatDuration(c.in); got != c.want {
			t.Fatalf("FormatDuration(%v) = %q，期望 %q", c.in, got, c.want)
		}
	}
}
