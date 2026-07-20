// main.go —— kvcli 命令行入口
//
// 用法：
//   kvcli [-addr http://localhost:8080] get KEY
//   kvcli [-addr http://localhost:8080] put KEY VALUE
//   kvcli [-addr http://localhost:8080] append KEY VALUE
package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
)

func main() {
	addr := flag.String("addr", "http://localhost:8080", "gateway base URL")
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: kvcli [-addr URL] <get|put|append> KEY [VALUE]")
		os.Exit(2)
	}

	c := NewClient(*addr)
	cmd, key := args[0], args[1]

	switch cmd {
	case "get":
		v, err := c.Get(key)
		if err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		fmt.Println(v)

	case "put":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "put requires VALUE")
			os.Exit(2)
		}
		if err := c.Put(key, args[2]); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}

	case "append":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "append requires VALUE")
			os.Exit(2)
		}
		if err := c.Append(key, args[2]); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}

	case "bench":
		// kvcli bench [op=mixed|get|put] [ops=1000] [workers=8]
		op, n, w := "mixed", 1000, 8
		if len(args) >= 3 {
			op = args[2]
		}
		if len(args) >= 4 {
			if v, err := strconv.Atoi(args[3]); err == nil && v > 0 {
				n = v
			}
		}
		if len(args) >= 5 {
			if v, err := strconv.Atoi(args[4]); err == nil && v > 0 {
				w = v
			}
		}
		res := c.Bench(n, w, op, 64)
		fmt.Printf("bench op=%s workers=%d ops=%d duration=%s ops/sec=%.1f errors=%d\n",
			res.Op, res.Workers, res.Ops, res.Duration, res.OpsPerSec, res.Errors)
		fmt.Printf("latency ms: p50=%.2f p95=%.2f p99=%.2f\n", res.LatP50, res.LatP95, res.LatP99)

	default:
		fmt.Fprintln(os.Stderr, "unknown command:", cmd)
		os.Exit(2)
	}
}
