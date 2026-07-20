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

	default:
		fmt.Fprintln(os.Stderr, "unknown command:", cmd)
		os.Exit(2)
	}
}
