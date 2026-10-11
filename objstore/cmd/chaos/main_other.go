//go:build !unix

package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "chaos: crashes and freezes nodes with Unix signals; it needs Linux or macOS")
	os.Exit(2)
}
