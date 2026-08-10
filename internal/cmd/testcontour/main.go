// Package main runs Ptah's build-tagged integration package contour.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"go.5x5.cz/ptah/internal/testcontour"
)

func main() {
	var (
		tags    = flag.String("tags", "integration", "comma-separated build tags")
		timeout = flag.Duration("timeout", 30*time.Minute, "test timeout")
		race    = flag.Bool("race", false, "run the contour with the race detector")
		dir     = flag.String("dir", ".", "module directory in which to run the contour")
	)
	flag.Parse()

	err := testcontour.Run(context.Background(), testcontour.Config{
		Package: "./integration/...",
		Tags:    splitTags(*tags),
		Timeout: *timeout,
		Race:    *race,
		Dir:     *dir,
		Stdout:  os.Stdout,
		Stderr:  os.Stderr,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "testcontour: %s\n", err)
		os.Exit(1)
	}
}

func splitTags(value string) []string {
	if value == "" {
		return nil
	}
	return strings.Split(value, ",")
}
