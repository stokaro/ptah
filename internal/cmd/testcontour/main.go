// Package main runs build-tagged live test contours for Ptah CI.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"go.5x5.cz/ptah/internal/testcontour"
)

func main() {
	var (
		packageName = flag.String("package", "", "Go package containing the contour")
		tag         = flag.String("tag", "", "build tag that declares contour membership")
		tags        = flag.String("tags", "", "additional comma-separated build tags")
		timeout     = flag.Duration("timeout", 0, "test timeout")
	)
	flag.Parse()

	err := testcontour.Run(context.Background(), testcontour.Config{
		Package: *packageName,
		Tag:     *tag,
		Tags:    splitTags(*tags),
		Timeout: *timeout,
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
