// Package main runs Ptah's build-tagged integration package contour, whole or
// one shard of it, and verifies that a sharded contour's reports add up to the
// whole.
//
//	testcontour [--tags T] [--timeout D] [--race] [--dir D]
//	testcontour --shard I --shards N --timings FILE --report-dir DIR ...
//	testcontour verify --reports DIR --floor N [--timings-out FILE] [--summary FILE]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"ptah.run/internal/testcontour"
	"ptah.run/internal/testcontour/shard"
)

func main() {
	var err error
	if len(os.Args) > 1 && os.Args[1] == "verify" {
		err = verify(os.Args[2:])
	} else {
		err = runContour(os.Args[1:])
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "testcontour: %s\n", err)
		os.Exit(1)
	}
}

func runContour(arguments []string) error {
	flags := flag.NewFlagSet("testcontour", flag.ExitOnError)
	var (
		tags      = flags.String("tags", "integration", "comma-separated build tags")
		timeout   = flags.Duration("timeout", 30*time.Minute, "test timeout")
		race      = flags.Bool("race", false, "run the contour with the race detector")
		dir       = flags.String("dir", ".", "module directory in which to run the contour")
		shardFlag = flags.Int("shard", 0, "the part of a sharded contour to run, counted from 1")
		shards    = flags.Int("shards", 0, "split the contour into this many parts")
		timings   = flags.String("timings", "", "timing report that balances the partition")
		reportDir = flags.String("report-dir", "", "directory that receives report.jsonl and, when sharded, manifest.json")
	)
	_ = flags.Parse(arguments)

	config := testcontour.Config{
		Package: "./integration/...",
		Tags:    splitTags(*tags),
		Timeout: *timeout,
		Race:    *race,
		Dir:     *dir,
		Stdout:  os.Stdout,
		Stderr:  os.Stderr,
		Shard:   *shardFlag,
		Shards:  *shards,

		TimingsFile: *timings,
	}
	if *reportDir != "" {
		closeReports, err := openReports(&config, *reportDir)
		if err != nil {
			return err
		}
		defer closeReports()
	}
	return testcontour.Run(context.Background(), config)
}

func openReports(config *testcontour.Config, dir string) (func(), error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create report directory: %w", err)
	}
	var files []*os.File
	closeAll := func() {
		for _, file := range files {
			_ = file.Close()
		}
	}
	report, err := os.Create(filepath.Join(dir, shard.EventsFile))
	if err != nil {
		return nil, fmt.Errorf("create contour report: %w", err)
	}
	files = append(files, report)
	config.Report = report
	if config.Shards != 0 {
		manifest, err := os.Create(filepath.Join(dir, shard.ManifestFile))
		if err != nil {
			closeAll()
			return nil, fmt.Errorf("create shard manifest: %w", err)
		}
		files = append(files, manifest)
		config.Manifest = manifest
	}
	return closeAll, nil
}

func verify(arguments []string) error {
	flags := flag.NewFlagSet("testcontour verify", flag.ExitOnError)
	var (
		reports    = flags.String("reports", "", "directory holding every shard's manifest.json and report.jsonl")
		floor      = flags.Int("floor", 0, "the fewest tests the contour may list")
		timingsOut = flags.String("timings-out", "", "file that receives the measured timings when the contour passes")
		source     = flags.String("source", "", "where the measured timings came from, recorded in them")
		summary    = flags.String("summary", "", "file the Markdown account is appended to")
	)
	_ = flags.Parse(arguments)
	return testcontour.Verify(testcontour.VerifyConfig{
		Reports:    *reports,
		Floor:      *floor,
		TimingsOut: *timingsOut,
		Source:     *source,
		Summary:    *summary,
		Stdout:     os.Stdout,
	})
}

func splitTags(value string) []string {
	if value == "" {
		return nil
	}
	return strings.Split(value, ",")
}
