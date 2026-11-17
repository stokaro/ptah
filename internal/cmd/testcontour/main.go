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
	"errors"
	"flag"
	"fmt"
	"io"
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
	}
	if *timings != "" {
		config.Timings = readTimings(*timings)
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

// readTimings reads the report that balances the partition. A report that
// cannot be read changes only the balance, so it is reported and the partition
// falls back to the test count instead of failing the run. Every shard reads
// the same bytes, so every shard falls back the same way.
func readTimings(path string) shard.Timings {
	file, err := os.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "testcontour: no contour timings, balancing by test count: %s\n", err)
		return shard.Timings{Source: "no readable timings"}
	}
	defer file.Close()
	timings, err := shard.ReadTimings(file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "testcontour: unreadable contour timings, balancing by test count: %s\n", err)
		return shard.Timings{Source: "unreadable timings"}
	}
	return timings
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
		timingsOut = flags.String("timings-out", "", "file that receives the measured timings")
		source     = flags.String("source", "", "where the measured timings came from, recorded in them")
		summary    = flags.String("summary", "", "file the Markdown account is appended to")
	)
	_ = flags.Parse(arguments)
	if *reports == "" {
		return errors.New("verify needs --reports")
	}

	loaded, err := shard.LoadReports(*reports)
	if err != nil {
		return err
	}
	result, verdict := shard.Verify(loaded, *floor)
	account := describe(result, *floor, verdict)
	fmt.Print(account)
	if *summary != "" {
		if err := appendFile(*summary, account); err != nil {
			return err
		}
	}
	if *timingsOut != "" {
		result.Timings.Source = *source
		if err := shard.WriteTimingsFile(*timingsOut, result.Timings); err != nil {
			return err
		}
	}
	return verdict
}

func describe(result shard.Result, floor int, verdict error) string {
	var b strings.Builder
	b.WriteString("### Integration contour\n\n")
	b.WriteString("| shard | tests | planned | test binaries ran |\n|---:|---:|---:|---:|\n")
	for _, part := range result.PerShard {
		fmt.Fprintf(&b, "| %d | %d | %s | %s |\n", part.Shard, part.Tests, minutes(part.PlannedMS), minutes(part.ElapsedMS))
	}
	fmt.Fprintf(&b, "\n- shards: %d; listed: %d (floor %d); ran: %d; passed: %d\n",
		result.Shards, result.Listed, floor, result.Ran, result.Passed)
	fmt.Fprintf(&b, "- missing: %d; unexpected: %d; repeated: %d; misplaced: %d; incomplete: %d; skipped: %d; failed: %d\n",
		len(result.Missing), len(result.Unexpected), len(result.Repeated), len(result.Misplaced),
		len(result.Incomplete), len(result.Skipped), len(result.Failed))
	if result.Weighting != "" {
		fmt.Fprintf(&b, "- balanced by %s; partition %s\n", result.Weighting, result.Digest)
	}
	if verdict == nil {
		b.WriteString("\nEvery listed test ran exactly once, on its shard, and passed.\n")
		return b.String()
	}
	b.WriteString("\nThe contour is refused:\n\n")
	for line := range strings.SplitSeq(verdict.Error(), "\n") {
		fmt.Fprintf(&b, "- %s\n", line)
	}
	return b.String()
}

func minutes(ms int64) string {
	return (time.Duration(ms) * time.Millisecond).Round(time.Second).String()
}

func appendFile(path, text string) error {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open summary: %w", err)
	}
	if _, err := io.WriteString(file, text); err != nil {
		_ = file.Close()
		return fmt.Errorf("write summary: %w", err)
	}
	return file.Close()
}

func splitTags(value string) []string {
	if value == "" {
		return nil
	}
	return strings.Split(value, ",")
}
