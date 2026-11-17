package testcontour

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"ptah.run/internal/testcontour/shard"
)

// VerifyConfig describes one check of a sharded contour's reports.
type VerifyConfig struct {
	// Reports is the directory that holds every shard's manifest and report,
	// as shard.LoadReports reads it.
	Reports string
	// Floor is the fewest tests the contour may list; see shard.Verify.
	Floor int
	// TimingsOut, when set, names the file that receives the timings the
	// shards measured. It is written only for a contour that passes.
	TimingsOut string
	// Source is recorded in the timings as the run they came from.
	Source string
	// Summary, when set, names a file the Markdown account is appended to.
	Summary string
	// Stdout receives the Markdown account. Nil discards it.
	Stdout io.Writer
}

// Verify loads the shards' reports, proves that they ran the listing exactly
// once, and writes the account to Stdout and to Summary. It returns the
// refusal when the contour is refused.
//
// The timings are kept only for a contour that passes. A refused run measures
// whatever went wrong, such as tests that fail in milliseconds against a
// database that never started. A partition balanced on that puts far more
// than a share on whichever shard draws the tests it measured as quick.
func Verify(config VerifyConfig) error {
	if config.Reports == "" {
		return errors.New("verify needs a reports directory")
	}
	if config.Stdout == nil {
		config.Stdout = io.Discard
	}
	loaded, err := shard.LoadReports(config.Reports)
	if err != nil {
		return err
	}
	result, verdict := shard.Verify(loaded, config.Floor)
	account := describe(result, config.Floor, verdict)
	if _, err := io.WriteString(config.Stdout, account); err != nil {
		return fmt.Errorf("write the contour account: %w", err)
	}
	if config.Summary != "" {
		if err := appendFile(config.Summary, account); err != nil {
			return err
		}
	}
	if verdict != nil {
		return verdict
	}
	if config.TimingsOut != "" {
		result.Timings.Source = config.Source
		if err := shard.WriteTimingsFile(config.TimingsOut, result.Timings); err != nil {
			return err
		}
	}
	return nil
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
	if len(result.Absent) != 0 {
		fmt.Fprintf(&b, "- shards that sent no report: %v\n", result.Absent)
	}
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
