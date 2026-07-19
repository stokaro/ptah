// Package goannotationcleanup removes Ptah schema annotations from Go source.
package goannotationcleanup

import (
	"bytes"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// Result describes cleanup changes for one file.
type Result struct {
	Path         string
	Changed      bool
	RemovedLines int
	Diff         string
}

// Options controls cleanup behavior.
type Options struct {
	RootDir string
	DryRun  bool
	Diff    bool
}

type cleanMode int

const (
	cleanModeWrite cleanMode = iota
	cleanModeDryRun
)

// CleanDir removes Ptah schema annotations from Go files under RootDir.
func CleanDir(opts Options) ([]Result, error) {
	root := opts.RootDir
	if root == "" {
		root = "."
	}
	root = filepath.Clean(root)
	var results []Result
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if path != root && (entry.Name() == "vendor" || strings.HasPrefix(entry.Name(), ".")) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		mode := cleanModeWrite
		if opts.DryRun || opts.Diff {
			mode = cleanModeDryRun
		}
		result, err := cleanFile(path, mode)
		if err != nil {
			return err
		}
		if result.Changed {
			if !opts.Diff {
				result.Diff = ""
			}
			results = append(results, result)
		}
		return nil
	})
	return results, err
}

func cleanFile(path string, mode cleanMode) (Result, error) {
	info, err := os.Stat(path)
	if err != nil {
		return Result{}, fmt.Errorf("stat %s: %w", path, err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		return Result{}, fmt.Errorf("read %s: %w", path, err)
	}
	after, removed := removeAnnotationLines(before)
	if removed == 0 {
		return Result{Path: path}, nil
	}
	formatted, err := format.Source(after)
	if err != nil {
		return Result{}, fmt.Errorf("format cleaned Go file %s: %w", path, err)
	}
	result := Result{
		Path:         path,
		Changed:      !bytes.Equal(before, formatted),
		RemovedLines: removed,
		Diff:         unifiedDiff(path, before, formatted),
	}
	if result.Changed && mode == cleanModeWrite {
		if err := os.WriteFile(path, formatted, info.Mode().Perm()); err != nil {
			return Result{}, fmt.Errorf("write cleaned Go file %s: %w", path, err)
		}
	}
	return result, nil
}

func removeAnnotationLines(data []byte) ([]byte, int) {
	lines := bytes.SplitAfter(data, []byte("\n"))
	filtered := make([][]byte, 0, len(lines))
	removed := 0
	for _, line := range lines {
		if isPtahSchemaAnnotationLine(line) {
			removed++
			continue
		}
		filtered = append(filtered, line)
	}
	return bytes.Join(filtered, nil), removed
}

func isPtahSchemaAnnotationLine(line []byte) bool {
	trimmed := strings.TrimSpace(string(line))
	return strings.HasPrefix(trimmed, "//migrator:schema:") ||
		strings.HasPrefix(trimmed, "//migrator:embedded")
}

func unifiedDiff(path string, before, after []byte) string {
	if bytes.Equal(before, after) {
		return ""
	}
	var builder strings.Builder
	builder.WriteString("--- " + path + "\n")
	builder.WriteString("+++ " + path + "\n")
	builder.WriteString("@@\n")
	beforeLines := strings.SplitAfter(string(before), "\n")
	afterLines := strings.SplitAfter(string(after), "\n")
	for _, line := range beforeLines {
		if line == "" {
			continue
		}
		if !containsLine(afterLines, line) {
			builder.WriteString("-" + line)
		}
	}
	for _, line := range afterLines {
		if line == "" {
			continue
		}
		if !containsLine(beforeLines, line) {
			builder.WriteString("+" + line)
		}
	}
	return builder.String()
}

func containsLine(lines []string, target string) bool {
	return slices.Contains(lines, target)
}
