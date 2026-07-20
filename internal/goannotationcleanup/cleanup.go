// Package goannotationcleanup removes Ptah schema annotations from Go source.
package goannotationcleanup

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
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

type removedLine struct {
	number int
}

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
	if len(removed) == 0 {
		return Result{Path: path}, nil
	}
	result := Result{
		Path:         path,
		Changed:      !bytes.Equal(before, after),
		RemovedLines: len(removed),
		Diff:         unifiedRemovalDiff(path, before, removed),
	}
	if result.Changed && mode == cleanModeWrite {
		if err := os.WriteFile(path, after, info.Mode().Perm()); err != nil {
			return Result{}, fmt.Errorf("write cleaned Go file %s: %w", path, err)
		}
	}
	return result, nil
}

func removeAnnotationLines(data []byte) ([]byte, []removedLine) {
	lines := bytes.SplitAfter(data, []byte("\n"))
	filtered := make([][]byte, 0, len(lines))
	removed := make([]removedLine, 0)
	for i, line := range lines {
		if isPtahSchemaAnnotationLine(line) {
			removed = append(removed, removedLine{
				number: i + 1,
			})
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

func unifiedRemovalDiff(path string, before []byte, removed []removedLine) string {
	if len(removed) == 0 {
		return ""
	}
	var builder strings.Builder
	builder.WriteString("--- " + path + "\n")
	builder.WriteString("+++ " + path + "\n")

	lines := splitLines(before)
	removedSet := make(map[int]struct{}, len(removed))
	for _, line := range removed {
		removedSet[line.number] = struct{}{}
	}

	const contextLines = 2
	removedBefore := 0
	for i := 0; i < len(removed); {
		oldStart := max(1, removed[i].number-contextLines)
		oldEnd := min(len(lines), removed[i].number+contextLines)
		j := i + 1
		for j < len(removed) && removed[j].number <= oldEnd+contextLines+1 {
			oldEnd = min(len(lines), max(oldEnd, removed[j].number+contextLines))
			j++
		}

		oldCount := oldEnd - oldStart + 1
		removedInHunk := countRemovedInRange(removed[i:j], oldStart, oldEnd)
		newStart := max(1, oldStart-removedBefore)
		newCount := oldCount - removedInHunk
		fmt.Fprintf(&builder, "@@ -%s +%s @@\n", diffRange(oldStart, oldCount), diffRange(newStart, newCount))
		for lineNumber := oldStart; lineNumber <= oldEnd; lineNumber++ {
			line := lines[lineNumber-1]
			if _, ok := removedSet[lineNumber]; ok {
				writeDiffLine(&builder, '-', line)
				continue
			}
			writeDiffLine(&builder, ' ', line)
		}

		removedBefore += j - i
		i = j
	}
	return builder.String()
}

func splitLines(data []byte) []string {
	lines := strings.SplitAfter(string(data), "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	return lines
}

func countRemovedInRange(lines []removedLine, start, end int) int {
	count := 0
	for _, line := range lines {
		if line.number >= start && line.number <= end {
			count++
		}
	}
	return count
}

func diffRange(start, count int) string {
	if count == 1 {
		return fmt.Sprintf("%d", start)
	}
	return fmt.Sprintf("%d,%d", start, count)
}

func writeDiffLine(builder *strings.Builder, prefix byte, line string) {
	builder.WriteByte(prefix)
	builder.WriteString(line)
	if !strings.HasSuffix(line, "\n") {
		builder.WriteByte('\n')
	}
}
