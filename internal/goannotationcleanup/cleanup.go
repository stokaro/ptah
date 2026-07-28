// Package goannotationcleanup removes Ptah schema annotations from Go source.
package goannotationcleanup

import (
	"bytes"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/stokaro/ptah/internal/annotationmeta"
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

type removedLine struct {
	number int
}

type filePlan struct {
	result Result
	info   os.FileInfo
	before []byte
	after  []byte
}

// CleanDir removes Ptah schema annotations from Go files under RootDir.
func CleanDir(opts Options) ([]Result, error) {
	root := opts.RootDir
	if root == "" {
		root = "."
	}
	root = filepath.Clean(root)
	var plans []filePlan
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
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("refuse to clean symlinked Go source %s", path)
		}
		plan, err := planFile(path)
		if err != nil {
			return err
		}
		if plan.result.Changed {
			plans = append(plans, plan)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !opts.DryRun && !opts.Diff {
		if err := applyPlans(plans); err != nil {
			return nil, err
		}
	}
	results := make([]Result, len(plans))
	for i := range plans {
		result := plans[i].result
		if !opts.Diff {
			result.Diff = ""
		}
		results[i] = result
	}
	return results, nil
}

func planFile(path string) (filePlan, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return filePlan{}, fmt.Errorf("stat %s: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return filePlan{}, fmt.Errorf("refuse to clean non-regular Go source %s", path)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		return filePlan{}, fmt.Errorf("read %s: %w", path, err)
	}
	after, removed, err := removeAnnotationLines(path, before)
	if err != nil {
		return filePlan{}, err
	}
	if len(removed) == 0 {
		return filePlan{
			result: Result{Path: path},
			info:   info,
			before: before,
			after:  after,
		}, nil
	}
	result := Result{
		Path:         path,
		Changed:      !bytes.Equal(before, after),
		RemovedLines: len(removed),
		Diff:         unifiedRemovalDiff(path, before, removed),
	}
	return filePlan{
		result: result,
		info:   info,
		before: before,
		after:  after,
	}, nil
}

func applyPlans(plans []filePlan) error {
	files := make([]*os.File, 0, len(plans))
	for _, plan := range plans {
		file, err := openValidatedPlan(plan)
		if err != nil {
			return errors.Join(err, closeFiles(files))
		}
		files = append(files, file)
	}

	for i, file := range files {
		if err := writePlan(file, plans[i]); err != nil {
			return errors.Join(err, closeFiles(files))
		}
	}
	return closeFiles(files)
}

func openValidatedPlan(plan filePlan) (*os.File, error) {
	info, err := os.Lstat(plan.result.Path)
	if err != nil {
		return nil, fmt.Errorf("stat Go source before cleanup %s: %w", plan.result.Path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("refuse to clean symlinked Go source %s", plan.result.Path)
	}

	file, err := os.OpenFile(plan.result.Path, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("open Go source for cleanup %s: %w", plan.result.Path, err)
	}
	currentInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("stat opened Go source %s: %w", plan.result.Path, err)
	}
	if !os.SameFile(plan.info, currentInfo) {
		_ = file.Close()
		return nil, fmt.Errorf("go source changed before cleanup: %s", plan.result.Path)
	}
	if err := validatePlanContent(file, plan); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func validatePlanContent(file *os.File, plan filePlan) error {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek Go source before cleanup %s: %w", plan.result.Path, err)
	}
	current, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("read Go source before cleanup %s: %w", plan.result.Path, err)
	}
	if !bytes.Equal(current, plan.before) {
		return fmt.Errorf("go source changed before cleanup: %s", plan.result.Path)
	}
	return nil
}

func writePlan(file *os.File, plan filePlan) error {
	if err := validatePlanContent(file, plan); err != nil {
		return err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek Go source for cleanup %s: %w", plan.result.Path, err)
	}
	if err := file.Truncate(0); err != nil {
		return fmt.Errorf("truncate Go source for cleanup %s: %w", plan.result.Path, err)
	}
	if _, err := file.Write(plan.after); err != nil {
		return fmt.Errorf("write cleaned Go source %s: %w", plan.result.Path, err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync cleaned Go source %s: %w", plan.result.Path, err)
	}
	return nil
}

func closeFiles(files []*os.File) error {
	var closeErr error
	for _, file := range files {
		if err := file.Close(); err != nil {
			closeErr = errors.Join(closeErr, fmt.Errorf("close cleaned Go source %s: %w", file.Name(), err))
		}
	}
	return closeErr
}

func removeAnnotationLines(path string, data []byte) ([]byte, []removedLine, error) {
	lines := bytes.SplitAfter(data, []byte("\n"))
	lineNumbers, err := annotationLineNumbers(path, data, lines)
	if err != nil {
		return nil, nil, err
	}
	filtered := make([][]byte, 0, len(lines))
	removed := make([]removedLine, 0)
	for i, line := range lines {
		lineNumber := i + 1
		if _, ok := lineNumbers[lineNumber]; ok {
			removed = append(removed, removedLine{
				number: lineNumber,
			})
			continue
		}
		filtered = append(filtered, line)
	}
	return bytes.Join(filtered, nil), removed, nil
}

func annotationLineNumbers(path string, data []byte, lines [][]byte) (map[int]struct{}, error) {
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, path, data, parser.ParseComments|parser.SkipObjectResolution)
	if err != nil {
		return nil, fmt.Errorf("parse Go source %s: %w", path, err)
	}

	lineNumbers := make(map[int]struct{})
	for _, group := range file.Comments {
		for _, comment := range group.List {
			lineNumber := fileSet.PositionFor(comment.Pos(), false).Line
			if !isPtahSchemaAnnotationComment(comment.Text) ||
				lineNumber < 1 ||
				lineNumber > len(lines) ||
				strings.TrimSpace(string(lines[lineNumber-1])) != strings.TrimSpace(comment.Text) {
				continue
			}
			lineNumbers[lineNumber] = struct{}{}
		}
	}
	return lineNumbers, nil
}

func isPtahSchemaAnnotationComment(comment string) bool {
	_, ok := annotationmeta.MatchCommentDirective(comment)
	return ok
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
		builder.WriteString("\\ No newline at end of file\n")
	}
}
