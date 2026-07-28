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
	"slices"
	"strings"

	"github.com/stokaro/ptah/internal/annotationmeta"
	"github.com/stokaro/ptah/internal/annotationparse"
)

// Result describes cleanup changes for one file.
type Result struct {
	Path         string
	Changed      bool
	RemovedLines int
	Diff         string
}

// Annotation identifies one planned Ptah annotation removal.
type Annotation struct {
	Path       string
	Line       int
	Directive  string
	Attributes []string
}

type removedLine struct {
	number     int
	annotation Annotation
}

type sourceFile struct {
	path string
	info os.FileInfo
}

type filePlan struct {
	result  Result
	info    os.FileInfo
	before  []byte
	after   []byte
	removed []removedLine
}

type stagedPlan struct {
	plan           filePlan
	cleanedPath    string
	backupPath     string
	preserveBackup bool
}

// Plan is an immutable set of validated annotation removals.
type Plan struct {
	sources []sourceFile
	changes []filePlan
}

// PlanDir validates Go sources under root and plans annotation removals without
// modifying any files.
func PlanDir(root string) (*Plan, error) {
	if root == "" {
		root = "."
	}
	root = filepath.Clean(root)
	cleanupPlan := &Plan{}
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
		file, err := planFile(path)
		if err != nil {
			return err
		}
		cleanupPlan.sources = append(cleanupPlan.sources, sourceFile{
			path: file.result.Path,
			info: file.info,
		})
		if file.result.Changed {
			cleanupPlan.changes = append(cleanupPlan.changes, file)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return cleanupPlan, nil
}

// Results returns planned changes without unified diffs.
func (p *Plan) Results() []Result {
	results := p.DiffResults()
	for i := range results {
		results[i].Diff = ""
	}
	return results
}

// DiffResults returns planned changes with unified diffs.
func (p *Plan) DiffResults() []Result {
	results := make([]Result, len(p.changes))
	for i := range p.changes {
		results[i] = p.changes[i].result
	}
	return results
}

// Annotations returns the validated annotations represented by the plan.
func (p *Plan) Annotations() []Annotation {
	var annotations []Annotation
	for _, change := range p.changes {
		for _, removed := range change.removed {
			annotation := removed.annotation
			annotation.Attributes = append([]string(nil), annotation.Attributes...)
			annotations = append(annotations, annotation)
		}
	}
	return annotations
}

// Apply commits the validated annotation-removal plan.
func (p *Plan) Apply() error {
	return applyPlans(p.changes)
}

// SourceAlias returns the planned Go source aliased by path, or an empty string
// when path does not refer to any source in the plan.
func (p *Plan) SourceAlias(path string) (string, error) {
	path = filepath.Clean(path)
	for _, source := range p.sources {
		if path == source.path {
			return source.path, nil
		}
	}

	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("stat potential Go source alias %s: %w", path, err)
	}
	for _, source := range p.sources {
		if os.SameFile(source.info, info) {
			return source.path, nil
		}
	}
	return "", nil
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
		result:  result,
		info:    info,
		before:  before,
		after:   after,
		removed: removed,
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

	staged, err := stagePlans(plans)
	if err != nil {
		return errors.Join(err, closeFiles(files))
	}
	for i, file := range files {
		if err := validateOpenPlan(file, plans[i]); err != nil {
			return errors.Join(err, closeFiles(files), cleanupStagedPlans(staged))
		}
	}
	if err := closeFiles(files); err != nil {
		return errors.Join(err, cleanupStagedPlans(staged))
	}
	if err := commitStagedPlans(staged); err != nil {
		return errors.Join(err, cleanupStagedPlans(staged))
	}
	return cleanupStagedPlans(staged)
}

func openValidatedPlan(plan filePlan) (*os.File, error) {
	if err := validatePlanPath(plan); err != nil {
		return nil, err
	}
	file, err := os.Open(plan.result.Path)
	if err != nil {
		return nil, fmt.Errorf("open Go source for cleanup %s: %w", plan.result.Path, err)
	}
	if err := validateOpenPlan(file, plan); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func validatePlanPath(plan filePlan) error {
	info, err := os.Lstat(plan.result.Path)
	if err != nil {
		return fmt.Errorf("stat Go source before cleanup %s: %w", plan.result.Path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("refuse to clean symlinked Go source %s", plan.result.Path)
	}
	if !info.Mode().IsRegular() || !os.SameFile(plan.info, info) {
		return fmt.Errorf("go source changed before cleanup: %s", plan.result.Path)
	}
	current, err := os.ReadFile(plan.result.Path)
	if err != nil {
		return fmt.Errorf("read Go source before cleanup %s: %w", plan.result.Path, err)
	}
	if !bytes.Equal(current, plan.before) {
		return fmt.Errorf("go source changed before cleanup: %s", plan.result.Path)
	}
	return nil
}

func validateOpenPlan(file *os.File, plan filePlan) error {
	currentInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat opened Go source %s: %w", plan.result.Path, err)
	}
	if !os.SameFile(plan.info, currentInfo) {
		return fmt.Errorf("go source changed before cleanup: %s", plan.result.Path)
	}
	return validatePlanContent(file, plan)
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

func stagePlans(plans []filePlan) ([]stagedPlan, error) {
	staged := make([]stagedPlan, 0, len(plans))
	for _, plan := range plans {
		stagedFile, err := stagePlan(plan)
		if err != nil {
			return nil, errors.Join(err, cleanupStagedPlans(staged))
		}
		staged = append(staged, stagedFile)
	}
	return staged, nil
}

func stagePlan(plan filePlan) (stagedPlan, error) {
	backupPath, err := stageFile(plan, "backup", plan.before)
	if err != nil {
		return stagedPlan{}, err
	}
	cleanedPath, err := stageFile(plan, "cleaned", plan.after)
	if err != nil {
		return stagedPlan{}, errors.Join(err, removeStagedFile(backupPath))
	}
	return stagedPlan{
		plan:        plan,
		cleanedPath: cleanedPath,
		backupPath:  backupPath,
	}, nil
}

func stageFile(plan filePlan, kind string, data []byte) (string, error) {
	dir := filepath.Dir(plan.result.Path)
	pattern := "." + filepath.Base(plan.result.Path) + ".ptah-" + kind + "-*"
	file, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return "", fmt.Errorf("create staged %s for %s: %w", kind, plan.result.Path, err)
	}
	path := file.Name()
	if err := file.Chmod(plan.info.Mode()); err != nil {
		_ = file.Close()
		return "", errors.Join(
			fmt.Errorf("set staged %s mode for %s: %w", kind, plan.result.Path, err),
			removeStagedFile(path),
		)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return "", errors.Join(
			fmt.Errorf("write staged %s for %s: %w", kind, plan.result.Path, err),
			removeStagedFile(path),
		)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return "", errors.Join(
			fmt.Errorf("sync staged %s for %s: %w", kind, plan.result.Path, err),
			removeStagedFile(path),
		)
	}
	if err := file.Close(); err != nil {
		return "", errors.Join(
			fmt.Errorf("close staged %s for %s: %w", kind, plan.result.Path, err),
			removeStagedFile(path),
		)
	}
	return path, nil
}

func commitStagedPlans(staged []stagedPlan) error {
	for i := range staged {
		if err := validatePlanPath(staged[i].plan); err != nil {
			return errors.Join(err, rollbackStagedPlans(staged[:i]))
		}
		if err := replaceFile(staged[i].cleanedPath, staged[i].plan.result.Path); err != nil {
			return errors.Join(
				fmt.Errorf("commit cleaned Go source %s: %w", staged[i].plan.result.Path, err),
				rollbackStagedPlans(staged[:i]),
			)
		}
		staged[i].cleanedPath = ""
	}
	return nil
}

func rollbackStagedPlans(staged []stagedPlan) error {
	var rollbackErr error
	for i := range slices.Backward(staged) {
		if err := replaceFile(staged[i].backupPath, staged[i].plan.result.Path); err != nil {
			staged[i].preserveBackup = true
			rollbackErr = errors.Join(
				rollbackErr,
				fmt.Errorf(
					"restore Go source %s from %s: %w",
					staged[i].plan.result.Path,
					staged[i].backupPath,
					err,
				),
			)
			continue
		}
		staged[i].backupPath = ""
	}
	return rollbackErr
}

func cleanupStagedPlans(staged []stagedPlan) error {
	var cleanupErr error
	for _, stagedFile := range staged {
		cleanupErr = errors.Join(cleanupErr, removeStagedFile(stagedFile.cleanedPath))
		if !stagedFile.preserveBackup {
			cleanupErr = errors.Join(cleanupErr, removeStagedFile(stagedFile.backupPath))
		}
	}
	return cleanupErr
}

func removeStagedFile(path string) error {
	if path == "" {
		return nil
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove staged cleanup file %s: %w", path, err)
	}
	return nil
}

func closeFiles(files []*os.File) error {
	var closeErr error
	for _, file := range files {
		if err := file.Close(); err != nil {
			closeErr = errors.Join(closeErr, fmt.Errorf("close Go source %s: %w", file.Name(), err))
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
		if annotation, ok := lineNumbers[lineNumber]; ok {
			removed = append(removed, removedLine{
				number:     lineNumber,
				annotation: annotation,
			})
			continue
		}
		filtered = append(filtered, line)
	}
	return bytes.Join(filtered, nil), removed, nil
}

func annotationLineNumbers(path string, data []byte, lines [][]byte) (map[int]Annotation, error) {
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, path, data, parser.ParseComments|parser.SkipObjectResolution)
	if err != nil {
		return nil, fmt.Errorf("parse Go source %s: %w", path, err)
	}

	lineNumbers := make(map[int]Annotation)
	for _, group := range file.Comments {
		for _, comment := range group.List {
			lineNumber := fileSet.PositionFor(comment.Pos(), false).Line
			directive, ok := annotationmeta.MatchCommentDirective(comment.Text)
			if !ok || lineNumber < 1 ||
				lineNumber > len(lines) ||
				strings.TrimSpace(string(lines[lineNumber-1])) != strings.TrimSpace(comment.Text) {
				continue
			}
			lineNumbers[lineNumber] = Annotation{
				Path:       path,
				Line:       lineNumber,
				Directive:  directive.Name,
				Attributes: annotationAttributes(comment.Text),
			}
		}
	}
	return lineNumbers, nil
}

func annotationAttributes(comment string) []string {
	annotations := annotationparse.Scan(comment)
	if len(annotations) == 0 {
		return nil
	}
	attributes := make([]string, len(annotations[0].Attributes))
	for i, attribute := range annotations[0].Attributes {
		attributes[i] = attribute.Name
	}
	return attributes
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
