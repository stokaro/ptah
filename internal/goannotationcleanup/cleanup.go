// Package goannotationcleanup removes Ptah schema annotations from Go source.
package goannotationcleanup

import (
	"bytes"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"github.com/stokaro/ptah/internal/annotationmeta"
	"github.com/stokaro/ptah/internal/annotationparse"
	"github.com/stokaro/ptah/internal/fsdurable"
	"github.com/stokaro/ptah/internal/goannotationsource"
	"github.com/stokaro/ptah/internal/pathguard"
)

var (
	// ErrRollbackConflict reports that cleanup could not safely restore an
	// original source because its committed file or parent directory changed.
	ErrRollbackConflict = errors.New("Go annotation cleanup rollback conflict")
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

type filePlan struct {
	result  Result
	source  goannotationsource.File
	before  []byte
	after   []byte
	removed []removedLine
}

type applyHooks struct {
	revalidate  func() error
	afterCommit func()
}

type openedPlan struct {
	plan       filePlan
	parent     *pathguard.OpenedDirectory
	targetName string
}

type stagedFile struct {
	name string
	info fs.FileInfo
}

type stagedPlan struct {
	opened         *openedPlan
	cleaned        stagedFile
	backup         stagedFile
	committed      bool
	preserveBackup bool
}

// Plan is an immutable set of validated annotation removals.
type Plan struct {
	snapshot *goannotationsource.Snapshot
	changes  []filePlan
}

// NewPlan plans annotation removals from one captured source view.
func NewPlan(snapshot *goannotationsource.Snapshot) (*Plan, error) {
	if snapshot == nil {
		return nil, errors.New("Go annotation source snapshot is nil")
	}
	cleanupPlan := &Plan{snapshot: snapshot}
	for _, source := range snapshot.Files() {
		file, err := planFile(source)
		if err != nil {
			return nil, err
		}
		if !file.result.Changed {
			continue
		}
		cleanupPlan.changes = append(cleanupPlan.changes, file)
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
	if err := p.snapshot.Revalidate(); err != nil {
		return fmt.Errorf("revalidate Go annotation sources before cleanup: %w", err)
	}
	return applyPlans(p.changes, applyHooks{
		revalidate:  p.snapshot.Revalidate,
		afterCommit: func() {},
	})
}

func planFile(source goannotationsource.File) (filePlan, error) {
	after, removed, err := removeAnnotationLines(source.Path, source.Contents)
	if err != nil {
		return filePlan{}, err
	}
	if len(removed) == 0 {
		return filePlan{
			result: Result{Path: source.Path},
			source: source,
			before: source.Contents,
			after:  after,
		}, nil
	}
	result := Result{
		Path:         source.Path,
		Changed:      !bytes.Equal(source.Contents, after),
		RemovedLines: len(removed),
		Diff:         unifiedRemovalDiff(source.Path, source.Contents, removed),
	}
	return filePlan{
		result:  result,
		source:  source,
		before:  source.Contents,
		after:   after,
		removed: removed,
	}, nil
}

func applyPlans(plans []filePlan, hooks applyHooks) error {
	opened, err := openPlans(plans)
	if err != nil {
		return err
	}
	staged, err := stagePlans(opened)
	if err != nil {
		return errors.Join(err, closeOpenedPlans(opened))
	}
	if err := hooks.revalidate(); err != nil {
		return finishApply(
			opened,
			staged,
			fmt.Errorf("revalidate Go annotation sources before cleanup commit: %w", err),
		)
	}
	if err := commitStagedPlans(staged, hooks.afterCommit); err != nil {
		return finishApply(opened, staged, err)
	}
	return finishApply(opened, staged, nil)
}

func finishApply(opened []openedPlan, staged []stagedPlan, applyErr error) error {
	finishErr := errors.Join(applyErr, cleanupStagedPlans(staged), closeOpenedPlans(opened))
	if finishErr == nil || !hasCommittedPlan(staged) || errors.Is(finishErr, fsdurable.ErrReplacementCommitted) {
		return finishErr
	}
	return fmt.Errorf("%w: %w", fsdurable.ErrReplacementCommitted, finishErr)
}

func hasCommittedPlan(staged []stagedPlan) bool {
	return slices.ContainsFunc(staged, func(plan stagedPlan) bool {
		return plan.committed
	})
}

func openPlans(plans []filePlan) ([]openedPlan, error) {
	opened := make([]openedPlan, 0, len(plans))
	parents := make(map[string]*pathguard.OpenedDirectory)
	for _, plan := range plans {
		parentPath := filepath.Clean(filepath.Dir(plan.result.Path))
		parent := parents[parentPath]
		openedParent := false
		if parent == nil {
			var err error
			parent, err = pathguard.OpenDirectory(parentPath)
			if err != nil {
				return nil, errors.Join(
					fmt.Errorf("open Go source parent for cleanup %s: %w", plan.result.Path, err),
					closeOpenedPlans(opened),
				)
			}
			parents[parentPath] = parent
			openedParent = true
		}
		current, err := openPlan(plan, parent)
		if err != nil {
			if openedParent {
				err = errors.Join(err, parent.Close())
			}
			return nil, errors.Join(err, closeOpenedPlans(opened))
		}
		opened = append(opened, current)
	}
	return opened, nil
}

func openPlan(plan filePlan, parent *pathguard.OpenedDirectory) (openedPlan, error) {
	targetName := filepath.Base(plan.result.Path)
	source, err := openValidatedPlan(parent, targetName, plan)
	if err != nil {
		return openedPlan{}, err
	}
	if err := source.Close(); err != nil {
		return openedPlan{}, fmt.Errorf("close validated Go source %s: %w", plan.result.Path, err)
	}
	return openedPlan{
		plan:       plan,
		parent:     parent,
		targetName: targetName,
	}, nil
}

func openValidatedPlan(
	parent *pathguard.OpenedDirectory,
	targetName string,
	plan filePlan,
) (*os.File, error) {
	entryInfo, err := parent.Lstat(targetName)
	if err != nil {
		return nil, fmt.Errorf("stat Go source before cleanup %s: %w", plan.result.Path, err)
	}
	if entryInfo.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("refuse to clean symlinked Go source %s", plan.result.Path)
	}
	if err := validatePlanInfo(entryInfo, plan); err != nil {
		return nil, err
	}
	file, err := parent.Open(targetName)
	if err != nil {
		return nil, fmt.Errorf("open Go source for cleanup %s: %w", plan.result.Path, err)
	}
	if err := validateOpenPlanEntry(parent, targetName, file, plan); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	return file, nil
}

func validatePlanInfo(info fs.FileInfo, plan filePlan) error {
	if !info.Mode().IsRegular() ||
		!plan.source.SameFile(info) ||
		info.Mode() != plan.source.Mode() {
		return fmt.Errorf("go source changed before cleanup: %s", plan.result.Path)
	}
	return nil
}

func validateOpenPlanEntry(
	parent *pathguard.OpenedDirectory,
	targetName string,
	file *os.File,
	plan filePlan,
) error {
	currentInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat opened Go source %s: %w", plan.result.Path, err)
	}
	if err := validatePlanInfo(currentInfo, plan); err != nil {
		return err
	}
	entryInfo, err := parent.Lstat(targetName)
	if err != nil {
		return fmt.Errorf("restat Go source before cleanup %s: %w", plan.result.Path, err)
	}
	if entryInfo.Mode()&os.ModeSymlink != 0 ||
		!os.SameFile(currentInfo, entryInfo) {
		return fmt.Errorf("go source changed before cleanup: %s", plan.result.Path)
	}
	if err := validatePlanContent(file, plan.before, plan.result.Path); err != nil {
		return err
	}
	finalInfo, statErr := file.Stat()
	finalEntryInfo, restatErr := parent.Lstat(targetName)
	if err := errors.Join(statErr, restatErr); err != nil {
		return fmt.Errorf("restat Go source after cleanup validation %s: %w", plan.result.Path, err)
	}
	if err := validatePlanInfo(finalInfo, plan); err != nil {
		return err
	}
	if finalEntryInfo.Mode()&os.ModeSymlink != 0 ||
		!os.SameFile(currentInfo, finalInfo) ||
		!os.SameFile(finalInfo, finalEntryInfo) ||
		finalEntryInfo.Mode() != plan.source.Mode() {
		return fmt.Errorf("go source changed before cleanup: %s", plan.result.Path)
	}
	return nil
}

func validatePlanPath(opened *openedPlan) error {
	file, err := openValidatedPlan(opened.parent, opened.targetName, opened.plan)
	if err != nil {
		return err
	}
	return file.Close()
}

func validatePlanContent(file *os.File, expected []byte, displayPath string) error {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek Go source before cleanup %s: %w", displayPath, err)
	}
	current, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("read Go source before cleanup %s: %w", displayPath, err)
	}
	if !bytes.Equal(current, expected) {
		return fmt.Errorf("go source changed before cleanup: %s", displayPath)
	}
	return nil
}

func stagePlans(opened []openedPlan) ([]stagedPlan, error) {
	staged := make([]stagedPlan, 0, len(opened))
	for i := range opened {
		stagedFile, err := stagePlan(&opened[i])
		if err != nil {
			return nil, errors.Join(err, cleanupStagedPlans(staged))
		}
		staged = append(staged, stagedFile)
	}
	return staged, nil
}

func stagePlan(opened *openedPlan) (stagedPlan, error) {
	backup, err := stageFile(opened, "backup", opened.plan.before)
	if err != nil {
		return stagedPlan{}, err
	}
	cleaned, err := stageFile(opened, "cleaned", opened.plan.after)
	if err != nil {
		return stagedPlan{}, errors.Join(err, removeStagedFile(opened.parent, &backup))
	}
	staged := stagedPlan{
		opened:  opened,
		cleaned: cleaned,
		backup:  backup,
	}
	if err := opened.parent.Sync(); err != nil {
		return stagedPlan{}, errors.Join(
			fmt.Errorf("sync staged cleanup files for %s: %w", opened.plan.result.Path, err),
			cleanupStagedPlans([]stagedPlan{staged}),
		)
	}
	return staged, nil
}

func stageFile(opened *openedPlan, kind string, data []byte) (stagedFile, error) {
	pattern := "." + opened.targetName + ".ptah-" + kind + "-*"
	file, name, err := opened.parent.CreateTemp(pattern)
	if err != nil {
		return stagedFile{}, fmt.Errorf(
			"create staged %s for %s: %w",
			kind,
			opened.plan.result.Path,
			err,
		)
	}
	staged := stagedFile{name: name}
	if _, err := file.Write(data); err != nil {
		return stagedFile{}, errors.Join(
			fmt.Errorf("write staged %s for %s: %w", kind, opened.plan.result.Path, err),
			file.Close(),
			removeStagedFile(opened.parent, &staged),
		)
	}
	if err := file.Sync(); err != nil {
		return stagedFile{}, errors.Join(
			fmt.Errorf("sync staged %s for %s: %w", kind, opened.plan.result.Path, err),
			file.Close(),
			removeStagedFile(opened.parent, &staged),
		)
	}
	info, err := file.Stat()
	if err != nil {
		return stagedFile{}, errors.Join(
			fmt.Errorf("stat staged %s for %s: %w", kind, opened.plan.result.Path, err),
			file.Close(),
			removeStagedFile(opened.parent, &staged),
		)
	}
	if err := file.Close(); err != nil {
		return stagedFile{}, errors.Join(
			fmt.Errorf("close staged %s for %s: %w", kind, opened.plan.result.Path, err),
			removeStagedFile(opened.parent, &staged),
		)
	}
	staged.info = info
	return staged, nil
}

func commitStagedPlans(staged []stagedPlan, afterCommit func()) error {
	for i := range staged {
		if err := staged[i].opened.parent.Revalidate(); err != nil {
			return errors.Join(
				fmt.Errorf("revalidate Go source parent before cleanup %s: %w", staged[i].opened.plan.result.Path, err),
				rollbackStagedPlans(staged[:i]),
			)
		}
		if err := validatePlanPath(staged[i].opened); err != nil {
			return errors.Join(err, rollbackStagedPlans(staged[:i]))
		}
		committed, err := commitStagedFile(&staged[i], &staged[i].cleaned)
		staged[i].committed = committed
		if committed {
			afterCommit()
		}
		if err != nil {
			rollbackEnd := i
			if committed {
				rollbackEnd++
			}
			return errors.Join(
				fmt.Errorf("commit cleaned Go source %s: %w", staged[i].opened.plan.result.Path, err),
				rollbackStagedPlans(staged[:rollbackEnd]),
			)
		}
	}
	return nil
}

func commitStagedFile(plan *stagedPlan, staged *stagedFile) (bool, error) {
	if err := plan.opened.parent.Revalidate(); err != nil {
		return false, err
	}
	replaceErr := plan.opened.parent.PublishFile(
		staged.name,
		plan.opened.targetName,
		staged.info,
		plan.opened.plan.source.Mode(),
	)
	committed := replaceErr == nil || errors.Is(replaceErr, fsdurable.ErrReplacementCommitted)
	if !committed {
		return false, replaceErr
	}
	staged.name = ""
	revalidateErr := plan.opened.parent.Revalidate()
	return true, replacementCommittedError(errors.Join(replaceErr, revalidateErr))
}

func replacementCommittedError(err error) error {
	if err == nil || errors.Is(err, fsdurable.ErrReplacementCommitted) {
		return err
	}
	return fmt.Errorf("%w: %w", fsdurable.ErrReplacementCommitted, err)
}

func rollbackStagedPlans(staged []stagedPlan) error {
	var rollbackErr error
	for i := range slices.Backward(staged) {
		if !staged[i].committed {
			continue
		}
		if err := staged[i].opened.parent.Revalidate(); err != nil {
			staged[i].preserveBackup = true
			rollbackErr = errors.Join(
				rollbackErr,
				rollbackConflictError(&staged[i], err),
			)
			continue
		}
		if err := validateCommittedPlan(&staged[i]); err != nil {
			staged[i].preserveBackup = true
			rollbackErr = errors.Join(
				rollbackErr,
				rollbackConflictError(&staged[i], err),
			)
			continue
		}
		committed, err := commitStagedFile(&staged[i], &staged[i].backup)
		if committed {
			staged[i].committed = false
		}
		if err == nil {
			continue
		}
		if !committed {
			staged[i].preserveBackup = true
		}
		rollbackErr = errors.Join(
			rollbackErr,
			fmt.Errorf(
				"restore Go source %s from %s: %w",
				staged[i].opened.plan.result.Path,
				stagedFilePath(staged[i].opened.parent, &staged[i].backup),
				err,
			),
		)
	}
	return rollbackErr
}

func validateCommittedPlan(staged *stagedPlan) error {
	entryInfo, err := staged.opened.parent.Lstat(staged.opened.targetName)
	if err != nil {
		return err
	}
	if !entryInfo.Mode().IsRegular() ||
		!os.SameFile(staged.cleaned.info, entryInfo) ||
		entryInfo.Mode() != staged.opened.plan.source.Mode() {
		return errors.New("committed cleaned source identity or mode changed")
	}
	file, err := staged.opened.parent.Open(staged.opened.targetName)
	if err != nil {
		return err
	}
	currentInfo, statErr := file.Stat()
	openedEntryInfo, restatErr := staged.opened.parent.Lstat(staged.opened.targetName)
	validationErr := errors.Join(statErr, restatErr)
	if validationErr == nil &&
		(!os.SameFile(staged.cleaned.info, currentInfo) ||
			!os.SameFile(currentInfo, openedEntryInfo)) {
		validationErr = errors.New("committed cleaned source identity changed")
	}
	if validationErr == nil {
		validationErr = validatePlanContent(
			file,
			staged.opened.plan.after,
			staged.opened.plan.result.Path,
		)
	}
	finalInfo, finalStatErr := file.Stat()
	finalEntryInfo, finalRestatErr := staged.opened.parent.Lstat(staged.opened.targetName)
	validationErr = errors.Join(validationErr, finalStatErr, finalRestatErr)
	if validationErr == nil &&
		(!finalInfo.Mode().IsRegular() ||
			finalInfo.Mode() != staged.opened.plan.source.Mode() ||
			finalEntryInfo.Mode() != staged.opened.plan.source.Mode() ||
			currentInfo.Size() != finalInfo.Size() ||
			!currentInfo.ModTime().Equal(finalInfo.ModTime()) ||
			!os.SameFile(currentInfo, finalInfo) ||
			!os.SameFile(finalInfo, finalEntryInfo)) {
		validationErr = errors.New("committed cleaned source changed during rollback validation")
	}
	return errors.Join(validationErr, file.Close())
}

func rollbackConflictError(staged *stagedPlan, cause error) error {
	return fmt.Errorf(
		"%w: refuse to restore %s because the committed cleaned source cannot be safely validated; preserved backup %s: %w",
		ErrRollbackConflict,
		staged.opened.plan.result.Path,
		stagedFilePath(staged.opened.parent, &staged.backup),
		cause,
	)
}

func cleanupStagedPlans(staged []stagedPlan) error {
	var cleanupErr error
	for i := range staged {
		cleanupErr = errors.Join(
			cleanupErr,
			removeStagedFile(staged[i].opened.parent, &staged[i].cleaned),
		)
		if staged[i].preserveBackup {
			preserveErr := finalizePreservedBackup(&staged[i])
			cleanupErr = errors.Join(cleanupErr, preserveErr)
			continue
		}
		cleanupErr = errors.Join(
			cleanupErr,
			removeStagedFile(staged[i].opened.parent, &staged[i].backup),
		)
	}
	return cleanupErr
}

func finalizePreservedBackup(staged *stagedPlan) error {
	return wrapStagedFileError(
		"finalize preserved cleanup backup",
		staged.opened.parent,
		&staged.backup,
		staged.opened.parent.FinalizeFile(
			staged.backup.name,
			staged.backup.info,
			staged.opened.plan.source.Mode(),
		),
	)
}

func removeStagedFile(parent *pathguard.OpenedDirectory, staged *stagedFile) error {
	if staged.name == "" {
		return nil
	}
	removeErr := parent.Remove(staged.name)
	if errors.Is(removeErr, os.ErrNotExist) {
		removeErr = nil
	}
	if removeErr == nil {
		staged.name = ""
	}
	syncErr := error(nil)
	if removeErr == nil {
		syncErr = parent.Sync()
	}
	return errors.Join(
		wrapStagedFileError("remove staged cleanup file", parent, staged, removeErr),
		syncErr,
	)
}

func wrapStagedFileError(
	action string,
	parent *pathguard.OpenedDirectory,
	staged *stagedFile,
	err error,
) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s %s: %w", action, stagedFilePath(parent, staged), err)
}

func stagedFilePath(parent *pathguard.OpenedDirectory, staged *stagedFile) string {
	if staged.name == "" {
		return filepath.Join(parent.Path(), "<committed>")
	}
	if err := parent.Revalidate(); err != nil {
		return fmt.Sprintf(
			"entry %q in the originally opened directory %q (the current path no longer identifies that directory)",
			staged.name,
			parent.Path(),
		)
	}
	return filepath.Join(parent.Path(), staged.name)
}

func closeOpenedPlans(opened []openedPlan) error {
	var closeErr error
	closed := make(map[*pathguard.OpenedDirectory]struct{})
	for i := range opened {
		if opened[i].parent == nil {
			continue
		}
		parent := opened[i].parent
		opened[i].parent = nil
		if _, ok := closed[parent]; ok {
			continue
		}
		closed[parent] = struct{}{}
		if err := parent.Close(); err != nil {
			closeErr = errors.Join(
				closeErr,
				fmt.Errorf("close Go source parent %s: %w", parent.Path(), err),
			)
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

// redactionMarker replaces a credential in display-only output. It matches the
// convention already used for connection strings in dbschema.
const redactionMarker = "***"

// redactSensitiveValues masks the value of every sensitive attribute in a
// source line, leaving the rest of the line byte-identical so the diff still
// shows the file, directive, and the other attributes.
//
// Matching is deliberately directive-INDEPENDENT. core/goschema builds a live
// role from a bare "//ptah:schema:role" prefix, which accepts spellings the
// directive recognizer rejects (a suffix, text before the "//", an exotic space
// after it). Gating redaction on recognition would print the credential for
// exactly those lines while Ptah still exported the role. A redactor has to be
// the widest matcher in the system, so this masks any sensitive attribute name
// on any line mentioning "ptah:". Over-masking is safe: the surface is
// display-only, and planning and destructive writes use the original bytes.
//
// The line is rescanned rather than reusing ranges collected during planning:
// those were computed against the comment text, whose offsets differ from the
// raw source line by the leading indentation. Masking is done by ValueRange
// offsets, not by matching the value, because a value may contain a quote.
func redactSensitiveValues(line string) string {
	if !strings.Contains(line, "ptah:") {
		return line
	}
	sensitive := annotationmeta.AllSensitiveAttributes()
	if len(sensitive) == 0 {
		return line
	}

	type span struct{ start, end int }
	var spans []span
	for _, attribute := range annotationparse.ScanAttributes(line) {
		if !sensitive[strings.ToLower(attribute.Name)] {
			continue
		}
		start, end := attribute.ValueRange.Start.Character, attribute.ValueRange.End.Character
		if start < 0 || end > len(line) || start >= end {
			continue
		}
		spans = append(spans, span{start: start, end: widenAmbiguousValue(line, start, end)})
	}
	if len(spans) == 0 {
		return line
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].start < spans[j].start })

	var builder strings.Builder
	cursor := 0
	for _, s := range spans {
		if s.start < cursor {
			continue
		}
		builder.WriteString(line[cursor:s.start])
		builder.WriteString(redactionMarker)
		cursor = s.end
	}
	builder.WriteString(line[cursor:])
	return builder.String()
}

// widenAmbiguousValue extends a masked span to the end of the line when the
// recognized value does not end at a token boundary.
//
// A password containing an unescaped quote, such as password="a"b", makes the
// attribute regex stop at the inner quote, so masking only the recognized span
// would print the remainder. The delimiters are ambiguous, so the safe reading
// is that everything after it may still be the secret.
func widenAmbiguousValue(line string, start, end int) int {
	if end >= len(line) {
		return end
	}
	switch line[end] {
	case ' ', '\t', '\r', '\n':
		return end
	}
	return end + len(strings.TrimRight(line[end:], "\r\n"))
}

func writeDiffLine(builder *strings.Builder, prefix byte, line string) {
	line = redactSensitiveValues(line)
	builder.WriteByte(prefix)
	builder.WriteString(line)
	if !strings.HasSuffix(line, "\n") {
		builder.WriteByte('\n')
		builder.WriteString("\\ No newline at end of file\n")
	}
}
