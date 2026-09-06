package agentpatch

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"time"

	"ptah.run/internal/agentdiag"
	"ptah.run/internal/agentgate"
	"ptah.run/internal/agentpolicy"
	"ptah.run/internal/agentworkspace"
	"ptah.run/internal/atlasmigrate"
	"ptah.run/internal/fsdurable"
	"ptah.run/internal/migrateops"
	"ptah.run/internal/pathguard"
	"ptah.run/migration/migrationfile"
)

// createdFileMode is the mode a newly created artifact gets.
//
// It matches internal/atlasmigrate's published migration files rather than
// being chosen here, because a migration written by an agent and one written by
// `ptah migrations generate` should not be distinguishable by their permissions.
// An update preserves the mode the file already had.
const createdFileMode fs.FileMode = 0o644

// lockTimeout bounds the wait for the migration directory lock. It is generous
// because the lock is held for the length of another Ptah command, and short
// enough that a stuck holder surfaces as an error rather than as a hang.
const lockTimeout = 30 * time.Second

// Verifier runs the gates. It is an interface so the applier depends on the
// checking rather than on one implementation of it, and so a test can hold the
// gates still while it exercises the write-and-undo path.
type Verifier interface {
	Run(ctx context.Context, scope *agentworkspace.Scope) (agentgate.Report, error)
}

// Result is what applying a plan did.
type Result struct {
	PatchID string                    `json:"patch_id"`
	Class   agentpolicy.ArtifactClass `json:"artifact"`
	// BaseDigest is what the artifact held before.
	BaseDigest string `json:"base_digest"`
	// ResultDigest is what it holds now, measured rather than predicted.
	ResultDigest string `json:"result_digest"`
	// ProjectedDigest is what the plan predicted, present when it differs --
	// which it does whenever the integrity file was refreshed, because the
	// prediction is made before that write.
	ProjectedDigest string       `json:"projected_digest,omitempty"`
	Files           []FileChange `json:"files"`
	// IntegrityRefreshed reports that the artifact's checksum file was rewritten
	// as part of the apply.
	IntegrityRefreshed bool `json:"integrity_refreshed"`
	// Baseline is what the gates said before the write, and Verification what
	// they said after. Both are reported: a caller comparing them itself is how
	// a pre-existing problem stops being attributed to this patch.
	Baseline     agentgate.Report `json:"baseline"`
	Verification agentgate.Report `json:"verification"`
	// Introduced and Resolved are the difference, computed once here so every
	// surface reports the same difference.
	Introduced []agentgate.Diagnostic `json:"introduced"`
	Resolved   []agentgate.Diagnostic `json:"resolved"`
	// RolledBack reports that the write was undone. When it is true the artifact
	// holds what it held before and ResultDigest equals BaseDigest.
	RolledBack bool `json:"rolled_back"`
	// RollbackFailure names an undo that could not complete, which is the one
	// state a caller must not ignore: the artifact is neither the old content
	// nor the new.
	RollbackFailure string `json:"rollback_failure,omitempty"`
}

// Apply writes the plan, verifies it, and undoes it if verification found
// something the plan introduced.
//
// The order is deliberate and is the only order that answers the question. A
// gate that ran before the write would be checking content that is not what
// lands; a gate whose failure left the write in place would make "verification
// is mandatory" a description of an event rather than of an outcome.
func Apply(ctx context.Context, plan *Plan, verifier Verifier) (*Result, error) {
	if plan.patch.ExpectedDigest == "" {
		return nil, fmt.Errorf(
			"%w: apply requires the artifact digest the patch was composed against",
			ErrInvalidPatch)
	}
	if verifier == nil {
		return nil, errors.New("apply requires a verifier: an unverified write is not this operation")
	}

	var result *Result
	var applyErr error
	run := func(ctx context.Context) error {
		result, applyErr = applyLocked(ctx, plan, verifier)
		return applyErr
	}

	if plan.Class() == agentpolicy.ClassMigrations {
		// The migration directory has a cross-process lock that every Ptah verb
		// touching it honors. Taking it here is what keeps a patch from racing
		// `ptah migrations generate` running in another terminal; the digest
		// check alone would report the race, and holding the lock avoids it.
		lockErr := atlasmigrate.WithMigrationDirectoryLock(ctx, plan.scope.Path(), lockTimeout, run)
		if applyErr == nil && lockErr != nil {
			return nil, lockErr
		}
		return result, applyErr
	}
	_ = run(ctx)
	return result, applyErr
}

// applyLocked is the whole transaction, with whatever lock the class needs
// already held.
func applyLocked(ctx context.Context, plan *Plan, verifier Verifier) (*Result, error) {
	if err := plan.scope.Revalidate(); err != nil {
		return nil, err
	}
	if err := confirmBaseDigest(plan); err != nil {
		return nil, err
	}

	baseline, err := verifier.Run(ctx, plan.scope)
	if err != nil {
		return nil, agentdiag.Errorf(agentdiag.CodeVerificationUnavailable,
			"baseline verification: %w", err)
	}

	undo, writeErr := writeFiles(plan)
	if writeErr != nil {
		return nil, errors.Join(writeErr, rollback(plan, undo, nil))
	}

	// What the checksum files hold is recorded before they are rewritten,
	// because an undo has to put them back rather than derive them again. See
	// [snapshotIntegrity].
	integrity, err := snapshotIntegrity(plan)
	if err != nil {
		return nil, errors.Join(err, rollback(plan, undo, nil))
	}

	refreshed, err := refreshIntegrity(plan)
	if err != nil {
		return nil, errors.Join(err, rollback(plan, undo, integrity))
	}

	verification, err := verifier.Run(ctx, plan.scope)
	if err != nil {
		return nil, errors.Join(
			agentdiag.Errorf(agentdiag.CodeVerificationUnavailable, "verification: %w", err),
			rollback(plan, undo, integrity))
	}

	result := &Result{
		PatchID:            plan.id,
		Class:              plan.patch.Class,
		BaseDigest:         plan.baseDigest,
		Files:              plan.Files(),
		IntegrityRefreshed: refreshed,
		Baseline:           baseline,
		Verification:       verification,
		Introduced:         verification.Introduced(baseline),
		Resolved:           verification.Resolved(baseline),
	}
	if introduced := errorsAmong(result.Introduced); len(introduced) > 0 {
		return undoAndReport(plan, undo, integrity, result, introduced)
	}

	measured, err := plan.scope.Digest()
	if err != nil {
		return nil, errors.Join(err, rollback(plan, undo, integrity))
	}
	result.ResultDigest = measured
	if measured != plan.resultDigest {
		result.ProjectedDigest = plan.resultDigest
	}
	return result, nil
}

// confirmBaseDigest re-reads the artifact and refuses a plan whose base no
// longer describes it.
//
// The plan already checked this. It is checked again because the window between
// planning and applying is exactly where a preview-then-approve workflow puts a
// human, and a directory somebody edited in that window is #1487's scenario 7.
func confirmBaseDigest(plan *Plan) error {
	current, err := plan.scope.Digest()
	if err != nil {
		return err
	}
	if current != plan.baseDigest {
		return fmt.Errorf(
			"%w: the %s artifact was %s when the patch was previewed and is %s now; "+
				"re-read it and compose a new patch",
			ErrDigestMismatch, plan.patch.Class, plan.baseDigest, current)
	}
	return nil
}

// undoStep restores one file to what it was.
type undoStep struct {
	file FileChange
}

// writeFiles publishes every change, returning the steps that would undo what
// it managed to publish.
//
// The undo list is returned even on failure, and it is why the signature is
// shaped this way rather than returning an error alone: a partial batch is
// exactly when the caller needs to know which half happened.
func writeFiles(plan *Plan) ([]undoStep, error) {
	done := make([]undoStep, 0, len(plan.files))
	for _, file := range plan.files {
		if err := writeOne(plan, file); err != nil {
			return done, agentdiag.Errorf(agentdiag.CodeWriteFailed,
				"%s %s: %w", file.Operation, file.Path, err)
		}
		done = append(done, undoStep{file: file})
	}
	if err := plan.scope.Directory().Sync(); err != nil {
		return done, agentdiag.Wrap(agentdiag.CodeWriteFailed, err)
	}
	return done, nil
}

// writeOne publishes a single change through the scope's bound handle.
func writeOne(plan *Plan, file FileChange) error {
	parent, base, err := plan.scope.Parent(file.Path)
	if err != nil {
		return err
	}
	defer closeParent(plan, parent)

	if file.Operation == Delete {
		return parent.Remove(base)
	}
	return publish(parent, base, file)
}

// closeParent releases a nested directory handle, leaving the scope's own
// handle open.
func closeParent(plan *Plan, parent *pathguard.OpenedDirectory) {
	if parent == plan.scope.Directory() {
		return
	}
	_ = parent.Close()
}

// publish stages the content beside its destination and commits it
// conditionally.
//
// The staging file is created inside the destination's own directory because
// the conditional renames are same-directory operations, and it is removed on
// every path that does not publish it -- an abandoned staged file would be a
// name in the artifact directory that the next digest sees.
func publish(parent *pathguard.OpenedDirectory, base string, file FileChange) (err error) {
	staged, stagedName, err := parent.CreateTemp(".ptah-agent-*")
	if err != nil {
		return err
	}
	published := false
	defer func() {
		if published {
			return
		}
		err = errors.Join(err, parent.Remove(stagedName))
	}()

	if _, err = staged.Write(file.content); err != nil {
		return errors.Join(err, staged.Close())
	}
	if err = staged.Sync(); err != nil {
		return errors.Join(err, staged.Close())
	}
	info, err := staged.Stat()
	if err != nil {
		return errors.Join(err, staged.Close())
	}
	if err = staged.Close(); err != nil {
		return err
	}

	if err = parent.PublishFile(stagedName, base, info, modeFor(file), destinationFor(file)); err != nil {
		// A commit that reported ErrReplacementCommitted did happen, so the
		// staged name is gone and removing it would report a second, misleading
		// error.
		published = errors.Is(err, fsdurable.ErrReplacementCommitted)
		return err
	}
	published = true
	return nil
}

// modeFor is the mode the published file ends up with: the one it already had
// for an update, and the standard artifact mode for a creation.
func modeFor(file FileChange) fs.FileMode {
	if file.Operation == Update && file.beforeMode != 0 {
		return file.beforeMode.Perm()
	}
	return createdFileMode
}

// destinationFor states what the commit expects to find, which is what makes
// the write conditional.
func destinationFor(file FileChange) fsdurable.Destination {
	if file.Operation == Update {
		return fsdurable.ExpectFile(file.beforeInfo)
	}
	return fsdurable.ExpectAbsent()
}

// refreshIntegrity rewrites the artifact's checksum file where the class has
// one.
//
// It is not optional and it is not the caller's to remember: a migration
// directory whose files changed and whose checksum did not is a directory every
// executing Ptah verb refuses, so a patch that skipped this would produce a
// repository that looks written and cannot be used.
func refreshIntegrity(plan *Plan) (bool, error) {
	if plan.patch.Class != agentpolicy.ClassMigrations {
		return false, nil
	}
	if _, err := migrateops.Rehash(plan.scope.Path(), migrationfile.DirFormatAuto); err != nil {
		return false, agentdiag.Errorf(agentdiag.CodeWriteFailed,
			"refresh migration integrity: %w", err)
	}
	return true, nil
}

// undoAndReport rolls the patch back and returns the result describing why.
func undoAndReport(
	plan *Plan,
	undo []undoStep,
	integrity []integrityFile,
	result *Result,
	introduced []agentgate.Diagnostic,
) (*Result, error) {
	result.RolledBack = true
	// The checksum files are restored by the rollback, from what they held
	// before the apply. Rehashing them here instead is what left a file behind
	// on a directory that had none (stokaro/ptah#2066).
	if err := rollback(plan, undo, integrity); err != nil {
		result.RollbackFailure = err.Error()
	}
	if measured, err := plan.scope.Digest(); err == nil {
		result.ResultDigest = measured
	}
	return result, fmt.Errorf("%w: %s", ErrGateFailed, describe(introduced))
}

// describe names the first few introduced errors, so the message a model reads
// is actionable without being the whole report.
func describe(diagnostics []agentgate.Diagnostic) string {
	const shown = 3
	parts := make([]string, 0, shown)
	for index, diagnostic := range diagnostics {
		if index == shown {
			parts = append(parts, fmt.Sprintf("and %d more", len(diagnostics)-shown))
			break
		}
		parts = append(parts, fmt.Sprintf("%s: %s", diagnostic.Gate, diagnostic.Message))
	}
	return strings.Join(parts, "; ")
}

// errorsAmong keeps the error-severity diagnostics.
func errorsAmong(diagnostics []agentgate.Diagnostic) []agentgate.Diagnostic {
	kept := make([]agentgate.Diagnostic, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		if diagnostic.Severity == agentgate.SeverityError {
			kept = append(kept, diagnostic)
		}
	}
	return kept
}

// rollback restores every published change, most recent first.
//
// Every step is attempted even after one fails, because stopping at the first
// failure would leave the rest of the batch applied for no reason. The joined
// error is what the caller reports as a rollback failure.
func rollback(plan *Plan, undo []undoStep, integrity []integrityFile) error {
	errs := make([]error, 0, len(undo)+2)
	for _, step := range slices.Backward(undo) {
		errs = append(errs, restore(plan, step.file))
	}
	errs = append(errs, restoreIntegrity(plan, integrity))
	errs = append(errs, plan.scope.Directory().Sync())
	return errors.Join(errs...)
}

// integrityFile is one checksum file's state before the apply, including the
// state of not being there.
type integrityFile struct {
	name    string
	content []byte
	existed bool
}

// snapshotIntegrity records what the artifact's checksum files hold, so an undo
// can put them back instead of deriving them again.
//
// Deriving them again is not the same thing, and the difference is a file left
// behind. Rehash writes a checksum file for the directory it finds, so on a
// migration directory that had none -- a project's first patch -- the undo
// CREATED one. The apply then reported `rolled_back: true` beside a
// ResultDigest that no longer equalled BaseDigest, which is the invariant
// Result documents, and the caller was left holding a digest that had gone
// stale through an operation that said it changed nothing (stokaro/ptah#2066).
func snapshotIntegrity(plan *Plan) ([]integrityFile, error) {
	names := managedFiles[plan.patch.Class]
	files := make([]integrityFile, 0, len(names))
	for _, name := range names {
		content, err := plan.scope.ReadFile(name)
		switch {
		case err == nil:
			files = append(files, integrityFile{name: name, content: content, existed: true})
		case errors.Is(err, fs.ErrNotExist):
			// Recorded rather than skipped: absence is a state to restore.
			files = append(files, integrityFile{name: name})
		default:
			return nil, fmt.Errorf("read %s: %w", name, err)
		}
	}
	return files, nil
}

// restoreIntegrity puts every checksum file back to the state the snapshot
// recorded, and reports every failure rather than the first.
func restoreIntegrity(plan *Plan, files []integrityFile) error {
	errs := make([]error, 0, len(files))
	for _, file := range files {
		errs = append(errs, restoreIntegrityFile(plan, file))
	}
	return errors.Join(errs...)
}

// restoreIntegrityFile puts one checksum file back, or takes it away again.
//
// A file that was not there is removed, and a removal of something already
// absent is not a failure: the apply may have failed before Rehash wrote
// anything, and reporting that as a rollback failure would name the one state a
// caller must not ignore for a rollback that went perfectly.
func restoreIntegrityFile(plan *Plan, file integrityFile) error {
	parent, base, err := plan.scope.Parent(file.name)
	if err != nil {
		return err
	}
	defer closeParent(plan, parent)

	if !file.existed {
		if err := parent.Remove(base); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		return nil
	}
	return restoreContent(parent, base, FileChange{before: file.content})
}

// restore puts one file back to the state the plan recorded.
func restore(plan *Plan, file FileChange) error {
	parent, base, err := plan.scope.Parent(file.Path)
	if err != nil {
		return err
	}
	defer closeParent(plan, parent)

	if file.Operation == Create {
		return parent.Remove(base)
	}
	return restoreContent(parent, base, file)
}

// restoreContent writes the pre-image back.
//
// The destination expectation is deliberately weak here -- whatever is there is
// being replaced by the bytes that were there before -- because a rollback that
// refused because the file changed again would leave the failed patch in place,
// which is the worse of the two outcomes.
func restoreContent(parent *pathguard.OpenedDirectory, base string, file FileChange) (err error) {
	staged, stagedName, err := parent.CreateTemp(".ptah-agent-undo-*")
	if err != nil {
		return err
	}
	published := false
	defer func() {
		if published {
			return
		}
		err = errors.Join(err, parent.Remove(stagedName))
	}()

	if _, err = staged.Write(file.before); err != nil {
		return errors.Join(err, staged.Close())
	}
	if err = staged.Sync(); err != nil {
		return errors.Join(err, staged.Close())
	}
	if err = staged.Close(); err != nil {
		return err
	}
	if err = parent.ReplaceFile(stagedName, base); err != nil {
		published = errors.Is(err, fsdurable.ErrReplacementCommitted)
		return err
	}
	published = true
	return nil
}
