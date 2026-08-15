// Package migrationintegrity owns the one rule every Ptah verb that EXECUTES
// SQL from a migration directory runs before executing any of it: a directory
// that carries an integrity file has to match it.
//
// The rule started life inside `ptah migrations up` (stokaro/ptah#955) as a
// private helper, and keeping it there is what produced the defect this package
// exists to close. Measured on one hashed directory whose `_init.down.sql` was
// rewritten and whose ptah.sum was left stale, `migrations up` exited 2 and
// refused, while `migrations down --target 0 --confirm` exited 0 and executed
// the rewritten file — a post-run table census showed the attacker's table
// present and the migration's own table gone. The same held for
// `migrations test`, which rolls forward and back through the same files, for
// the shadow replays inside `migrations checkpoint` and `migrations baseline`,
// for the `--dev-url` replay inside `migrations lint`, and for
// `migrations repair --resume-from`, which executes the remaining statements of
// the body that failed straight out of the file.
//
// The membership test is "does this verb execute SQL taken from the directory",
// and it is not answered by the verb's name. `repair` reads as metadata
// maintenance and is exactly that WITHOUT --resume-from; with it, the verb
// executes migration statements and belongs here. The gate is therefore applied
// per invocation there, not per verb — see cmd/migraterepair. The enumeration
// itself lives beside the namespace that registers the verbs, in
// cmd/migrations/integrity_class_test.go, so a new verb has a visible place
// where its verdict is missing.
//
// Verification guarding only the constructive direction is backwards. `down` is
// the direction where an operator cannot inspect the result afterwards, because
// the objects the run was supposed to remove are gone either way; and
// `checkpoint` is worse still, because it replays the tampered history onto a
// shadow database and then writes what it observed there into a NEW migration
// under a FRESH integrity file — laundering the tampering into a directory that
// verifies clean. That is the same shape stokaro/ptah#1095 closed on the
// compat surface, where `migrate import` was rewriting a directory
// `migrate apply` refuses.
//
// So the predicate is expressed once, here, and called by every verb in the
// class rather than reimplemented per verb. The compat surface reached the same
// conclusion for the same reason; see the file comment on
// cmd/atlas/migrate_integrity_gate.go, which records that leaving the rule
// inside `migrate apply` produced #974 and then #1095.
//
// # What the gate does NOT change
//
// An UNHASHED directory keeps its ungated behavior, exactly as it does on
// `migrations up`. A directory nobody ever hashed has no recorded intent to
// compare against, and refusing it would remove a capability rather than
// protect one. The gate fires on drift from a sum the directory actually
// carries.
//
// The stricter `--verify-sum` contract — where a MISSING sum file is itself an
// error — is NOT the default and never becomes it. It is an explicit operator
// request for more than the default, so it lives behind the flag, and
// [AllowUnverifiedEnvVar] does not relax it. It is expressed here, as
// [Options.RequireSum], rather than inside any one verb: `up` owning the
// always-on rule privately is what let five other verbs drift, and the strict
// rule is the same shape of thing.
//
// # Why --verify-sum is not redundant given the always-on gate
//
// They answer different questions, and the difference is measurable on an
// UNHASHED directory. The always-on gate asks "does this directory match the
// sum it carries" and a directory carrying no sum passes, because there is no
// recorded intent to compare against. --verify-sum asks "is this directory
// covered by a sum at all", which a published artifact can fail. Without the
// flag an operator has no spelling that DEMANDS coverage, so an artifact
// published with no integrity file is indistinguishable at the command line
// from one whose sum verified.
package migrationintegrity

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"slices"

	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/envbool"
	"go.5x5.cz/ptah/internal/fsnapshot"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/migrationsnapshot"
	"go.5x5.cz/ptah/migration/migrator"
)

// ErrAuthorizedHistoryChanged reports that migration history changed after a
// checkpoint body was derived from it. The edited checkpoint is allowed to
// change; every preexisting migration and metadata file remains bound to the
// snapshot that was replayed.
var ErrAuthorizedHistoryChanged = errors.New("migration history changed while editing checkpoint")

// CheckpointEditAuthorization is the exact post-write state an interactive
// checkpoint edit may change. Its fields are private so callers can obtain one
// only by verifying a bound migration directory through
// [AuthorizeCheckpointEdit].
type CheckpointEditAuthorization struct {
	snapshot    fsnapshot.Snapshot
	editedNames []string
	format      migrator.MigrationDirFormat
	writer      *atlasmigrate.MigrationWriter
}

// AllowUnverifiedEnvVar is the escape hatch: set it to a true value and a
// hashed directory that does NOT match its integrity file is executed anyway.
//
// It exists because refusing outright would remove a capability the tool has
// today, which repository policy forbids. An operator recovering from a botched
// edit genuinely may need to roll back through a directory whose sum is stale —
// that is precisely the moment the down files matter and precisely the moment
// re-hashing first would record the botched bytes as intended.
//
// It is an environment variable and NOT a flag on purpose. The conformance
// `cli-surface` tier asserts flag parity against the community binary, so a new
// flag on any verb this gate guards would break it. Every boolean `PTAH_*` is
// declared once through [go.5x5.cz/ptah/internal/envbool]; see
// cmd/internal/envboolguard for the guard that enforces it.
//
// Using it is never silent. A flag leaves a trace in the command line an
// operator can read back afterwards; an environment variable exported three
// shells ago does not, so a run whose integrity gate was overridden has to say
// so itself, and say what it skipped — see [Gate].
const AllowUnverifiedEnvVar = "PTAH_ALLOW_UNVERIFIED_MIGRATION_DIR"

// allowUnverified is the declaration of the variable, made once, in the package
// that owns the rule it relaxes.
//
// The default is false, which is the strict side. Every boolean `PTAH_*` in
// this tree opts IN to the more permissive behavior, so a typo lands on the
// strict default and fails closed.
//
// It is [go.5x5.cz/ptah/internal/envbool.Gated]. The pinned community binary
// has no spelling that executes a hashed directory whose integrity file does
// not match: `atlas.sum` mismatch is a refusal there with `migrate hash` as the
// only way out. A true value therefore runs migrations that binary would not
// have run, which is a capability it lacks rather than one Ptah is restoring,
// and a conformance run that executed under it would be measuring a different
// program. Strict mode refuses it; the default surface keeps the escape hatch.
var allowUnverified = envbool.New(AllowUnverifiedEnvVar, false, envbool.Gated)

// Options selects how strict one gate call is.
type Options struct {
	// RequireSum makes a MISSING integrity file an error instead of a pass.
	//
	// It is the `--verify-sum` contract. Set it only when the operator asked
	// for it on the command line: a directory nobody ever hashed has no
	// recorded intent to compare against, and refusing it by default would
	// remove a capability rather than protect one.
	//
	// It also switches off [AllowUnverifiedEnvVar]. An explicit command-line
	// request for a stricter contract is not overridden by an environment
	// variable exported three shells ago — the flag is the more specific and
	// more recent statement of intent, and a build where the variable won
	// would let a CI environment file silently defeat the flag its own
	// pipeline passes.
	RequireSum bool
}

// Policy is the process-boundary decision for the migration-directory
// integrity escape hatch. Commands resolve it before validating their own
// arguments, then carry the value to every integrity check they perform.
// Keeping the decision explicit prevents an invalid environment value from
// hiding behind an early command return and prevents one invocation from
// observing different values at different gates.
type Policy struct {
	allowUnverified bool
}

// Resolve reads the integrity escape hatch once. Callers that own migration
// directory execution resolve it at their command boundary, before any other
// validation or external work.
//
// The environment variable is resolved BEFORE the directory is read, so an
// unparseable value refuses the command whatever state the directory is in. A
// value that only fails the run on already-drifted directories would let
// `PTAH_ALLOW_UNVERIFIED_MIGRATION_DIR=yes` sit unnoticed in a CI environment
// file until the day it was load-bearing, which is the reverse of what the
// envbool contract is for. Resolving at the command boundary rather than inside
// the gate is also what covers [Options.RequireSum], which does not honor the
// variable: a typo is refused there too, instead of reaching a branch that
// happens not to read it.
func Resolve() (Policy, error) {
	allow, err := allowUnverified.Resolve()
	if err != nil {
		return Policy{}, err
	}
	return Policy{allowUnverified: allow}, nil
}

// GateWithPolicy verifies fsys against the integrity file it carries and
// reports which file verified, or the empty string when nothing was checked,
// applying a decision already resolved by [Resolve] at the owning command
// boundary.
//
// Call it before the verb executes anything, and — where the verb connects to a
// database — before it connects. A gate that fires after the connection has
// already told the operator less than one that fires instead of it.
//
// The returned name is what the CALLER may claim it verified. It is empty both
// when the directory was never hashed and when [AllowUnverifiedEnvVar]
// suppressed a real failure, so a run that skipped the check cannot go on to
// report that a check passed. `migrations up`, `down` and `status` use exactly
// that to decide whether their movable-OCI-tag provenance warning has anything
// to qualify.
//
// notice receives the override announcement, and receives it only when an
// override actually suppressed a refusal: a clean directory skips nothing, so
// it says nothing. Pass the command's stderr.
func GateWithPolicy(
	notice io.Writer,
	fsys fs.FS,
	format migrator.MigrationDirFormat,
	policy Policy,
	opts Options,
) (string, error) {
	if opts.RequireSum {
		result, err := migratesum.VerifyWithFormat(fsys, format)
		if err != nil {
			return "", fmt.Errorf("migration sum verification failed: %w", err)
		}
		if !result.OK() {
			return "", fmt.Errorf("migration sum verification failed:\n%s", result.Describe())
		}
		return result.SumFileName, nil
	}

	result, hashed, err := migratesum.VerifyHashed(fsys, format)
	if err != nil {
		return "", fmt.Errorf("migration sum verification failed: %w", err)
	}
	if !hashed {
		return "", nil
	}
	if result.OK() {
		return result.SumFileName, nil
	}
	if !policy.allowUnverified {
		return "", fmt.Errorf("migration sum verification failed:\n%s", result.Describe())
	}
	fmt.Fprintf(notice,
		"warning: %s is set; %s verification was SKIPPED and this run is executing "+
			"migration SQL that no reviewed checksum covers:\n%s\n",
		AllowUnverifiedEnvVar,
		sumFileNameOf(result),
		result.Describe(),
	)
	return "", nil
}

// AuthorizeCheckpointEdit records the exact post-write state before an editor
// opens. The directory must equal the replay-authorized history plus the new
// checkpoint files, and its current integrity file must verify that state.
func AuthorizeCheckpointEdit(
	ctx context.Context,
	writer *atlasmigrate.MigrationWriter,
	format migrator.MigrationDirFormat,
	authorized fs.FS,
	editedPaths ...string,
) (CheckpointEditAuthorization, error) {
	if writer == nil {
		return CheckpointEditAuthorization{}, errors.New("migration writer is required")
	}
	if authorized == nil {
		return CheckpointEditAuthorization{}, errors.New("authorized migration history is required")
	}
	authorizedSnapshot, err := migrationsnapshot.Capture(authorized)
	if err != nil {
		return CheckpointEditAuthorization{}, fmt.Errorf("capture authorized migration history before checkpoint edit: %w", err)
	}
	editedNames, err := editedCheckpointNames(writer.Path(), editedPaths)
	if err != nil {
		return CheckpointEditAuthorization{}, err
	}
	var authorization CheckpointEditAuthorization
	err = atlasmigrate.WithMigrationDirectoryLock(ctx, writer.Path(), 0, func(context.Context) error {
		if err := writer.Revalidate(); err != nil {
			return fmt.Errorf("revalidate migration directory before checkpoint edit: %w", err)
		}
		fsys, err := writer.FS()
		if err != nil {
			return fmt.Errorf("open migration directory before checkpoint edit: %w", err)
		}
		newFiles := make(map[string][]byte, len(editedNames)+1)
		for _, name := range editedNames {
			contents, readErr := fs.ReadFile(fsys, name)
			if readErr != nil {
				return fmt.Errorf("read checkpoint %s before edit: %w", name, readErr)
			}
			newFiles[name] = contents
		}
		sumName, err := migratesum.FileNameForFormat(format)
		if err != nil {
			return err
		}
		currentSum, err := fs.ReadFile(fsys, sumName)
		if err != nil {
			return fmt.Errorf("read %s before checkpoint edit: %w", sumName, err)
		}
		newFiles[sumName] = currentSum
		expected, err := authorizedSnapshot.WithFiles(newFiles)
		if err != nil {
			return fmt.Errorf("build authorized checkpoint snapshot before edit: %w", err)
		}
		current, err := migrationsnapshot.Capture(fsys)
		if err != nil {
			return fmt.Errorf("capture migration directory before checkpoint edit: %w", err)
		}
		if !expected.Equal(current) {
			return ErrAuthorizedHistoryChanged
		}
		result, hashed, err := migratesum.VerifyHashed(current, format)
		if err != nil {
			return fmt.Errorf("verify %s before checkpoint edit: %w", sumName, err)
		}
		if !hashed || !result.OK() {
			return ErrAuthorizedHistoryChanged
		}
		authorization = CheckpointEditAuthorization{
			snapshot:    current,
			editedNames: slices.Clone(editedNames),
			format:      format,
			writer:      writer,
		}
		return nil
	})
	return authorization, err
}

// RefreshEditedCheckpointIntegrity publishes a checksum for edited checkpoint
// files without authorizing any other change made while the editor was open.
// writer must be the same handle passed to [AuthorizeCheckpointEdit].
func RefreshEditedCheckpointIntegrity(
	ctx context.Context,
	writer *atlasmigrate.MigrationWriter,
	authorization CheckpointEditAuthorization,
) error {
	if writer == nil {
		return errors.New("migration writer is required")
	}
	if len(authorization.editedNames) == 0 {
		return errors.New("checkpoint edit authorization is required")
	}
	if authorization.writer != writer {
		return errors.New("checkpoint edit authorization belongs to a different migration writer")
	}
	return atlasmigrate.WithMigrationDirectoryLock(ctx, writer.Path(), 0, func(context.Context) error {
		if err := writer.Revalidate(); err != nil {
			return fmt.Errorf("revalidate migration directory after checkpoint edit: %w", err)
		}
		fsys, err := writer.FS()
		if err != nil {
			return fmt.Errorf("open migration directory after checkpoint edit: %w", err)
		}
		editedFiles := make(map[string][]byte, len(authorization.editedNames))
		for _, name := range authorization.editedNames {
			contents, readErr := fs.ReadFile(fsys, name)
			if readErr != nil {
				return fmt.Errorf("read edited checkpoint %s: %w", name, readErr)
			}
			editedFiles[name] = contents
		}
		expected, err := authorization.snapshot.WithFiles(editedFiles)
		if err != nil {
			return fmt.Errorf("build authorized checkpoint snapshot after edit: %w", err)
		}
		current, err := migrationsnapshot.Capture(fsys)
		if err != nil {
			return fmt.Errorf("capture migration directory after checkpoint edit: %w", err)
		}
		if !expected.Equal(current) {
			return ErrAuthorizedHistoryChanged
		}
		sumName, err := migratesum.FileNameForFormat(authorization.format)
		if err != nil {
			return err
		}
		sum, err := migratesum.ComputeWithFormat(expected, authorization.format)
		if err != nil {
			return fmt.Errorf("compute %s after checkpoint edit: %w", sumName, err)
		}
		if _, err := writer.PublishSum(authorization.format, sum); err != nil {
			return fmt.Errorf("publish %s after checkpoint edit: %w", sumName, err)
		}
		if err := writer.SyncDir(); err != nil {
			return fmt.Errorf("flush migration directory after checkpoint edit: %w", err)
		}
		return nil
	})
}

func editedCheckpointNames(dir string, paths []string) ([]string, error) {
	if len(paths) == 0 {
		return nil, errors.New("edited checkpoint path is required")
	}
	names := make([]string, 0, len(paths))
	seen := make(map[string]struct{}, len(paths))
	for _, checkpointPath := range paths {
		rel, err := filepath.Rel(dir, checkpointPath)
		if err != nil || rel == "." || filepath.Dir(rel) != "." {
			return nil, fmt.Errorf("edited checkpoint path %q is not a direct child of %s", checkpointPath, dir)
		}
		name := filepath.ToSlash(rel)
		if _, exists := seen[name]; exists {
			return nil, fmt.Errorf("edited checkpoint path %q is duplicated", checkpointPath)
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	return names, nil
}

// sumFileNameOf names the integrity file a drifted result belongs to, falling
// back to the Ptah spelling when the result did not record one.
func sumFileNameOf(result *migratesum.Result) string {
	if result.SumFileName != "" {
		return result.SumFileName
	}
	return migratesum.FileName
}
