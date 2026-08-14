// Package migratevalidate implements the migration validation command: it
// verifies a migrations directory against its committed ptah.sum and exits
// non-zero on any drift (#161).
package migratevalidate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/cmd/internal/migrationsource"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/migrationvalidate"
	"go.5x5.cz/ptah/internal/ociartifact"
	"go.5x5.cz/ptah/migration/migrator"
)

const (
	atlasChecksumHeader = "You have a checksum error in your migration directory.\n"
	atlasChecksumFooter = "Please check your migration files and run 'atlas migrate hash' to re-hash the contents\n\n"
)

var (
	errAtlasChecksumMismatch     = errors.New("checksum mismatch")
	errAtlasChecksumFileNotFound = errors.New("checksum file not found")
)

// NewMigrateValidateCommand returns the migration validation command.
//
// The native verb resolves an `oci://` --dir through the same puller
// `migrations up`, `down`, `status` and `lint` use, and registers --plain-http
// so a local registry is reachable. Before stokaro/ptah#1499 it was the one
// verb in that neighborhood that stat'ed the reference as a path, which left
// the read-only integrity question — "do these artifact bytes match the sum
// they carry" — with no spelling that did not also execute or roll back
// migrations.
func NewMigrateValidateCommand() *cobra.Command {
	src := &source{registryBacked: true}
	cmd := newMigrateValidateCommand(
		src,
		runNativeValidate,
		"Local directory or oci:// reference containing migration files",
	)
	dbcli.RegisterPlainHTTPFlag(cmd.Flags(), &src.plainHTTP)
	return cmd
}

// NewAtlasMigrateValidateCommand returns migration validation with Atlas CE
// checksum-mismatch output semantics.
//
// It deliberately does NOT resolve `oci://` and does not register
// --plain-http. The compatibility surface's floor is the pinned community
// binary, which reads --dir as a filesystem path: a build that pulled an
// artifact here would exit 0 where that binary exits 1, and --plain-http would
// be a flag the conformance cli-surface tier finds on one side only. The
// capability is reachable on the native verb, which is where this repository
// puts behavior the compatibility surface may not carry.
func NewAtlasMigrateValidateCommand() *cobra.Command {
	return newMigrateValidateCommand(&source{}, runAtlasValidate, "Directory containing migration files")
}

// source is one --dir together with the flags that decide how it is read.
type source struct {
	dir       string
	dirFormat string
	devURL    string
	plainHTTP bool
	// registryBacked reports whether an `oci://` dir is pulled from a registry
	// rather than handed to the filesystem. It is a field rather than a check
	// on the scheme alone because the two constructors above disagree about it,
	// and the disagreement is the compatibility policy rather than an accident.
	registryBacked bool
}

// FailAtlasChecksumMismatch writes the Atlas CE checksum-mismatch guidance for
// mismatch (nil for a malformed sum file, which has no entry-level mismatch)
// and returns the exit-1 "checksum mismatch" error. Apply-time integrity gates
// use it so refusing a tampered directory is byte-identical to
// `migrate validate` on the same directory.
func FailAtlasChecksumMismatch(cmd *cobra.Command, mismatch *migratesum.Mismatch) error {
	return failAtlasChecksum(cmd, mismatch, errAtlasChecksumMismatch)
}

// FailAtlasChecksumUnreadableEntry writes the Atlas CE checksum guidance for a
// directory holding a covered entry that cannot be read, and returns the exit-1
// error carrying cause's own message.
//
// The community binary treats this as a checksum error and not as an ordinary
// command failure: it prints the same stdout guidance block it prints for
// drift, then the read failure on stderr, on `migrate validate`, `apply`,
// `status` and `set` alike. Measured against the pinned v1.3.0 binary, that is
// the shape reproduced here (stokaro/ptah#991). There is no `L<n>:` line
// because no entry mismatched — the directory could not be hashed at all.
//
// `migrate hash` is the one verb that does NOT get the preamble, on that binary
// and here: there is no recorded sum to be in error with, so the read failure
// is reported bare.
func FailAtlasChecksumUnreadableEntry(cmd *cobra.Command, cause error) error {
	return failAtlasChecksum(cmd, nil, cause)
}

// FailAtlasChecksumFileNotFound writes the Atlas CE checksum guidance for a
// directory that carries no integrity file and returns the exit-1 "checksum
// file not found" error. Apply-time integrity gates use it so refusing an
// unhashed directory is byte-identical to `migrate validate` on the same
// directory (#970).
func FailAtlasChecksumFileNotFound(cmd *cobra.Command) error {
	return failAtlasChecksum(cmd, nil, errAtlasChecksumFileNotFound)
}

type validateRunner func(*cobra.Command, *source) error

func newMigrateValidateCommand(src *source, run validateRunner, dirUsage string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Verify a migrations directory against its committed ptah.sum",
		Long: `migrations validate recomputes the integrity hashes of a migrations directory
and compares them against the committed ptah.sum. It exits:

  0  the directory matches ptah.sum
  1  a migration file was added, removed, or edited out of band (drift)
  2  ptah.sum is missing or unreadable, or the directory is inaccessible

Run it in CI to guarantee already-committed migrations are never changed.`,
		Args:          cmdutil.NoPositionalArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd, src)
		},
	}
	cmd.Flags().StringVar(&src.dir, "dir", "./migrations", dirUsage)
	cmd.Flags().StringVar(&src.dirFormat, "dir-format", string(migrator.MigrationDirFormatAuto), "Migration directory format: auto, ptah, or atlas")
	cmd.Flags().StringVar(&src.devURL, "dev-url", "", "Dev database URL used to clean and replay migrations for SQL validation")
	cmd.SetFlagErrorFunc(cmdutil.FlagErrorFunc)
	return cmd
}

func runNativeValidate(cmd *cobra.Command, src *source) error {
	checked, err := validate(cmd.Context(), src)
	if err != nil {
		// Native validation treats missing, unreadable, and malformed sum files
		// as usage failures distinct from content drift.
		return cmdutil.Fail(cmd, err)
	}

	if !checked.result.Integrity.OK() {
		fmt.Fprintln(cmd.ErrOrStderr(), checked.result.Integrity.Describe())
		return exitcode.New(1, errors.New("migration directory integrity check failed"))
	}

	return writeNativeValidationSuccess(cmd, checked)
}

func runAtlasValidate(cmd *cobra.Command, src *source) error {
	checked, err := validate(cmd.Context(), src)
	result := checked.result
	switch {
	case errors.Is(err, migratesum.ErrSumFileMissing):
		empty, emptyErr := DirectoryHoldsNoSQLFiles(src.dir)
		if emptyErr != nil {
			return cmdutil.Fail(cmd, emptyErr)
		}
		if empty {
			// An existing directory holding no migration files has nothing for
			// an integrity file to cover, so its absence is not drift. The
			// pinned community binary v1.3.0 exits 0 with no output here, and
			// its own `migrate apply` on the same directory says "No migration
			// files to execute" rather than refusing (stokaro/ptah#1241 item 7).
			//
			// The refusal stays for a directory that DOES hold migration files
			// and carries no integrity file: measured on that binary, exit 1
			// with this same byte-identical guidance block.
			return nil
		}
		return FailAtlasChecksumFileNotFound(cmd)
	case errors.Is(err, migratesum.ErrSumFileMalformed):
		// A malformed sum file has no entry-level mismatch to point at.
		return FailAtlasChecksumMismatch(cmd, nil)
	case errors.Is(err, migratesum.ErrCoveredEntryUnreadable):
		// A covered entry that is a directory (#991). The community binary
		// prints the checksum preamble for it, so this is a checksum refusal
		// and not a usage failure.
		return FailAtlasChecksumUnreadableEntry(cmd, err)
	case err != nil:
		return cmdutil.Fail(cmd, AtlasDirectoryError(src.dir, err))
	}

	if !result.Integrity.OK() {
		return FailAtlasChecksumMismatch(cmd, result.Integrity.FirstMismatch())
	}

	return nil
}

// AtlasDirectoryError adapts cmdutil.StatDir's missing-directory diagnostic for
// dir to the Atlas CE `sql/migrate` wording. Other failures keep their original
// text, including permission errors, unrelated nested stat errors, and paths
// that exist but are not directories.
//
// The display wrapper unwraps to the complete original error rather than only
// the extracted [os.PathError], so callers retain every contextual layer for
// errors.Is and errors.As while the compatibility surface prints Atlas's text.
func AtlasDirectoryError(dir string, err error) error {
	var pathErr *os.PathError
	if !errors.As(err, &pathErr) ||
		pathErr.Op != "stat" ||
		pathErr.Path != dir ||
		!errors.Is(pathErr, fs.ErrNotExist) ||
		err.Error() != "migrations directory "+dir+": "+pathErr.Error() {
		return err
	}
	return atlasDirectoryDisplayError{
		text: "sql/migrate: " + pathErr.Error(),
		err:  err,
	}
}

type atlasDirectoryDisplayError struct {
	text string
	err  error
}

func (e atlasDirectoryDisplayError) Error() string { return e.text }
func (e atlasDirectoryDisplayError) Unwrap() error { return e.err }

// DirectoryHoldsNoSQLFiles reports whether dir holds no `.sql` file at its top
// level, which is the shape an integrity file has nothing to cover.
//
// It deliberately asks about `.sql` rather than about the covered set of one
// directory format: every format Ptah reads names its migrations with that
// extension, so a directory holding any of them is one where a missing
// integrity file is still a refusal. Nested directories are ignored because no
// format reads them.
//
// It is exported so the Atlas-compatible `migrate validate` reaches the same
// answer on a converted directory as the forwarding path does on a native one.
func DirectoryHoldsNoSQLFiles(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, fmt.Errorf("read migrations directory %s: %w", dir, err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.EqualFold(filepath.Ext(entry.Name()), ".sql") {
			return false, nil
		}
	}
	return true, nil
}

// checkedSource is one completed validation together with the registry
// artifact it ran over, when there was one.
//
// The provenance of the bytes is carried alongside the verdict rather than
// folded into it because a sum verification and what that verification is
// worth are two different statements: the sum says the directory matches the
// sum stored beside it, and only the reference's own shape says whether those
// bytes are the reviewed ones.
type checkedSource struct {
	result migrationvalidate.Result
	// resolved is the registry artifact the reference selected, or nil for a
	// local directory.
	resolved *migrationsource.Source
}

func validate(ctx context.Context, src *source) (checkedSource, error) {
	if src.registryBacked && strings.HasPrefix(src.dir, ociartifact.Scheme) {
		return validateArtifact(ctx, src)
	}

	if err := cmdutil.StatDir(src.dir); err != nil {
		return checkedSource{}, err
	}

	dirFormat, err := migrator.ParseMigrationDirFormat(src.dirFormat)
	if err != nil {
		return checkedSource{}, err
	}

	result, err := migrationvalidate.Validate(ctx, migrationvalidate.Options{
		Dir:       src.dir,
		DirFormat: dirFormat,
		DevURL:    src.devURL,
	})
	return checkedSource{result: result}, err
}

// validateArtifact pulls the registry artifact and validates the bytes that
// came back.
//
// The pulled filesystem is handed to [migrationvalidate.Validate] directly
// rather than written to a temporary directory first. That is the point of the
// verb: what it reports on must be the bytes the pull produced, and a
// materialize-then-read round trip inserts a second read the answer would be
// about instead. It is also why the resolved DirFormat is used rather than the
// requested one — an artifact records the format it was published with, and
// `--dir-format auto` against a registry has no directory to sniff.
func validateArtifact(ctx context.Context, src *source) (checkedSource, error) {
	dirFormat, err := migrator.ParseMigrationDirFormat(src.dirFormat)
	if err != nil {
		return checkedSource{}, err
	}

	resolved, err := migrationsource.Resolve(ctx, src.dir, migrationsource.Options{
		DirFormat: dirFormat,
		PlainHTTP: src.plainHTTP,
	})
	if err != nil {
		return checkedSource{}, err
	}

	result, err := migrationvalidate.Validate(ctx, migrationvalidate.Options{
		Dir:       resolved.Display,
		FS:        resolved.FileSystem,
		DirFormat: resolved.DirFormat,
		DevURL:    src.devURL,
	})
	return checkedSource{result: result, resolved: &resolved}, err
}

func writeNativeValidationSuccess(cmd *cobra.Command, checked checkedSource) error {
	var message strings.Builder
	fmt.Fprintf(&message, "OK: migrations directory matches %s\n", checked.result.Integrity.SumFileName)
	if checked.result.DevSQLValidated {
		message.WriteString("OK: migration SQL validated on dev database\n")
	}
	if _, err := io.WriteString(cmd.OutOrStdout(), message.String()); err != nil {
		return fmt.Errorf("write validation success: %w", err)
	}
	return writeMutableTagQualifier(cmd, checked)
}

// writeMutableTagQualifier states what an OK over a movable tag is worth.
//
// A bare "OK: migrations directory matches ptah.sum" over an `oci://` tag
// over-claims in exactly the direction stokaro/ptah#928 item 5 is about: for an
// artifact the sum travels INSIDE the artifact, so anyone who can push to the
// repository can rewrite the migrations, rehash them, repoint the tag, and
// watch this verb print OK over bytes nobody reviewed. `migrations up`, `down`
// and `status` already qualify that claim, through the same shared sentence
// used here — the reason it is shared is that a verb which forgets it is
// indistinguishable, in its output, from one that checked something stronger.
//
// It goes to standard error so the success line on standard output stays what
// a caller parses, and it is unreachable for a local directory and for a
// digest-pinned reference: [migrationsource.MutableTagSumWarning] returns the
// empty string for both.
func writeMutableTagQualifier(cmd *cobra.Command, checked checkedSource) error {
	if checked.resolved == nil {
		return nil
	}
	warning := migrationsource.MutableTagSumWarning(*checked.resolved, checked.result.Integrity.SumFileName)
	if warning == "" {
		return nil
	}
	if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "Warning: %s\n", warning); err != nil {
		return fmt.Errorf("write provenance qualifier: %w", err)
	}
	return nil
}

func failAtlasChecksum(
	cmd *cobra.Command,
	mismatch *migratesum.Mismatch,
	atlasErr error,
) error {
	writeErr := errors.Join(
		writeAtlasChecksumGuidance(cmd.OutOrStdout(), mismatch),
		writeAtlasChecksumError(cmd.ErrOrStderr(), atlasErr),
	)
	if writeErr != nil {
		return exitcode.New(1, fmt.Errorf("%w: failed to write checksum output: %w", atlasErr, writeErr))
	}
	return exitcode.New(1, atlasErr)
}

func writeAtlasChecksumGuidance(w io.Writer, mismatch *migratesum.Mismatch) error {
	var guidance strings.Builder
	guidance.WriteString(atlasChecksumHeader)
	if mismatch != nil {
		fmt.Fprintf(&guidance, "\n\tL%d: %s was %s\n\n", mismatch.Line, mismatch.File, mismatch.Reason)
	}
	guidance.WriteString(atlasChecksumFooter)
	if _, err := fmt.Fprint(w, guidance.String()); err != nil {
		return fmt.Errorf("write checksum guidance: %w", err)
	}
	return nil
}

func writeAtlasChecksumError(w io.Writer, atlasErr error) error {
	if _, err := fmt.Fprintf(w, "Error: %s\n", atlasErr); err != nil {
		return fmt.Errorf("write checksum error: %w", err)
	}
	return nil
}
