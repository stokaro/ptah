package atlas

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdadapter"
	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/migrate"
	"go.5x5.cz/ptah/internal/atlasargs"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/migration/migrator"
)

// newAtlasMigrateNewCommand returns `atlas migrate new`.
//
// The verb is still a plain forward into `ptah migrations create`; what this
// wrapper adds is the atlas.sum integrity gate in front of it
// (stokaro/ptah#1086). It cannot live in the forwarded command: the compat
// surface is where the Atlas-form --dir, its URL query, the atlas.hcl env and
// both environment spellings of the directory are resolved into the one
// directory the native command will write into, and the refusal has to happen
// before that command runs at all.
//
// It is deliberately the same resolution the `migrate hash` / `migrate
// validate` wrapper uses (resolveAtlasMigrateSource), not a second one:
// "which directory does this verb touch" is one question, and answering it
// twice is how a gate ends up verifying a directory the writer never opens.
func newAtlasMigrateNewCommand() *cobra.Command {
	verb := atlasMigrateNewVerb()
	cmd := newAtlasAdapterCommand("migrate", verb)
	forward := cmd.RunE
	cmd.RunE = func(cmd *cobra.Command, args []string) (runErr error) {
		if atlasArgsHaveHelp(args) {
			return forward(cmd, args)
		}
		cleanup := &cmdadapter.CleanupScope{}
		defer func() {
			runErr = errors.Join(runErr, cleanup.Close())
		}()
		source, err := resolveAtlasMigrateSource(cmd, verb, args, cleanup)
		if err != nil {
			return err
		}
		if !atlasmigrate.ReadsNativeAtlasDir(source.format) {
			return runAtlasMigrateNewConverted(cmd, verb, source)
		}
		if source.dir == "" {
			// No layer named a directory. `ptah migrations create` registers no
			// default for it and refuses with its own diagnostic, which is the
			// one this surface has always printed; inventing a directory to
			// verify here would change that message and verify nothing.
			return forward(cmd, source.forwardArgs)
		}
		if err := verifyAtlasWriteDirChecksum(cmd, source.project, source.localDir); err != nil {
			return err
		}
		return forward(cmd, source.forwardArgs)
	}
	return cmd
}

func atlasMigrateNewVerb() atlasVerb {
	return atlasVerb{
		use:        "new",
		displayUse: "new [flags] [name]",
		short:      "Create a new migration file",
		native:     "migrations create",
		factory:    migrate.NewMigrateCreateCommand,
		flags: []atlasargs.Flag{
			atlasargs.NativeLocalDir("dir", "", "Migration directory", "migrations-dir"),
			atlasMigrateDirFormatFlag("dir-format"),
			atlasargs.NativeBool("edit", "", "Edit the created migration files", "edit"),
		},
	}
}

// atlasMigrateNewArgs is what the converted `migrate new` path needs from the
// command line that the shared source resolution does not already carry: the
// migration name, which arrives as a positional, and --edit.
type atlasMigrateNewArgs struct {
	name string
	edit bool
}

// parseAtlasMigrateNewArgs reads the verb's own flags and positional from the
// Atlas-form arguments.
//
// The forwarding path lets `ptah migrations create` reject an unknown flag or a
// second positional; this path executes directly, so it has to reject them
// itself rather than silently drop them. It is deliberately the flag set
// [checkAtlasMigrateSourceArgs] builds, so the two paths cannot drift on which
// flags this verb accepts — the difference is only that `migrate new` takes one
// positional where the integrity verbs take none.
func parseAtlasMigrateNewArgs(
	cmd *cobra.Command,
	verb atlasVerb,
	args []string,
) (atlasMigrateNewArgs, error) {
	flagSet := atlasVerbFlagSet(verb)
	if err := flagSet.Parse(args); err != nil {
		return atlasMigrateNewArgs{}, cmdutil.Fail(cmd, err)
	}
	positional := flagSet.Args()
	if len(positional) > 1 {
		return atlasMigrateNewArgs{}, cmdutil.Fail(
			cmd,
			fmt.Errorf("accepts at most 1 arg(s), received %d", len(positional)),
		)
	}
	edit, err := flagSet.GetBool("edit")
	if err != nil {
		return atlasMigrateNewArgs{}, cmdutil.Fail(cmd, err)
	}
	out := atlasMigrateNewArgs{edit: edit}
	if len(positional) == 1 {
		out.name = positional[0]
	}
	return out, nil
}

// runAtlasMigrateNewConverted creates an empty migration in a source tool's own
// directory convention, for the four external layouts `migrate new` used to
// refuse outright (stokaro/ptah#845).
//
// The order below is the whole point of the function and is measured, not
// stylistic. The community binary refuses an unhashed or drifted directory
// before it writes anything — `migrate new addcol --dir-format golang-migrate`
// on an unhashed two-file directory exits 1 with `Error: checksum file not
// found` and leaves the directory untouched — so the gate runs first, over the
// covered set of the SELECTED layout rather than the Atlas one. Writing first
// and hashing after would rewrite atlas.sum over drift and hide it from
// `migrate validate`, which is the failure stokaro/ptah#1086 was.
//
// A directory that does not exist yet is not an integrity error: both binaries
// create it. That exemption lives in [verifyAtlasWriteDirCoveredChecksum].
func runAtlasMigrateNewConverted(cmd *cobra.Command, verb atlasVerb, source atlasMigrateSource) error {
	parsed, err := parseAtlasMigrateNewArgs(cmd, verb, source.projectArgs)
	if err != nil {
		return err
	}
	if parsed.edit {
		return cmdutil.Fail(cmd, fmt.Errorf(
			"atlas migrate %s --edit: --edit applies only to an atlas directory, but this directory is read as %s",
			verb.use,
			source.format,
		))
	}
	if source.dir == "" {
		// No layer named a directory. The forwarding path leaves this to `ptah
		// migrations create`, whose own diagnostic is the one this surface has
		// always printed; inventing a directory here would change the message
		// and, on this path, would also create one.
		return cmdutil.Fail(cmd, errors.New("migrations directory is required"))
	}
	if err := verifyAtlasWriteDirCoveredChecksum(cmd, source); err != nil {
		return err
	}
	written, err := writeAtlasMigrateNewSkeleton(source, parsed.name)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("atlas migrate %s: %w", verb.use, err))
	}
	reportAtlasMigrateNewFiles(cmd, written)
	return nil
}

// verifyAtlasWriteDirCoveredChecksum is [verifyAtlasWriteDirChecksum] for a
// directory laid out in a foreign tool's convention: the same preflight, over
// the file set that layout's atlas.sum covers.
//
// It is [verifyCoveredAtlasDirChecksum] under the same policy `migrate apply`
// uses, so a directory `migrate new` refuses is one `migrate apply`,
// `migrate hash` and `migrate validate` refuse too. Measured against the pinned
// community binary v1.3.0 on 2026-08-06, `migrate new addcol --dir-format
// golang-migrate` exits 1 with `checksum file not found` on an unhashed
// golang-migrate directory and with `checksum mismatch ... L2: 1_init.up.sql
// was edited` on a hashed-then-edited one, writing nothing in either case.
func verifyAtlasWriteDirCoveredChecksum(cmd *cobra.Command, source atlasMigrateSource) error {
	captured, err := source.project.captureLocal(source.localDir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return verifyCoveredAtlasDirChecksum(cmd, captured.FileSystem, source.format, requireAtlasSum)
}

// writeAtlasMigrateNewSkeleton creates the migration directory if needed, writes
// the empty migration in the selected layout, and rewrites atlas.sum over that
// layout's covered set. It returns the created files in creation order.
//
// The version is the UTC `yyyyMMddHHmmss` stamp both binaries use. When a file
// of that name already exists the stamp is advanced by one second and the whole
// set is retried, which is the rule [generateEmptyAtlasMigration] applies on the
// native path: the alternative is O_EXCL failing the command for a directory
// that merely already holds this second's migration.
func writeAtlasMigrateNewSkeleton(source atlasMigrateSource, name string) ([]string, error) {
	dir, err := pathguard.ResolveWithinRoot(source.localDir.Path, source.localDir.AllowedRoot)
	if err != nil {
		return nil, fmt.Errorf("invalid migrations directory: %w", err)
	}
	// The mode matches what `migrate hash` leaves on atlas.sum and what the
	// native create path leaves on an Atlas migration, so a directory holding
	// both does not carry two answers to "who may read a migration".
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}
	for version := atlasCompatMigrationVersion(); ; version++ {
		files, err := atlasmigrateimport.SkeletonFiles(source.format, version, name)
		if err != nil {
			return nil, err
		}
		written, err := createAtlasMigrateNewFiles(dir, files)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if err := rehashAtlasMigrateNewDir(dir, source.format); err != nil {
			removeAtlasMigrateNewFiles(written)
			return nil, err
		}
		return written, nil
	}
}

// createAtlasMigrateNewFiles writes one layout's file set, removing whatever it
// already created if a later file cannot be created. A half-written pair is
// worse than none: `migrate hash` would then cover the up file of a migration
// with no rollback half.
func createAtlasMigrateNewFiles(dir string, files []atlasmigrateimport.SkeletonFile) ([]string, error) {
	written := make([]string, 0, len(files))
	for _, file := range files {
		path := filepath.Join(dir, file.Name)
		if err := writeExclusiveAtlasMigrationFile(path, file.Content); err != nil {
			removeAtlasMigrateNewFiles(written)
			return nil, err
		}
		written = append(written, path)
	}
	return written, nil
}

func writeExclusiveAtlasMigrationFile(path, content string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := file.WriteString(content); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}

func removeAtlasMigrateNewFiles(paths []string) {
	for _, path := range paths {
		_ = os.Remove(path)
	}
}

// rehashAtlasMigrateNewDir rewrites atlas.sum over the covered set of the
// layout just written into.
//
// It is the computation `runAtlasMigrateHash` performs, called rather than
// restated, so a migration this verb creates is one `migrate validate` accepts
// without an intervening `migrate hash`.
func rehashAtlasMigrateNewDir(dir string, format atlasmigrateimport.Format) error {
	fsys := os.DirFS(dir)
	names, err := atlasmigrateimport.SumFileNames(fsys, format)
	if err != nil {
		return err
	}
	sum, err := migratesum.ComputeAtlasFiles(fsys, names)
	if err != nil {
		return err
	}
	return migratesum.WritePrecomputedWithFormat(dir, migrator.MigrationDirFormatAtlas, sum)
}

// atlasCompatMigrationVersion returns the UTC `yyyyMMddHHmmss` migration version
// both binaries stamp a new migration with.
func atlasCompatMigrationVersion() int64 {
	version, err := strconv.ParseInt(time.Now().UTC().Format("20060102150405"), 10, 64)
	if err != nil {
		return migrator.GetNextMigrationVersion()
	}
	return version
}

// reportAtlasMigrateNewFiles prints the created files the way the forwarded
// `ptah migrations create` prints them, so naming a directory's layout does not
// change how this verb reports what it made.
//
// The community binary prints nothing at all on success here. That divergence
// is older than this path — it is what `migrate new --dir-format atlas` already
// does on the forwarding branch — and matching it on the converted branch alone
// would make one verb report two ways.
func reportAtlasMigrateNewFiles(cmd *cobra.Command, written []string) {
	out := cmd.OutOrStdout()
	if len(written) == 1 {
		fmt.Fprintf(out, "Generated empty migration file:\n")
		fmt.Fprintf(out, "SQL:  %s\n", written[0])
		return
	}
	fmt.Fprintf(out, "Generated empty migration files:\n")
	fmt.Fprintf(out, "UP:   %s\n", written[0])
	fmt.Fprintf(out, "DOWN: %s\n", written[1])
}
