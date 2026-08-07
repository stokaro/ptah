package atlas

import (
	"errors"
	"fmt"
	"io/fs"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdadapter"
	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/migrate"
	"go.5x5.cz/ptah/internal/atlasargs"
	"go.5x5.cz/ptah/internal/atlasmigrate"
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
		writesDir:  true,
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
	written, err := atlasmigrate.WriteSkeletonMigration(
		migrateDiffWriterRoot(source.project, source.localDir),
		source.localDir.Path,
		source.format,
		parsed.name,
	)
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
