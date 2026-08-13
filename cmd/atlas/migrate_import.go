package atlas

import (
	"fmt"
	"io/fs"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/migratevalidate"
	"go.5x5.cz/ptah/internal/atlasargs"
	"go.5x5.cz/ptah/internal/atlascompatpolicy"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
)

func newAtlasMigrateImportCommand(policy atlascompatpolicy.Policy) *cobra.Command {
	var opts atlasmigrateimport.Options
	cmd := &cobra.Command{
		Use:   "import",
		Short: "Import migrations from another tool",
		Long: `Atlas OSS ` + "`atlas migrate import`" + ` command path.

Imports a local migration directory from an Atlas-supported migration tool into
Atlas single-file migration layout and writes atlas.sum. The source format can
be selected with --dir-format or with the format query parameter on --from, for
example file://migrations?format=flyway.

Ptah currently imports local file:// directories only. The destination directory
must be different from the source directory and must not already contain SQL
migration files or atlas.sum.

Flyway repeatable migrations are converted on a reserved version slot above
every versioned migration. The destination file name carries that slot rather
than an R suffix, so the imported directory keeps one-time migration semantics
instead of Flyway-style reapply semantics.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAtlasMigrateImport(cmd, policy, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.FromURL, "from", "file://migrations", "Source migration directory URL")
	flags.StringVar(&opts.ToURL, "to", "file://migrations", "Destination migration directory URL")
	flags.StringVar(&opts.DirFormat, "dir-format", "atlas", "Source migration directory format: atlas, golang-migrate, goose, flyway, liquibase, or dbmate")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgsHint("name the directories with --from and --to"))
	return cmd
}

// runAtlasMigrateImport writes the imported directory and says nothing about
// it. Atlas CE reports a successful import only through the files on disk, so
// the destination directory and its atlas.sum are the whole result; a progress
// listing here is output the compatibility surface is not supposed to produce.
//
// Silence is achieved by not writing, never by pointing this command's writer
// at io.Discard. Unlike the adapter verbs, `migrate import` is registered
// directly on the compatibility tree, so the command this runs on is the
// persistent child that survives every Execute on a reused root — not a native
// target minted per execution. Cobra's getOut prefers a command's own non-nil
// outWriter over its parent's, so assigning io.Discard here would pin it
// forever and a later root SetOut would stop reaching `migrate import --help`.
// Failures still route through cmdutil.Fail, which keeps every error loud.
//
// The three steps are capture, gate, write, in that order and with nothing
// between them, for the reason `migrate apply` uses the same order (#973):
// the source directory's atlas.sum has to be verified before its layout is
// parsed, and before any byte is written. Writing first and refusing afterwards
// would leave the converted directory — and the fresh atlas.sum computed over
// it — behind, which is the laundering half of #1095 and survives any test that
// only checks the exit code.
func runAtlasMigrateImport(
	cmd *cobra.Command,
	policy atlascompatpolicy.Policy,
	opts atlasmigrateimport.Options,
) error {
	opts, fromDir, err := resolveAtlasMigrateImportSource(opts)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	// The source has to exist before its layout can be compared to the target
	// layout; see [resolveAtlasMigrateImportSource] for the measured order and
	// for why the comparison itself stays where it is.
	if err := cmdutil.StatDir(fromDir); err != nil {
		return cmdutil.Fail(cmd, migratevalidate.AtlasDirectoryError(fromDir, err))
	}
	captured, err := atlasmigrateimport.CaptureImport(opts)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	// `--to` is required to carry a scheme, and it is required HERE. Measured
	// on the pinned community binary v1.3.0 with a source that exists:
	// `--to dst --dir-format flyway` prints the scheme refusal and writes
	// nothing, while `--to dst` with the atlas layout prints the
	// already-in-target-format refusal instead — so the scheme check follows
	// the comparison [atlasmigrateimport.CaptureImport] performs, and both
	// precede any byte being written.
	if err := atlasargs.RequireDirScheme(opts.ToURL); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if err := verifyAtlasImportSourceChecksum(cmd, captured.Source, captured.Format); err != nil {
		return err
	}
	if err := policy.ValidateMigrationSource(captured.Source); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if _, err := captured.Write(); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	return nil
}

// resolveAtlasMigrateImportSource resolves `migrate import`'s source directory
// through the same rules every other compat verb applies to its `--dir`, and
// returns options carrying the ONE resolved layout plus the local source path.
//
// `migrate import` was the verb this hoist never reached. It kept a private
// resolver — [atlasmigrateimport] `sourceFormat` — that lowercases and trims
// its input and reads a present-but-empty `?format=` as "no selection". Every
// other verb resolves through [atlasmigrate.ResolveApplyDirFormat], which
// matches values verbatim and lets an empty query value select the native
// Atlas layout. Measured against the pinned community binary v1.3.0 on a
// Flyway source directory, each row read from an unpiped invocation:
//
//	--dir-format FLYWAY                        CE 1  ptah 0, WROTE
//	--dir-format ' flyway '                    CE 1  ptah 0, WROTE
//	--from 'file://src?format=FLYWAY'          CE 1  ptah 0, WROTE
//	--from 'file://src?format=' --dir-format flyway
//	                                           CE 1  ptah 0, WROTE
//	--from src (no scheme)                     CE 1  ptah 0, WROTE
//
// All five are exit 0 where that binary exits 1, on the compat verb that
// WRITES a directory — the direction parity must never take — and the first
// four are the same coercion stokaro/ptah#1013 removed from `migrate diff` and
// `migrate lint`. The rows that must not move, same fixture and same binary:
//
//	--dir-format flyway                        CE 0, WROTE  ptah 0, WROTE
//	--from 'file://src?format=flyway'          CE 0, WROTE  ptah 0, WROTE
//	--from 'file://src?nonsense=1' --dir-format flyway
//	                                           CE 0, WROTE  ptah 0, WROTE
//	--from 'file://src?format=flyway&format=goose'
//	                                           CE 0, WROTE  ptah 0, WROTE
//
// The resolved layout is written back onto the options and the query is
// dropped from the source URL, so [atlasmigrateimport.CaptureImport]'s own
// resolver reads a single lowercase value with no query and cannot reach a
// different answer. Resolving twice and hoping the two agree is what let the
// empty-query row through: this surface would have said "atlas" while the
// importer read "flyway".
//
// WHY THE SOURCE EXISTENCE CHECK IS THE CALLER'S. The importer refuses a
// source already in the target layout before it opens anything, and its
// comment says moving the capture ahead of that refusal "would only trade one
// refusal message for another while changing what a nonexistent --from
// reports". That ordering is deliberate and is left alone. What it costs is
// the report: `--from file://nope` answered
// `cannot import a migration directory already in "atlas" format` for a
// directory that does not exist. The pinned community binary answers
// `sql/migrate: stat nope: no such file or directory` there, and answers the
// format refusal only for a source that exists — so the existence check is a
// read-only stat at this boundary, ahead of the importer, and the importer's
// own order is untouched. Native `ptah migrations import` takes a plain
// `--source-dir` path through a different command and is unaffected.
func resolveAtlasMigrateImportSource(
	opts atlasmigrateimport.Options,
) (atlasmigrateimport.Options, string, error) {
	// The scheme requirement is first, and on `--from` rather than `--to`:
	// measured, `--from src --to dst` with both spelled bare blames `--from`,
	// and `--from src --dir-format bogus` prints the scheme refusal rather
	// than the format one.
	if err := atlasargs.RequireDirScheme(opts.FromURL); err != nil {
		return opts, "", err
	}
	// The `import --from: ` prefix is the one this surface already printed for
	// a non-local source URL, kept verbatim: the pinned community binary has no
	// counterpart for a registry directory, so there is nothing to match and
	// nothing to gain from renaming a diagnostic that is already ours.
	from, err := atlasargs.ParseLocalDir(opts.FromURL)
	if err != nil {
		return opts, "", fmt.Errorf("import --from: %w", err)
	}
	format, err := atlasmigrate.ResolveApplyDirFormat(opts.DirFormat, from.Query)
	if err != nil {
		return opts, "", atlasDirFormatError("import", atlasDirFormatSpelling(from.Query), err)
	}
	opts.FromURL = atlasDirWithoutQuery(opts.FromURL)
	opts.DirFormat = string(format)
	// An empty path is `file://` alone, which the importer reads as the working
	// directory. Statting the same thing it will open keeps one answer to
	// "which directory is the source".
	fromDir := from.Path
	if fromDir == "" {
		fromDir = "."
	}
	return opts, fromDir, nil
}

// verifyAtlasImportSourceChecksum enforces the atlas.sum integrity gate on the
// captured SOURCE directory of an import.
//
// It is the verb-neutral gate under the import policy — a present atlas.sum
// must verify, an absent one is not an error — and it is a call site rather
// than a second verifier on purpose. `migrate import` reads the same
// directories, through the same capture, over the same per-format covered set
// as the verbs that already refuse a drifted one (`migrate apply`, `status` and
// `set`) and the two that compute that set (`hash` and `validate`). Expressing
// the rule twice is how two views of one directory drift apart (#974, #976).
//
// The refusal is returned unwrapped, like every other caller of the gate: the
// migratevalidate helpers have already written the checksum block to stdout and
// return an exitcode.New(1, ...) error the root command prints as
// `Error: checksum mismatch`. Routing it through cmdutil.Fail would prepend
// `error: ` and move the text to the other stream, losing the byte parity with
// the pinned community binary that is the point.
//
// The native Atlas branch of the gate is unreachable here: an atlas-format
// source is refused by [atlasmigrateimport.CaptureImport] before the capture,
// because importing a directory already in the target layout is refused by both
// tools.
func verifyAtlasImportSourceChecksum(
	cmd *cobra.Command,
	fsys fs.FS,
	format atlasmigrateimport.Format,
) error {
	return verifyCoveredAtlasDirChecksum(cmd, fsys, format, verifyAtlasSumWhenPresent)
}
