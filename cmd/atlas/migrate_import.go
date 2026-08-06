package atlas

import (
	"io/fs"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
)

func newAtlasMigrateImportCommand() *cobra.Command {
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

Flyway repeatable migrations are converted, on a reserved version slot above
every versioned migration. The destination file name carries that slot rather
than an R suffix, because Ptah cannot execute an R-suffixed Atlas migration.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAtlasMigrateImport(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.FromURL, "from", "file://migrations", "Source migration directory URL")
	flags.StringVar(&opts.ToURL, "to", "file://migrations", "Destination migration directory URL")
	flags.StringVar(&opts.DirFormat, "dir-format", "atlas", "Source migration directory format: atlas, golang-migrate, goose, flyway, liquibase, or dbmate")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
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
func runAtlasMigrateImport(cmd *cobra.Command, opts atlasmigrateimport.Options) error {
	captured, err := atlasmigrateimport.CaptureImport(opts)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if err := verifyAtlasImportSourceChecksum(cmd, captured.Source, captured.Format); err != nil {
		return err
	}
	if _, err := captured.Write(); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	return nil
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
