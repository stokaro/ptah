package atlas

import (
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

Flyway repeatable migrations are rejected explicitly until Ptah has an
executable representation for Atlas R-suffixed imported migrations.`,
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
func runAtlasMigrateImport(cmd *cobra.Command, opts atlasmigrateimport.Options) error {
	if _, err := atlasmigrateimport.Import(opts); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	return nil
}
