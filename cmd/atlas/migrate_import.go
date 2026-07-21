package atlas

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/internal/atlasmigrateimport"
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

func runAtlasMigrateImport(cmd *cobra.Command, opts atlasmigrateimport.Options) error {
	result, err := atlasmigrateimport.Import(opts)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	out := cmd.OutOrStdout()
	fmt.Fprintln(out, "Imported migration files:")
	for _, file := range result.Files {
		fmt.Fprintln(out, file)
	}
	fmt.Fprintf(out, "Wrote %s\n", result.SumFile)
	return nil
}
