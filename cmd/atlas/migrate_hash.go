package atlas

import (
	"io"
	"os"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/migratehash"
	"go.5x5.cz/ptah/internal/atlasargs"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// newAtlasMigrateHashCommand returns `atlas migrate hash`. A native Atlas
// directory forwards to `ptah migrations hash` unchanged; a directory in a
// foreign tool's layout, named with either spelling Atlas accepts, is hashed
// over that tool's file set here.
func newAtlasMigrateHashCommand() *cobra.Command {
	return newAtlasMigrateIntegrityCommand(atlasMigrateHashVerb(), runAtlasMigrateHash)
}

func newSilentNativeMigrateHashCommand() *cobra.Command {
	cmd := migratehash.NewMigrateHashCommand()
	run := cmd.RunE
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		// Atlas CE writes only the checksum file on success. The adapter creates
		// this native target for each execution, so suppressing its progress does
		// not retain a writer on the reusable compatibility command.
		cmd.SetOut(io.Discard)
		return run(cmd, args)
	}
	return cmd
}

func atlasMigrateHashVerb() atlasVerb {
	return atlasVerb{
		use:     "hash",
		short:   "Write or update the migration directory checksum",
		native:  "migrations hash",
		factory: newSilentNativeMigrateHashCommand,
		flags: []atlasargs.Flag{
			atlasargs.NativeLocalDir("dir", "", "Migration directory", "dir"),
			atlasMigrateDirFormatFlag("dir-format"),
		},
	}
}

// runAtlasMigrateHash writes atlas.sum over the source files the selected
// foreign layout covers.
//
// The file set and its order come from atlasmigrateimport.SumFileNames, which
// reproduces Atlas CE's per-format selection, and the rolling hash from
// migratesum.ComputeAtlasFiles — the same pair `migrate validate` verifies
// with, so a directory this writes is one that verifies.
func runAtlasMigrateHash(cmd *cobra.Command, source atlasMigrateSource) error {
	if err := cmdutil.StatDir(source.dir); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	fsys := os.DirFS(source.dir)
	names, err := atlasmigrateimport.SumFileNames(fsys, source.format)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	sum, err := migratesum.ComputeAtlasFiles(fsys, names)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if err := migratesum.WritePrecomputedWithFormat(source.dir, migrator.MigrationDirFormatAtlas, sum); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	return nil
}
