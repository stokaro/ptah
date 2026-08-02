package atlas

import (
	"fmt"
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

func atlasMigrateHashVerb() atlasVerb {
	return atlasVerb{
		use:     "hash",
		short:   "Write or update the migration directory checksum",
		native:  "migrations hash",
		factory: migratehash.NewMigrateHashCommand,
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
//
// The success output is the native command's, not Atlas CE's: CE writes
// nothing at all on a successful hash. That divergence predates this path and
// is tracked separately; reproducing the native output here keeps the two
// layouts of the same verb consistent with each other.
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

	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "Wrote %s/%s\n", source.dir, migratesum.AtlasFileName)
	fmt.Fprintf(out, "%d migration file(s) hashed\n", len(sum.Entries))
	return nil
}
