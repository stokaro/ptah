package atlas

import (
	"errors"
	"fmt"
	"io/fs"
	"os"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/migratevalidate"
	"go.5x5.cz/ptah/internal/atlasargs"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/migrationreplay"
	"go.5x5.cz/ptah/migration/migrator"
)

// newAtlasMigrateValidateCommand returns `atlas migrate validate`. A native
// Atlas directory forwards to `ptah migrations validate` unchanged; a directory
// in a foreign tool's layout, named with either spelling Atlas accepts, is
// verified against that tool's file set here.
func newAtlasMigrateValidateCommand() *cobra.Command {
	return newAtlasMigrateIntegrityCommand(atlasMigrateValidateVerb(), runAtlasMigrateValidate)
}

func atlasMigrateValidateVerb() atlasVerb {
	return atlasVerb{
		use:     "validate",
		short:   "Validate migration directory integrity",
		native:  "migrations validate",
		factory: migratevalidate.NewAtlasMigrateValidateCommand,
		flags: []atlasargs.Flag{
			atlasargs.NativeString("dev-url", "", "Dev database URL", "dev-url"),
			atlasargs.NativeLocalDir("dir", "", "Migration directory", "dir"),
			atlasMigrateDirFormatFlag("dir-format"),
		},
	}
}

// runAtlasMigrateValidate verifies atlas.sum over the source files the selected
// foreign layout covers, and replays them on --dev-url when one is given.
//
// Every refusal is rendered by the same migratevalidate helpers the native
// Atlas-format path and the apply-time gate use, so a converted directory is
// refused byte-identically to a native one: an unhashed directory reports
// "checksum file not found", drift reports "checksum mismatch" with the
// `L<line>: <file> was <reason>` line naming the SOURCE file, and a clean
// directory prints nothing at all.
//
// Integrity is checked before the directory is parsed, matching Atlas CE:
// a tampered file that the source tool can no longer parse is still reported as
// a checksum mismatch rather than as a conversion failure.
func runAtlasMigrateValidate(cmd *cobra.Command, source atlasMigrateSource) error {
	if err := cmdutil.StatDir(source.dir); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	fsys := os.DirFS(source.dir)
	names, err := atlasmigrateimport.SumFileNames(fsys, source.format)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	result, hashed, err := migratesum.VerifyAtlasFilesHashed(fsys, names)
	switch {
	case errors.Is(err, migratesum.ErrSumFileMalformed):
		// A malformed atlas.sum has no entry-level mismatch to point at.
		return migratevalidate.FailAtlasChecksumMismatch(cmd, nil)
	case err != nil:
		return cmdutil.Fail(cmd, err)
	case !hashed:
		return migratevalidate.FailAtlasChecksumFileNotFound(cmd)
	case !result.OK():
		return migratevalidate.FailAtlasChecksumMismatch(cmd, result.FirstMismatch())
	}

	if source.devURL == "" {
		return nil
	}
	return replayAtlasMigrateSource(cmd, source, fsys)
}

// replayAtlasMigrateSource replays a converted migration directory on the dev
// database. It converts through the same ResolveApplySourceForFormat the apply
// path executes, so what validate proves runnable is what apply would run.
func replayAtlasMigrateSource(cmd *cobra.Command, source atlasMigrateSource, fsys fs.FS) error {
	converted, err := atlasmigrate.ResolveApplySourceForFormat(fsys, source.dir, source.format)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	err = migrationreplay.Replay(cmd.Context(), migrationreplay.Options{
		Dir:       source.dir,
		DirFormat: migrator.MigrationDirFormatAtlas,
		DevURL:    source.devURL,
		FS:        converted,
	})
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("error validating migration SQL on dev database: %w", err))
	}
	return nil
}
