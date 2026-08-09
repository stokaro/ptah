// Package migratecheckpoint implements `ptah migrations checkpoint`, which
// squashes a migration directory's history into a cumulative-schema checkpoint.
package migratecheckpoint

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/cmd/internal/editor"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/dblock"
	"go.5x5.cz/ptah/internal/migrateops"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/migrationversion"
	"go.5x5.cz/ptah/migration/generator"
	"go.5x5.cz/ptah/migration/migrator"
)

const (
	migrationsFlag           = "migrations-dir"
	shadowDBFlag             = "shadow-db"
	dialectFlag              = "dialect"
	descriptionFlag          = "description"
	versionFlag              = "version"
	dryRunFlag               = "dry-run"
	dirFormatFlag            = "dir-format"
	qualifierFlag            = "qualifier"
	editFlag                 = "edit"
	editorFlag               = "editor"
	migrationLockTimeoutFlag = "migration-lock-timeout"
)

type options struct {
	migrationsDir        string
	shadowDB             string
	dialect              string
	description          string
	version              string
	dryRun               bool
	dirFormat            string
	schemas              string
	connectTimeout       string
	qualifier            string
	edit                 bool
	editor               string
	migrationLockTimeout string
}

// NewMigrateCheckpointCommand returns the `checkpoint` command.
func NewMigrateCheckpointCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "checkpoint",
		Short: "Squash migration history into a cumulative-schema checkpoint",
		Long: `Write a checkpoint migration that captures the full cumulative schema at the
current version.

The entire migration directory is replayed on a fresh shadow database, the
resulting schema is introspected, and a checkpoint migration is written. A fresh
database then bootstraps from the newest checkpoint instead of replaying all
history, while an already-migrated database ignores it.

--dir-format selects the checkpoint convention:

  ptah   a reversible pair, NNNNNNNNNN_description.checkpoint.up.sql and
         .checkpoint.down.sql, with ptah.sum refreshed (default)
  atlas  a single up-only file, <version>_description.sql whose first line is
         the "-- atlas:checkpoint" directive, with atlas.sum refreshed`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return migrateCheckpointCommand(cmd, args, &opts)
		},
	}
	registerFlags(cmd, &opts)
	cmdutil.ConfigureCommand(cmd)
	return cmd
}

func registerFlags(cmd *cobra.Command, opts *options) {
	flags := cmd.Flags()
	flags.StringVar(&opts.migrationsDir, migrationsFlag, "./migrations", "Directory containing migration files")
	flags.StringVar(&opts.shadowDB, shadowDBFlag, "", "Ephemeral shadow database URL the directory is replayed into (required)")
	flags.StringVar(&opts.dialect, dialectFlag, "", "Database dialect; inferred from the shadow database when omitted")
	flags.StringVar(&opts.description, descriptionFlag, "checkpoint", "Checkpoint description used in the file name")
	flags.StringVar(&opts.version, versionFlag, "", "Checkpoint version; defaults to one above the newest migration")
	flags.BoolVar(&opts.dryRun, dryRunFlag, false, "Print the checkpoint SQL instead of writing files")
	flags.StringVar(&opts.dirFormat, dirFormatFlag, string(migrator.MigrationDirFormatPtah), "Migration directory format")
	flags.StringVar(&opts.qualifier, qualifierFlag, "", "Qualify every object in the checkpoint with a custom schema qualifier (single-schema checkpoints only)")
	flags.BoolVar(&opts.edit, editFlag, false, "Open the written checkpoint files in an editor before reporting them (the directory checksum is refreshed afterwards)")
	flags.StringVar(&opts.editor, editorFlag, "", "Editor command used with --edit (defaults to $VISUAL, then $EDITOR)")
	flags.StringVar(&opts.migrationLockTimeout, migrationLockTimeoutFlag, "", "Maximum time to wait for the shadow database's migration advisory lock during the replay (for example 10s). Empty waits indefinitely.")
	dbcli.RegisterSchemasFlag(flags, &opts.schemas)
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
}

func migrateCheckpointCommand(cmd *cobra.Command, _ []string, opts *options) error {
	ctx := cmd.Context()
	out := cmd.OutOrStdout()

	if opts.shadowDB == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("a shadow database URL is required (--%s)", shadowDBFlag))
	}
	if opts.migrationsDir == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("migrations directory is required"))
	}

	dirFormat, err := migrator.ParseMigrationDirFormat(opts.dirFormat)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	// Checkpoints are written in the ptah two-file convention
	// (NNNNNNNNNN_name.checkpoint.up.sql / .down.sql plus ptah.sum) or in the
	// Atlas convention (<version>_name.sql carrying the `-- atlas:checkpoint`
	// directive, plus atlas.sum). "auto" selects neither: it is a READ-side
	// probe, and writing under it would have to guess which convention and
	// which integrity file the directory wants, leaving a mixed-format
	// directory with a stale sum. Refuse it up front rather than reporting
	// success and corrupting the directory.
	if dirFormat == migrator.MigrationDirFormatAuto {
		return cmdutil.Fail(cmd, fmt.Errorf(
			"checkpoint cannot write under --%s=%s: name the target convention with %s or %s",
			dirFormatFlag, migrator.MigrationDirFormatAuto,
			migrator.MigrationDirFormatPtah, migrator.MigrationDirFormatAtlas,
		))
	}
	if err := checkIntegrityFileConflict(opts.migrationsDir, dirFormat); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	connectTimeout, err := dbcli.ParseConnectTimeout(opts.connectTimeout)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	migrationLockTimeout, err := migrator.ParseMigrationLockTimeout(opts.migrationLockTimeout)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	warnUnlockableShadowDialect(cmd, opts)
	// Both edit preconditions are checked before the replay, not after the
	// files are written: the replay drops and rebuilds the shadow database, and
	// a run that is going to refuse must not pay for that or leave a checkpoint
	// behind that nobody could edit.
	if err := checkEditPreconditions(cmd, opts); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	version, err := resolveCheckpointVersion(opts.version, opts.migrationsDir, dirFormat)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	upSQL, downSQL, err := generator.GenerateCheckpointFromShadow(ctx, generator.CheckpointFromShadowOptions{
		ShadowDatabaseURL:    opts.shadowDB,
		MigrationsDir:        opts.migrationsDir,
		Dialect:              opts.dialect,
		Schemas:              dbcli.ParseSchemas(opts.schemas),
		ProviderOptions:      []migrator.FSProviderOption{migrator.WithMigrationDirFormat(dirFormat)},
		ConnectTimeout:       connectTimeout,
		MigrationLockTimeout: migrationLockTimeout,
		SchemaQualifier:      opts.qualifier,
	})
	if err != nil {
		return err
	}

	if dirFormat == migrator.MigrationDirFormatAtlas {
		return writeAtlasCheckpoint(cmd, out, opts, version, upSQL)
	}

	if opts.dryRun {
		upName := migrator.GenerateCheckpointMigrationFileName(version, opts.description, "up")
		downName := migrator.GenerateCheckpointMigrationFileName(version, opts.description, "down")
		fmt.Fprintf(out, "-- checkpoint version %d (dry run, no files written)\n\n-- %s\n%s\n\n-- %s\n%s\n",
			version, upName, upSQL, downName, downSQL)
		return nil
	}

	upPath, downPath, err := generator.WriteCheckpointFiles(opts.migrationsDir, version, opts.description, upSQL, downSQL)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if err := editWrittenCheckpoint(cmd, opts, dirFormat, upPath, downPath); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	fmt.Fprintf(out, "Wrote checkpoint version %d:\n  %s\n  %s\n", version, upPath, downPath)
	return nil
}

// warnUnlockableShadowDialect says so when a lock timeout was asked for on a
// shadow database whose dialect implements no advisory locking (SQLite, and
// ClickHouse), where the replay takes no lock and the timeout therefore bounds
// nothing.
//
// Silence would be the worse answer: an operator who set a bound would believe
// concurrent replays against one shadow database are serialized. The same
// disclosure exists on `schema apply` for the same reason.
//
// It is emitted only when the flag was set explicitly, so an ordinary SQLite
// checkpoint stays quiet, and on stderr, so it never contaminates the
// checkpoint SQL a --dry-run writes to stdout.
func warnUnlockableShadowDialect(cmd *cobra.Command, opts *options) {
	if !cmd.Flags().Changed(migrationLockTimeoutFlag) {
		return
	}
	dialect := opts.dialect
	if dialect == "" {
		// A URL Ptah cannot classify is not evidence either way, so an error
		// here means no claim is made rather than a wrong one.
		dialect, _ = atlasurl.DialectFromURL(opts.shadowDB)
	}
	if dialect == "" || dblock.Supported(dialect) {
		return
	}
	fmt.Fprintf(cmd.ErrOrStderr(),
		"note: migration locking is not supported for dialect %q; --%s is ignored and the checkpoint replay proceeds without a lock\n",
		dialect, migrationLockTimeoutFlag,
	)
}

// checkEditPreconditions refuses an --edit run that could not finish, before
// any database work happens.
//
// Two things can make an editor session impossible, and both are silent
// failures if left to the editor itself: no editor is configured (the run would
// write a checkpoint and then stop), and no terminal is attached (an
// interactive editor started without one blocks forever, which in CI is
// indistinguishable from a hang). Refusing up front turns both into an exit
// code and a message.
//
// The sibling `ptah migrations create --edit` deliberately keeps its existing
// behavior; changing it is a separate change with its own blast radius.
func checkEditPreconditions(cmd *cobra.Command, opts *options) error {
	if !opts.edit {
		if opts.editor != "" {
			return fmt.Errorf("--%s requires --%s", editorFlag, editFlag)
		}
		return nil
	}
	if _, err := editor.Resolve(opts.editor); err != nil {
		if errors.Is(err, editor.ErrNoEditor) {
			return fmt.Errorf("%w, or pass --%s", err, editorFlag)
		}
		return err
	}
	return editor.RequireInteractive(cmd.InOrStdin())
}

// editWrittenCheckpoint opens the checkpoint files just written and refreshes
// the directory's integrity file, because editing changes bytes the write path
// already hashed. Both conventions maintain a sum (ptah.sum, atlas.sum), so the
// refresh is unconditional here, unlike on `migrations create`, where the ptah
// path maintains none.
func editWrittenCheckpoint(
	cmd *cobra.Command,
	opts *options,
	dirFormat migrator.MigrationDirFormat,
	paths ...string,
) error {
	if !opts.edit {
		return nil
	}
	if err := editor.Open(cmd.Context(), opts.editor, paths...); err != nil {
		return err
	}
	if _, err := migrateops.Rehash(opts.migrationsDir, dirFormat); err != nil {
		return fmt.Errorf("refresh %s after editing: %w", integrityFileName(dirFormat), err)
	}
	return nil
}

func integrityFileName(dirFormat migrator.MigrationDirFormat) string {
	if dirFormat == migrator.MigrationDirFormatAtlas {
		return migratesum.AtlasFileName
	}
	return migratesum.FileName
}

// writeAtlasCheckpoint emits the Atlas single-file checkpoint convention. The
// down body the generator produced is deliberately discarded: the Atlas format
// is up-only and measured Atlas checkpoints have no down file, so writing one
// would put a file in the directory that Atlas cannot read.
func writeAtlasCheckpoint(cmd *cobra.Command, out io.Writer, opts *options, version int64, upSQL string) error {
	if opts.dryRun {
		name, contents := generator.AtlasCheckpointArtifact(version, opts.description, upSQL)
		fmt.Fprintf(out, "-- checkpoint version %d (dry run, no files written)\n\n-- %s\n%s", version, name, contents)
		return nil
	}

	path, err := generator.WriteAtlasCheckpointFile(opts.migrationsDir, version, opts.description, upSQL)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if err := editWrittenCheckpoint(cmd, opts, migrator.MigrationDirFormatAtlas, path); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	fmt.Fprintf(out, "Wrote checkpoint version %d:\n  %s\n", version, path)
	return nil
}

// checkIntegrityFileConflict refuses to write a checkpoint whose convention
// would leave the directory carrying a second integrity file.
//
// Each format refreshes its own sum: ptah writes ptah.sum, atlas writes
// atlas.sum. A directory holding both is ambiguous — `--dir-format auto`
// refuses to read it at all ("both ptah.sum and atlas.sum exist") — and the
// checkpoint command would otherwise exit 0 and surface the damage on some
// later command instead. Writing the checkpoint is the step that creates the
// second file, so it is the step that refuses.
func checkIntegrityFileConflict(migrationsDir string, dirFormat migrator.MigrationDirFormat) error {
	writes, foreign := migratesum.AtlasFileName, migratesum.FileName
	if dirFormat == migrator.MigrationDirFormatPtah {
		writes, foreign = migratesum.FileName, migratesum.AtlasFileName
	}
	switch _, err := os.Stat(filepath.Join(migrationsDir, foreign)); {
	case err == nil:
		return fmt.Errorf(
			"cannot write %s-format checkpoint files into a directory that already has %s: it would leave both %s and %s behind, which --%s=%s refuses to read; re-hash the directory into one format first",
			dirFormat, foreign, foreign, writes, dirFormatFlag, migrator.MigrationDirFormatAuto,
		)
	case !errors.Is(err, os.ErrNotExist):
		return fmt.Errorf("failed to inspect migrations directory: %w", err)
	}
	return checkPtahFileConflict(migrationsDir, dirFormat)
}

// checkPtahFileConflict refuses an Atlas-format checkpoint in a directory that
// holds ptah-convention migration files, whether or not it has been hashed.
//
// The integrity-file check above only catches directories somebody already ran
// `migrations hash` on. An unhashed ptah directory has neither sum file, so it
// passes that check — and then the two auto-mode rules disagree about the
// result forever: discovery prefers the ptah files and never sees the
// checkpoint, while verification finds the atlas.sum this command wrote and
// reports the directory as valid. The checkpoint ends up permanently invisible
// and permanently integrity-covered, which is worse than either failure alone.
//
// The test is content-shaped, so it holds for both. A ptah file name carries a
// direction component that ParseMigrationFileName requires, so a pure Atlas
// directory yields zero matches and never trips this.
//
// The converse (Atlas-shaped files under --dir-format=ptah) is NOT symmetric
// and is deliberately not checked here: the Atlas name grammar also accepts
// ptah's own `NNNNNNNNNN_name.up.sql`, so every ptah directory would match it
// and the guard would refuse the format's ordinary use.
func checkPtahFileConflict(migrationsDir string, dirFormat migrator.MigrationDirFormat) error {
	if dirFormat != migrator.MigrationDirFormatAtlas {
		return nil
	}
	ptahFiles, err := migrator.DiscoverMigrationFiles(os.DirFS(migrationsDir), migrator.MigrationDirFormatPtah)
	if err != nil || len(ptahFiles) == 0 {
		// A directory with no readable ptah files is exactly the case this
		// guard must let through, and DiscoverMigrationFiles reports "nothing
		// matched this format" as an error, so a failure here is not evidence
		// of a conflict.
		return nil
	}
	return fmt.Errorf(
		"cannot write %s-format checkpoint files into a directory that holds ptah-format migrations (%s): "+
			"the checkpoint would be invisible to format auto-detection, which reads the ptah files instead; "+
			"convert the directory first or pass --%s=%s",
		dirFormat, ptahFiles[0].Path, dirFormatFlag, migrator.MigrationDirFormatPtah,
	)
}

// maxMigrationVersion is the largest value the 10-digit NNNNNNNNNN file-name
// prefix can hold. A larger version would produce a file name the migration
// parser cannot recognize, so the checkpoint would be silently invisible.
const maxMigrationVersion = migrationversion.PtahMax

// ptahVersionWidth is the digit count of a ptah migration file-name prefix.
// Atlas auto-detection deliberately refuses a suffixless Atlas name whose
// version is exactly this wide, because it cannot be told apart from a ptah
// name. An Atlas checkpoint at such a version is therefore invisible to any
// reader that has not been told the format explicitly — the same silent-
// invisibility failure maxMigrationVersion guards on the ptah side.
const ptahVersionWidth = 10

// resolveCheckpointVersion returns the checkpoint version.
//
// Without --version the default depends on the target convention. The ptah
// format counts: one above the newest migration. The Atlas format timestamps,
// as Atlas itself was measured to do. Either way the checkpoint must sort
// after every migration the replay covered, so that a fresh database runs the
// checkpoint alone.
//
// The two rules are combined rather than trusted individually: the timestamp
// is only used when it already outranks the whole history, and otherwise falls
// back to the counter. [generator.ResolveAtlasCheckpointVersion] scans only the
// top level, while the replay and the reader recurse, so on a directory with a
// nested future-dated migration the timestamp alone sorts BELOW a file whose
// SQL the checkpoint already contains — the fresh database would then run the
// checkpoint and replay that migration on top of it, which is the double-apply
// of stokaro/ptah#954. `latest` here comes from the recursive walk, so it sees
// what the timestamp cannot.
//
// An explicit --version must be a positive value that fits the format's
// file-name shape and is above every existing migration — otherwise the
// checkpoint would overwrite history, collide with an ordinary migration, or
// fail to squash it — so those are rejected up front rather than producing a
// directory that only breaks on a later command.
func resolveCheckpointVersion(explicit, migrationsDir string, dirFormat migrator.MigrationDirFormat) (int64, error) {
	files, err := migrator.DiscoverMigrationFiles(os.DirFS(migrationsDir), dirFormat)
	if err != nil {
		return 0, fmt.Errorf("failed to scan migrations directory: %w", err)
	}
	var latest int64
	for _, file := range files {
		if file.Version > latest {
			latest = file.Version
		}
	}

	atlas := dirFormat == migrator.MigrationDirFormatAtlas

	if explicit == "" {
		if atlas {
			if version := generator.ResolveAtlasCheckpointVersion(migrationsDir); version > latest {
				return version, nil
			}
			return migrationversion.Next(latest, migrator.MigrationDirFormatAtlas)
		}
		return migrationversion.Next(latest, migrator.MigrationDirFormatPtah)
	}

	version, err := strconv.ParseInt(explicit, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid --%s value %q: %w", versionFlag, explicit, err)
	}
	if version <= 0 {
		return 0, fmt.Errorf("invalid --%s value %d: must be a positive version", versionFlag, version)
	}
	if atlas {
		// The rendered value, not the flag text: the file name is written with
		// %d, so `--version 01234567890` renders as 1234567890 — exactly the
		// ten-digit name this guard exists to prevent, while len(explicit) is
		// eleven.
		if rendered := strconv.FormatInt(version, 10); len(rendered) == ptahVersionWidth {
			return 0, fmt.Errorf(
				"invalid --%s value %s: an Atlas checkpoint version of exactly %d digits is indistinguishable from a ptah migration name and is skipped by format auto-detection",
				versionFlag, rendered, ptahVersionWidth,
			)
		}
	} else if version > maxMigrationVersion {
		return 0, fmt.Errorf("invalid --%s value %d: exceeds the maximum migration version %d", versionFlag, version, int64(maxMigrationVersion))
	}
	if version <= latest {
		return 0, fmt.Errorf("invalid --%s value %d: must be above the newest existing migration version %d so the checkpoint covers the whole history", versionFlag, version, latest)
	}
	return version, nil
}
