// Package migratemaint holds the flag surface and applied-migration guard shared
// by the migration-directory maintenance commands (`ptah migrations rm | edit |
// rebase`, #662). Each command mutates the on-disk migration set through
// internal/migrateops and refuses to rewrite already-applied history.
package migratemaint

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/spf13/pflag"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/migrateops"
	"go.5x5.cz/ptah/migration/migrator"
)

// Options are the flags common to every maintenance command.
type Options struct {
	DBURL         string
	MigrationsDir string
	Version       string
	DirFormat     string
	Force         bool
	AtlasEnv      string

	ConnectTimeout      string
	MigrationsSchema    string
	MigrationsTable     string
	RevisionTableFormat string
}

// RegisterFlags installs the shared maintenance flags on a command.
func RegisterFlags(flags *pflag.FlagSet, opts *Options) {
	flags.StringVar(&opts.MigrationsDir, "migrations-dir", "./migrations", "Directory containing migration files")
	flags.StringVar(&opts.Version, "version", "", "Migration version to operate on (required)")
	flags.StringVar(&opts.DirFormat, "dir-format", string(migrator.MigrationDirFormatAuto), "Migration directory format: auto, ptah, or atlas")
	flags.StringVar(&opts.DBURL, "db-url", "", "Database URL used to verify applied state; when omitted, applied state is not checked")
	flags.BoolVar(&opts.Force, "force", false, "Proceed even when the migration is already applied")
	flags.StringVar(&opts.AtlasEnv, "atlas-env", "", "Value exposed as .Env when rendering Atlas SQL template migrations")
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.ConnectTimeout)
	dbcli.RegisterMigrationsSchemaFlag(flags, &opts.MigrationsSchema)
	dbcli.RegisterMigrationsTableFlag(flags, &opts.MigrationsTable)
	dbcli.RegisterRevisionTableFormatFlag(flags, &opts.RevisionTableFormat)
}

// Resolved holds the validated directory, version, and format for an operation.
type Resolved struct {
	Dir     string
	Version int64
	Format  migrator.MigrationDirFormat
}

// Resolve validates the directory, parses the version, and parses the directory
// format.
func (o *Options) Resolve() (Resolved, error) {
	if o.MigrationsDir == "" {
		return Resolved{}, fmt.Errorf("--migrations-dir is required")
	}
	if err := cmdutil.StatDir(o.MigrationsDir); err != nil {
		return Resolved{}, err
	}
	if o.Version == "" {
		return Resolved{}, fmt.Errorf("--version is required")
	}
	version, err := strconv.ParseInt(o.Version, 10, 64)
	if err != nil || version <= 0 {
		return Resolved{}, fmt.Errorf("invalid --version %q: must be a positive integer", o.Version)
	}
	format, err := migrator.ParseMigrationDirFormat(o.DirFormat)
	if err != nil {
		return Resolved{}, err
	}
	return Resolved{Dir: o.MigrationsDir, Version: version, Format: format}, nil
}

// Guard refuses to modify an already-applied version. With --db-url it queries
// the applied set and enforces migrateops.EnsureNotApplied; without --db-url it
// warns to w that applied state could not be verified and proceeds (mirroring how
// hash treats the database as optional).
func (o *Options) Guard(ctx context.Context, w io.Writer, version int64, format migrator.MigrationDirFormat) error {
	if o.DBURL == "" {
		fmt.Fprintln(w, "warning: --db-url not provided; applied migration state was not verified")
		return nil
	}
	if o.Force {
		return nil
	}

	revisionFormat, err := migrator.ParseRevisionTableFormat(o.RevisionTableFormat)
	if err != nil {
		return err
	}
	connectTimeout, err := dbcli.ParseConnectTimeout(o.ConnectTimeout)
	if err != nil {
		return err
	}

	connectCtx, cancelConnect := dbcli.ConnectContext(ctx, connectTimeout)
	conn, err := dbschema.ConnectToDatabase(connectCtx, o.DBURL)
	cancelConnect()
	if err != nil {
		return fmt.Errorf("error connecting to database: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	mig, err := migrator.NewFSMigrator(
		conn,
		os.DirFS(o.MigrationsDir),
		migrator.WithMigrationDirFormat(format),
		migrator.WithAtlasTemplateData(migrator.AtlasTemplateData{Env: o.AtlasEnv}),
	)
	if err != nil {
		return fmt.Errorf("error preparing migrator: %w", err)
	}
	mig = mig.WithMigrationsTable(o.MigrationsSchema, o.MigrationsTable).
		WithRevisionTableFormat(revisionFormat)

	applied, err := mig.GetAppliedMigrations(ctx)
	if err != nil {
		return fmt.Errorf("error reading applied migrations: %w", err)
	}
	return migrateops.EnsureNotApplied(applied, version)
}
