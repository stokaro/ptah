// Package migratetag implements the native `ptah migrations tag` command: it
// records, lists, and removes the tags that name a migration-directory state,
// so `ptah migrations down --to-tag` has something to resolve against.
package migratetag

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing/fstest"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

const (
	dbURLFlag   = "db-url"
	versionFlag = "version"
	deleteFlag  = "delete"
)

type options struct {
	dbURL               string
	version             string
	remove              bool
	connectTimeout      string
	configPath          string
	envName             string
	migrationsSchema    string
	migrationsTable     string
	revisionTableFormat string
}

// NewMigrateTagCommand returns the migrations tag command.
func NewMigrateTagCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "tag [name]",
		Short: "Record, list, or remove a migration tag",
		Long: `Record a tag naming the migration version a database has reached, list the
tags recorded against it, or remove one.

A tag names a migration-directory state, the way a registry tag names an
artifact, and "ptah migrations down --to-tag" reverts to the version a tag
selects. Recording a tag that already exists moves it: these are movable
pointers, like the tags "ptah migrations push --tag" writes.

With no name, every recorded tag is listed. With a name and --version, the tag
is recorded at that version; with a name alone, it is recorded at the version
the database has currently reached. With --delete, the named tag is removed.

Tags are metadata. Recording or removing one never runs migration SQL and
never changes which migrations are applied.`,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMigrateTag(cmd, opts, args)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.dbURL, dbURLFlag, "", "Database URL (required). Example: postgres://localhost:5432/dbname")
	flags.StringVar(&opts.version, versionFlag, "",
		"Migration version the tag names (default: the version the database has reached)")
	flags.BoolVar(&opts.remove, deleteFlag, false, "Remove the named tag instead of recording it")
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
	dbcli.RegisterConfigFlag(flags, &opts.configPath)
	dbcli.RegisterEnvFlag(flags, &opts.envName)
	dbcli.RegisterMigrationsSchemaFlag(flags, &opts.migrationsSchema)
	dbcli.RegisterMigrationsTableFlag(flags, &opts.migrationsTable)
	dbcli.RegisterRevisionTableFormatFlag(flags, &opts.revisionTableFormat)
	cmdutil.ConfigureCommandArgs(cmd, cobra.MaximumNArgs(1))
	return cmd
}

func runMigrateTag(cmd *cobra.Command, opts options, args []string) error {
	projectCfg, err := dbcli.LoadProjectConfig(cmd, opts.configPath)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	opts.dbURL = dbcli.EffectiveString(cmd, dbURLFlag, opts.dbURL,
		projectCfg.StringValue(projectconfig.StringDatabaseURL))
	opts.migrationsSchema = dbcli.EffectiveString(cmd, dbcli.MigrationsSchemaFlagName,
		opts.migrationsSchema, projectCfg.StringValue(projectconfig.StringMigrationRevisionsSchema))
	opts.migrationsTable = dbcli.EffectiveString(cmd, dbcli.MigrationsTableFlagName,
		opts.migrationsTable, projectCfg.StringValue(projectconfig.StringMigrationRevisionsTable))
	opts.revisionTableFormat = dbcli.EffectiveString(cmd, dbcli.RevisionTableFormatFlagName,
		opts.revisionTableFormat, projectCfg.StringValue(projectconfig.StringMigrationRevisionFormat))
	connectTimeoutValue := dbcli.EffectiveString(cmd, dbcli.ConnectTimeoutFlagName,
		opts.connectTimeout, projectCfg.StringValue(projectconfig.StringMigrationConnectTimeout))

	if strings.TrimSpace(opts.dbURL) == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("database URL is required"))
	}
	name := ""
	if len(args) == 1 {
		name = args[0]
	}
	if opts.remove && name == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("--%s needs the name of the tag to remove", deleteFlag))
	}
	if name == "" && cmd.Flags().Changed(versionFlag) {
		// --version with nothing to attach it to would silently list instead
		// of recording, which reads as success for a command that did nothing.
		return cmdutil.Fail(cmd, fmt.Errorf("--%s needs a tag name to record it against", versionFlag))
	}
	revisionFormat, err := migrator.ParseRevisionTableFormat(opts.revisionTableFormat)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	connectTimeout, err := dbcli.ParseConnectTimeout(connectTimeoutValue)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	connectCtx, cancel := dbcli.ConnectContext(cmd.Context(), connectTimeout)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.dbURL)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("error connecting to database: %w", err))
	}
	defer dbschema.CloseAndWarn(conn)

	// The tag namespace and the current version are both revision-table
	// state, so this command registers no migrations: an empty filesystem
	// keeps it from reading a directory it never uses, and from failing on a
	// directory that happens to be malformed.
	mig, err := migrator.NewFSMigrator(conn, fstest.MapFS{})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	mig = mig.
		WithMigrationsTable(opts.migrationsSchema, opts.migrationsTable).
		WithRevisionTableFormat(revisionFormat)

	switch {
	case opts.remove:
		if err := mig.DeleteMigrationTag(cmd.Context(), name); err != nil {
			return cmdutil.Fail(cmd, err)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "Removed tag %s.\n", name)
		return nil
	case name != "":
		version, err := tagVersion(cmd, mig, opts)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		if err := mig.RecordMigrationTag(cmd.Context(), name, version); err != nil {
			return cmdutil.Fail(cmd, err)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "Tagged version %d as %s.\n", version, name)
		return nil
	}
	tags, err := mig.MigrationTags(cmd.Context())
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	return writeTags(cmd.OutOrStdout(), tags)
}

// tagVersion resolves what version a tag names: the one given on --version, or
// the version the database has currently reached.
func tagVersion(cmd *cobra.Command, mig *migrator.Migrator, opts options) (int64, error) {
	trimmed := strings.TrimSpace(opts.version)
	if trimmed == "" {
		version, err := mig.GetCurrentVersion(cmd.Context())
		if err != nil {
			return 0, fmt.Errorf("read the current migration version: %w", err)
		}
		return version, nil
	}
	version, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("--%s %q is not a valid migration version: %w", versionFlag, opts.version, err)
	}
	if version < 0 {
		return 0, fmt.Errorf("--%s must not be negative", versionFlag)
	}
	return version, nil
}

// writeTags renders the tag listing. An empty namespace says so in words
// rather than printing a header over nothing.
func writeTags(w io.Writer, tags []migrator.MigrationTag) error {
	if len(tags) == 0 {
		_, err := fmt.Fprintln(w, "No migration tags recorded.")
		return err
	}
	table := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "TAG\tVERSION\tRECORDED"); err != nil {
		return err
	}
	for _, tag := range tags {
		if _, err := fmt.Fprintf(table, "%s\t%d\t%s\n",
			tag.Tag, tag.Version, tag.RecordedAt.UTC().Format(time.RFC3339)); err != nil {
			return err
		}
	}
	return table.Flush()
}
