// Package migratelog implements the native `ptah migrations log` command: it
// reads the append-only record of migration operations a database has seen.
//
// The revision table answers what is applied; this answers what happened. A
// completed rollback deletes the revision row, so without this a database that
// was on version 43 yesterday reads exactly like one that never reached it
// (stokaro/ptah#3406).
package migratelog

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing/fstest"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"ptah.run/config/projectconfig"
	"ptah.run/dbschema"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/internal/cli/internal/migrateflags"
	"ptah.run/migration/migrator"
)

const (
	dbURLFlag = "db-url"
	limitFlag = "limit"
	jsonFlag  = "json"
)

type options struct {
	dbURL               string
	limit               int
	jsonOutput          bool
	connectTimeout      string
	configPath          string
	envName             string
	migrationsSchema    string
	migrationsTable     string
	migrationsEngine    string
	revisionTableFormat string
}

// NewMigrateLogCommand returns the migrations log command.
func NewMigrateLogCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "log",
		Short: "Show the recorded history of migration operations",
		Long: `Show what happened to this database, newest first.

Each line is one attempt: when it started, which migration it was about, which
direction it ran, how it ended, and who the run said it was. An attempt whose
process stopped mid-flight has no outcome and is reported as undetermined,
because a run that was killed says nothing about whether its work landed.

This reads a log, not an audit trail. The table lives in the database it
describes and is writable by the account that runs migrations: whoever can
apply a migration can edit the record of having done so. The actor is recorded
with where the name came from, because a name a caller supplied and the user
the process ran as are different claims.

"ptah migrations status" remains the answer to where the database stands. This
command never decides that.`,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runMigrateLog(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.dbURL, dbURLFlag, "", "Database URL (required). Example: postgres://localhost:5432/dbname")
	flags.IntVar(&opts.limit, limitFlag, 50, "Maximum attempts to show; 0 shows every recorded attempt")
	flags.BoolVar(&opts.jsonOutput, jsonFlag, false, "Output the log as JSON")
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
	dbcli.RegisterConfigFlag(flags, &opts.configPath)
	dbcli.RegisterEnvFlag(flags, &opts.envName)
	dbcli.RegisterMigrationsSchemaFlag(flags, &opts.migrationsSchema)
	dbcli.RegisterMigrationsTableFlag(flags, &opts.migrationsTable)
	dbcli.RegisterMigrationsEngineFlag(flags, &opts.migrationsEngine)
	dbcli.RegisterRevisionTableFormatFlag(flags, &opts.revisionTableFormat)
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runMigrateLog(cmd *cobra.Command, opts options) error {
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
	if opts.limit < 0 {
		return cmdutil.Fail(cmd, fmt.Errorf("--%s must not be negative", limitFlag))
	}
	revisionFormat, err := migrateflags.ParseRevisionTableFormat(opts.revisionTableFormat)
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

	// The log is revision-table state, so this command registers no
	// migrations: an empty filesystem keeps it from reading a directory it
	// never uses, and from failing on one that happens to be malformed.
	mig, err := migrator.NewFSMigrator(conn, fstest.MapFS{})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	mig = mig.
		WithMigrationsTable(opts.migrationsSchema, opts.migrationsTable).
		WithMigrationsEngine(opts.migrationsEngine).
		WithRevisionTableFormat(revisionFormat)

	attempts, err := mig.MigrationLog(cmd.Context(), opts.limit)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if opts.jsonOutput {
		return writeJSON(cmd.OutOrStdout(), attempts)
	}
	return writeTable(cmd.OutOrStdout(), attempts)
}

// logRecord is the JSON shape one attempt is published as.
//
// It is a shape of this package's own rather than the migrator's two entries,
// because a consumer wants the attempt: when it started, how it ended, and
// whether anybody knows.
type logRecord struct {
	StartedAt    time.Time  `json:"started_at"`
	FinishedAt   *time.Time `json:"finished_at,omitempty"`
	RunID        string     `json:"run_id"`
	Operation    string     `json:"operation"`
	Version      int64      `json:"version"`
	Outcome      string     `json:"outcome"`
	Undetermined bool       `json:"undetermined"`
	Actor        string     `json:"actor,omitempty"`
	ActorSource  string     `json:"actor_source"`
	Checksum     string     `json:"checksum,omitempty"`
	Error        string     `json:"error,omitempty"`
}

func writeJSON(out io.Writer, attempts []migrator.MigrationLogAttempt) error {
	records := make([]logRecord, 0, len(attempts))
	for _, attempt := range attempts {
		records = append(records, newLogRecord(attempt))
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(records)
}

func newLogRecord(attempt migrator.MigrationLogAttempt) logRecord {
	record := logRecord{
		StartedAt:    attempt.Start.At,
		RunID:        attempt.Start.RunID,
		Operation:    attempt.Start.Operation,
		Version:      attempt.Start.Version,
		Outcome:      string(attempt.Outcome.State),
		Undetermined: attempt.Undetermined(),
		Actor:        attempt.Start.Actor,
		ActorSource:  string(attempt.Start.ActorSource),
		Checksum:     attempt.Start.Checksum,
		Error:        attempt.Outcome.Error,
	}
	if !attempt.Undetermined() {
		finished := attempt.Outcome.At
		record.FinishedAt = &finished
	}
	return record
}

func writeTable(out io.Writer, attempts []migrator.MigrationLogAttempt) error {
	if len(attempts) == 0 {
		_, err := fmt.Fprintln(out, "No migration operations recorded.")
		return err
	}
	writer := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(writer, "STARTED\tVERSION\tOPERATION\tOUTCOME\tACTOR\tSOURCE")
	for _, attempt := range attempts {
		fmt.Fprintf(writer, "%s\t%d\t%s\t%s\t%s\t%s\n",
			attempt.Start.At.UTC().Format(time.RFC3339),
			attempt.Start.Version,
			attempt.Start.Operation,
			outcomeLabel(attempt),
			actorLabel(attempt.Start.Actor),
			attempt.Start.ActorSource,
		)
	}
	return writer.Flush()
}

// outcomeLabel names what happened, including the case where nobody knows.
func outcomeLabel(attempt migrator.MigrationLogAttempt) string {
	if attempt.Undetermined() {
		return "undetermined"
	}
	return string(attempt.Outcome.State)
}

func actorLabel(actor string) string {
	if actor == "" {
		return "-"
	}
	return actor
}
