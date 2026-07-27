// Package migrationstest implements the "ptah migrations test" command, which
// runs declarative YAML migration test cases against an ephemeral or throwaway
// database.
package migrationstest

import (
	"context"
	"fmt"
	"io"
	"slices"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/migration/dbtest"
	"github.com/stokaro/ptah/migration/migrator"
)

const (
	dirFlag           = "dir"
	migrationsDirFlag = "migrations-dir"
	dbURLFlag         = "db-url"
	dirFormatFlag     = "dir-format"
	reportFlag        = "report"

	reportFormatText = "text"
)

// reportFormats are the report formats the command accepts.
var reportFormats = []string{"text", "json", "html"}

type options struct {
	dir           string
	migrationsDir string
	dbURL         string
	dirFormat     string
	report        string
}

// NewMigrationsTestCommand returns the "test" command for the migrations
// namespace.
func NewMigrationsTestCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:          "test",
		Short:        "Run declarative migration tests",
		SilenceUsage: true,
		Long: `Run declarative YAML migration test cases against a throwaway database.

Each test file is a YAML document with a top-level cases: list. A case is a
named, ordered list of steps; each step performs exactly one action:

  - migrate_to: migrate to a target version (an integer, "latest", or "0").
  - exec:       run raw SQL.
  - assert:     run a query and check one of row_count, scalar, or
                error_contains.

When --db-url is omitted, an ephemeral SQLite database is provisioned in a
temporary directory and removed afterwards. When --db-url is set it must point
at a throwaway database, because tests mutate schema and data.

The command exits non-zero if any case fails.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd.Context(), cmd.OutOrStdout(), opts)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&opts.dir, dirFlag, "./tests", "Directory containing declarative test-case YAML files")
	flags.StringVar(&opts.migrationsDir, migrationsDirFlag, "./migrations", "Directory containing migration files")
	flags.StringVar(&opts.dbURL, dbURLFlag, "", "Throwaway database URL (optional). An ephemeral SQLite database is used when empty.")
	flags.StringVar(&opts.dirFormat, dirFormatFlag, string(migrator.MigrationDirFormatPtah), "Migration directory format: auto, ptah, or atlas")
	flags.StringVar(&opts.report, reportFlag, reportFormatText, "Report format: text, json, or html")

	cmdutil.ConfigureCommand(cmd)
	return cmd
}

func run(ctx context.Context, out io.Writer, opts options) error {
	if opts.report != "" && !slices.Contains(reportFormats, opts.report) {
		return fmt.Errorf("unsupported report format %q: want text, json, or html", opts.report)
	}

	dirFormat, err := migrator.ParseMigrationDirFormat(opts.dirFormat)
	if err != nil {
		return err
	}

	cases, err := dbtest.LoadCases(opts.dir)
	if err != nil {
		return fmt.Errorf("failed to load test cases: %w", err)
	}
	if len(cases) == 0 {
		return fmt.Errorf("no test cases found in %s", opts.dir)
	}

	report, err := dbtest.RunMigrationTest(ctx, dbtest.Options{
		Cases:         cases,
		MigrationsDir: opts.migrationsDir,
		DBURL:         opts.dbURL,
		DirFormat:     dirFormat,
	})
	if err != nil {
		return err
	}

	rendered, err := report.Render(opts.report)
	if err != nil {
		return err
	}
	fmt.Fprint(out, rendered)
	if report.Failed() {
		return fmt.Errorf("migration tests failed")
	}
	return nil
}
