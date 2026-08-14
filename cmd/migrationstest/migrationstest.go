// Package migrationstest implements the "ptah migrations test" command, which
// runs declarative YAML migration test cases against an ephemeral or throwaway
// database.
package migrationstest

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/internal/migrationintegrity"
	"go.5x5.cz/ptah/internal/migrationsnapshot"
	"go.5x5.cz/ptah/migration/dbtest"
	"go.5x5.cz/ptah/migration/migrator"
)

const (
	dirFlag           = "dir"
	migrationsDirFlag = "migrations-dir"
	rootDirFlag       = "root-dir"
	seedDirFlag       = "seed-dir"
	dbURLFlag         = "db-url"
	dirFormatFlag     = "dir-format"
	reportFlag        = "report"
	runFlag           = "run"

	reportFormatText = "text"
)

// reportFormats are the report formats the command accepts.
var reportFormats = []string{"text", "json", "html"}

type options struct {
	dir              string
	migrationsDir    string
	rootDir          string
	seedDir          string
	dbURL            string
	dirFormat        string
	report           string
	runPattern       string
	migrationsSchema string
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

  - migrate_to: migrate to a target version (a non-negative integer, "latest",
                or "0").
  - apply_schema: apply the desired schema under --root-dir.
  - seed:       apply environment-scoped SQL seed files.
  - exec:       run raw SQL.
  - assert:     run a query and check one of row_count, scalar, or
                error_contains.

Seed steps may specify their own dir. When omitted, --seed-dir supplies the
default directory for the run.

When --db-url is omitted, an ephemeral SQLite database is provisioned in a
temporary directory and removed afterwards. When --db-url is set it must point
at a throwaway database, because tests mutate schema and data.

The command exits non-zero if any case fails.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), opts)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&opts.dir, dirFlag, "./tests", "Directory containing declarative test-case YAML files")
	flags.StringVar(&opts.migrationsDir, migrationsDirFlag, "./migrations", "Directory containing migration files")
	flags.StringVar(&opts.rootDir, rootDirFlag, "./models", "Root directory to scan for apply_schema Go annotations")
	flags.StringVar(&opts.seedDir, seedDirFlag, "", "Default directory for seed steps that omit dir")
	flags.StringVar(&opts.dbURL, dbURLFlag, "", "Throwaway database URL (optional). An ephemeral SQLite database is used when empty.")
	flags.StringVar(&opts.dirFormat, dirFormatFlag, string(migrator.MigrationDirFormatPtah), "Migration directory format: auto, ptah, or atlas")
	flags.StringVar(&opts.report, reportFlag, reportFormatText, "Report format: text, json, or html")
	flags.StringVar(&opts.runPattern, runFlag, "", "Run only case names matching this Go regular expression")
	dbcli.RegisterMigrationsSchemaFlag(flags, &opts.migrationsSchema)

	cmdutil.ConfigureCommand(cmd)
	return cmd
}

func run(ctx context.Context, out, notice io.Writer, opts options) error {
	integrityPolicy, err := migrationintegrity.Resolve()
	if err != nil {
		return err
	}
	if opts.report != "" && !slices.Contains(reportFormats, opts.report) {
		return fmt.Errorf("unsupported report format %q: want text, json, or html", opts.report)
	}

	dirFormat, err := migrator.ParseMigrationDirFormat(opts.dirFormat)
	if err != nil {
		return err
	}

	// The shared integrity gate. `migrations test` belongs to the class for a
	// reason that is easy to miss: a migrate_to step rolls the directory
	// FORWARD and BACK, so this verb executes the down files too. Measured on
	// the same tampered directory that produced the defect, a passing test run
	// left the attacker's table behind in the target database at exit 0.
	//
	// --db-url is documented as a throwaway database, but nothing constrains it
	// to one, and "the operator promised it was disposable" is not a gate. The
	// verb keeps working on directories nobody hashed, which is the common case
	// while migrations are being written; it refuses only a directory that
	// carries a checksum its files no longer match.
	migrationsFS, err := migrationsnapshot.CaptureDirectory(opts.migrationsDir)
	if err != nil {
		return fmt.Errorf("capture migration directory: %w", err)
	}
	if _, err := migrationintegrity.GateWithPolicy(
		notice, migrationsFS, dirFormat, integrityPolicy, migrationintegrity.Options{},
	); err != nil {
		return err
	}

	cases, err := dbtest.LoadCasesOfKind(opts.dir, dbtest.AtlasTestKindMigrate)
	if err != nil {
		return fmt.Errorf("failed to load test cases: %w", err)
	}
	cases, err = dbtest.FilterCases(cases, opts.runPattern)
	if err != nil {
		return err
	}
	if len(cases) == 0 {
		if opts.runPattern != "" {
			return fmt.Errorf("no test cases match --run %q", opts.runPattern)
		}
		return fmt.Errorf("no test cases found in %s", opts.dir)
	}

	// A snapshot cannot tell an empty directory from an absent one, and the
	// difference decides whether the run means anything: `migrations test`
	// never creates --migrations-dir, so a path that is not there is a typo,
	// and a migrate_to step over the empty history it would otherwise produce
	// reports PASS having executed no migrations at all. The check is here,
	// after the cases are known, because the flag has a default and a suite of
	// apply_schema cases legitimately never reads the directory.
	if err := requireMigrationsDirForCases(cases, opts.migrationsDir); err != nil {
		return err
	}

	report, err := dbtest.RunMigrationTest(ctx, dbtest.Options{
		Cases:           cases,
		MigrationsDir:   opts.migrationsDir,
		MigrationsFS:    migrationsFS,
		RootDir:         opts.rootDir,
		SeedDir:         opts.seedDir,
		DBURL:           opts.dbURL,
		DirFormat:       dirFormat,
		RevisionsSchema: opts.migrationsSchema,
	})
	if err != nil {
		return err
	}

	rendered, err := report.Render(opts.report)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprint(out, rendered); err != nil {
		return fmt.Errorf("write migration test report: %w", err)
	}
	if report.Failed() {
		return exitcode.New(1, fmt.Errorf("migration tests failed"))
	}
	return nil
}

// requireMigrationsDirForCases refuses a missing --migrations-dir when one of
// the cases about to run reads it.
//
// The predicate is a migrate_to step, which is exactly what dbtest builds a
// migrator for; every other step kind touches the desired schema, raw SQL or
// the seed directory instead.
func requireMigrationsDirForCases(cases []dbtest.Case, migrationsDir string) error {
	needsMigrations := slices.ContainsFunc(cases, func(testCase dbtest.Case) bool {
		return slices.ContainsFunc(testCase.Steps, func(step dbtest.Step) bool {
			return strings.TrimSpace(step.MigrateTo) != ""
		})
	})
	if !needsMigrations {
		return nil
	}
	return migrationsnapshot.RequireExistingDirectory(migrationsDir)
}
