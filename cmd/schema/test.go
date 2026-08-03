package schema

import (
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/cmd/internal/schemaload"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/migration/dbtest"
)

const (
	testDirFlag     = "dir"
	testRootDirFlag = "root-dir"
	testSeedDirFlag = "seed-dir"
	testDBURLFlag   = "db-url"
	testReportFlag  = "report"
	testRunFlag     = "run"

	testReportFormatText = "text"
)

// testReportFormats are the report formats the command accepts.
var testReportFormats = []string{"text", "json", "html"}

type testOptions struct {
	dir     string
	rootDir string
	seedDir string
	dbURL   string
	report  string
	run     string
}

// NewSchemaTestCommand returns the "test" command for the schema namespace. It
// applies a desired schema to a throwaway database once and runs declarative
// YAML test cases against it.
func NewSchemaTestCommand() *cobra.Command {
	opts := testOptions{}
	cmd := &cobra.Command{
		Use:          "test",
		Short:        "Run declarative schema tests",
		SilenceUsage: true,
		Long: `Run declarative YAML schema test cases against a throwaway database.

The desired schema is read from --root-dir and converged through live
introspection and planning before test steps run. Three source kinds are
accepted: a directory of Go schema annotations, a .sql or .hcl schema file, and
a database URL whose live schema is introspected. Each test file is a YAML
document with a top-level cases: list. A case is a named, ordered list of steps;
each step performs exactly one action:

  - exec:   run raw SQL.
  - seed:   apply environment-scoped SQL seed files.
  - apply_schema: recheck the desired schema and repair supported drift.
  - assert: run a query and check one of row_count, scalar, or error_contains.

Seed steps may specify their own dir. When omitted, --seed-dir supplies the
default directory for the run.

A migrate_to step is not valid in a schema test (use "ptah migrations test").

When --db-url is omitted, an ephemeral SQLite database is provisioned in a
temporary directory and removed afterwards. When --db-url is set it must point
at a throwaway database, because tests mutate schema and data.

A database --root-dir must share the dialect of the throwaway database the cases
run against, so a non-SQLite source requires an explicit --db-url. Roles and
grants introspected from a database source are dropped before the schema is
applied, because a schema test must not mutate cluster-scoped security state;
the omission is reported on stdout.

The command exits non-zero if any case fails.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSchemaTest(cmd.Context(), cmd.OutOrStdout(), opts)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&opts.dir, testDirFlag, "./tests", "Directory containing declarative test-case YAML files")
	flags.StringVar(&opts.rootDir, testRootDirFlag, "./models",
		"Desired schema source: a directory of Go schema annotations, a .sql or .hcl schema file, or a database URL")
	flags.StringVar(&opts.seedDir, testSeedDirFlag, "", "Default directory for seed steps that omit dir")
	flags.StringVar(&opts.dbURL, testDBURLFlag, "", "Throwaway database URL (optional). An ephemeral SQLite database is used when empty.")
	flags.StringVar(&opts.report, testReportFlag, testReportFormatText, "Report format: text, json, or html")
	flags.StringVar(&opts.run, testRunFlag, "", "Run only case names matching this Go regular expression")

	cmdutil.ConfigureCommand(cmd)
	return cmd
}

func runSchemaTest(ctx context.Context, out io.Writer, opts testOptions) error {
	if opts.report != "" && !slices.Contains(testReportFormats, opts.report) {
		return fmt.Errorf("unsupported report format %q: want text, json, or html", opts.report)
	}

	cases, err := dbtest.LoadCasesOfKind(opts.dir, dbtest.AtlasTestKindSchema)
	if err != nil {
		return fmt.Errorf("failed to load test cases: %w", err)
	}
	cases, err = dbtest.FilterCases(cases, opts.run)
	if err != nil {
		return err
	}
	if len(cases) == 0 {
		if opts.run != "" {
			return fmt.Errorf("no test cases match --run %q", opts.run)
		}
		return fmt.Errorf("no test cases found in %s", opts.dir)
	}

	desired, err := resolveTestDesiredSchema(ctx, out, opts)
	if err != nil {
		return err
	}

	report, err := dbtest.RunSchemaTest(ctx, dbtest.SchemaOptions{
		Cases:   cases,
		RootDir: opts.rootDir,
		Desired: desired,
		SeedDir: opts.seedDir,
		DBURL:   opts.dbURL,
	})
	if err != nil {
		return err
	}

	rendered, err := report.Render(opts.report)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprint(out, rendered); err != nil {
		return fmt.Errorf("write schema test report: %w", err)
	}
	if report.Failed() {
		return exitcode.New(1, fmt.Errorf("schema tests failed"))
	}
	return nil
}

// resolveTestDesiredSchema classifies the desired-schema source and resolves it.
//
// A directory keeps the historical Go-annotation path and is left to the runner
// so its error wording does not change. A .sql or .hcl file goes through the
// shared loader every other schema-consuming verb already uses -- `schema diff
// --to file://schema.sql` and `--to file://schema.hcl` both work. A database URL
// is introspected live through the same resolver `schema apply`, `schema diff`,
// `schema inspect` and `migrate diff` already route their desired state through.
//
// Only the database branch is new. Classification deliberately does not govern
// the other two: it calls a plain directory a local schema file and would hand
// it to the schema-file loader, which is why `schema diff --to file://models`
// fails with "schema file is a directory". A Go-annotation directory must keep
// reaching goschema.ParseDir inside the runner.
func resolveTestDesiredSchema(ctx context.Context, out io.Writer, opts testOptions) (*goschema.Database, error) {
	set, err := atlassource.ClassifySet("--"+testRootDirFlag, []string{opts.rootDir}, atlassource.ProjectEnv{})
	if err == nil && set.Kind == atlassource.KindDatabase {
		return resolveTestDesiredDatabase(ctx, out, opts, set)
	}
	info, statErr := os.Stat(opts.rootDir)
	if statErr != nil || info.IsDir() {
		// Leave a missing path to the runner, which names it in its own error.
		return nil, nil //nolint:nilerr // the runner reports this source
	}
	database, err := schemaload.LoadContext(ctx, schemaload.Options{SchemaFiles: []string{opts.rootDir}})
	if err != nil {
		return nil, fmt.Errorf("load desired schema from %s: %w", opts.rootDir, err)
	}
	return database, nil
}

// resolveTestDesiredDatabase introspects a database desired-state source. The
// dialect gate runs before any connection is opened, so a mismatched source
// fails on the mismatch rather than on whatever the connection would have done.
func resolveTestDesiredDatabase(
	ctx context.Context,
	out io.Writer,
	opts testOptions,
	set atlassource.Set,
) (*goschema.Database, error) {
	devDialect, err := ensureTestDevDialect(set, opts.dbURL)
	if err != nil {
		return nil, err
	}
	state, err := set.Resolve(ctx, atlassource.ResolveOptions{
		Dialect:     devDialect,
		DialectFlag: "--" + testDBURLFlag,
	})
	if err != nil {
		return nil, err
	}
	if err := dropClusterScopedTestState(out, state.Schema); err != nil {
		return nil, err
	}
	return state.Schema, nil
}

// ensureTestDevDialect refuses a database desired-state source whose dialect
// differs from the throwaway database the cases run against, and returns the
// dialect both share.
//
// Without this gate the mismatch is silent rather than loud: a PostgreSQL
// source with no --db-url is applied to the ephemeral SQLite default, every
// object SQLite cannot express is dropped on the way, and the run reports a
// green "1 cases, 1 passed, 0 failed" for semantics it never exercised.
func ensureTestDevDialect(set atlassource.Set, dbURL string) (string, error) {
	implied := set.ImpliedDialect()
	if strings.TrimSpace(dbURL) == "" {
		if implied == platform.SQLite || implied == "" {
			return implied, nil
		}
		return "", fmt.Errorf(
			"--%s database dialect %q requires an explicit --%s throwaway database of the same dialect,"+
				" because the default ephemeral test database is SQLite",
			testRootDirFlag, implied, testDBURLFlag)
	}
	devDialect, err := atlasurl.DialectFromURL(dbURL)
	if err != nil {
		return "", err
	}
	if implied == "" || devDialect == "" || implied == devDialect {
		return devDialect, nil
	}
	return "", fmt.Errorf("--%s dialect %q does not match --%s database dialect %q",
		testDBURLFlag, devDialect, testRootDirFlag, implied)
}

// dropClusterScopedTestState removes roles and grants from a database-sourced
// desired schema and reports exactly what it removed.
//
// The runner refuses any desired schema carrying roles or grants, because
// applying them mutates cluster-scoped security state that outlives the
// throwaway database. That refusal is right for an authored source and stays
// untouched there. It is wrong for an introspected one: every PostgreSQL
// database ships its own `GRANT USAGE ON SCHEMA public TO PUBLIC` and reports
// the connecting role, so the guard would refuse every live PostgreSQL source
// over security state the author never wrote. Dropping them silently would be
// the opposite defect -- an author who did write grants would get a green test
// that never applied them -- so the omission is reported, never silent.
func dropClusterScopedTestState(out io.Writer, schema *goschema.Database) error {
	roles, grants := len(schema.Roles), len(schema.Grants)
	if roles == 0 && grants == 0 {
		return nil
	}
	schema.Roles = nil
	schema.Grants = nil
	if _, err := fmt.Fprintf(out,
		"note: dropped %s and %s introspected from the desired-state database;"+
			" schema tests do not apply cluster-scoped security state\n",
		countedNoun(roles, "role"), countedNoun(grants, "grant"),
	); err != nil {
		return fmt.Errorf("write desired-state note: %w", err)
	}
	return nil
}

// countedNoun renders a count with its singular or plural noun.
func countedNoun(count int, noun string) string {
	if count == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", count, noun)
}
