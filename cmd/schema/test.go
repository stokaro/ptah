package schema

import (
	"context"
	"fmt"
	"io"
	"os"
	"slices"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/cmd/internal/schemaload"
	"go.5x5.cz/ptah/core/goschema"
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
// applies a desired schema (from Go annotations) to a throwaway database once
// and runs declarative YAML test cases against it.
func NewSchemaTestCommand() *cobra.Command {
	opts := testOptions{}
	cmd := &cobra.Command{
		Use:          "test",
		Short:        "Run declarative schema tests",
		SilenceUsage: true,
		Long: `Run declarative YAML schema test cases against a throwaway database.

The desired schema is parsed from Go annotations under --root-dir and converged
through live introspection and planning before test steps run. Each test file is
a YAML document with a top-level cases: list. A case is a named, ordered list of
steps; each step performs exactly one action:

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

The command exits non-zero if any case fails.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSchemaTest(cmd.Context(), cmd.OutOrStdout(), opts)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&opts.dir, testDirFlag, "./tests", "Directory containing declarative test-case YAML files")
	flags.StringVar(&opts.rootDir, testRootDirFlag, "./models",
		"Desired schema source: a directory of Go schema annotations, or a .sql or .hcl schema file")
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

	desired, err := resolveTestDesiredSchema(ctx, opts.rootDir)
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

// resolveTestDesiredSchema turns the desired-schema source into a schema.
//
// A directory keeps the historical Go-annotation path and is left to the runner
// so its error wording does not change. A .sql or .hcl file goes through the
// shared loader every other schema-consuming verb already uses -- `schema diff
// --to file://schema.sql` and `--to file://schema.hcl` both work, and only this
// verb was restricted to Go annotations.
func resolveTestDesiredSchema(ctx context.Context, source string) (*goschema.Database, error) {
	info, err := os.Stat(source)
	if err != nil || info.IsDir() {
		// Leave a missing path to the runner, which names it in its own error.
		return nil, nil //nolint:nilerr // the runner reports this source
	}
	database, err := schemaload.LoadContext(ctx, schemaload.Options{SchemaFiles: []string{source}})
	if err != nil {
		return nil, fmt.Errorf("load desired schema from %s: %w", source, err)
	}
	return database, nil
}
