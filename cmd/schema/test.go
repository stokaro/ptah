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
	"go.5x5.cz/ptah/internal/schemascope"
	"go.5x5.cz/ptah/migration/dbtest"
)

const (
	testDirFlag     = "dir"
	testRootDirFlag = "root-dir"
	testSeedDirFlag = "seed-dir"
	testDBURLFlag   = "db-url"
	testReportFlag  = "report"
	testRunFlag     = "run"
	testSchemaFlag  = "schema"

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
	schemas []string
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

--schema restricts the desired schema to the named schemas before it is applied,
so the cases run against that subset only. Repeated and comma-separated values
union. A selection that keeps nothing is refused rather than run, because an
empty desired schema makes every case fail on a missing object instead of
reporting the selection as the cause.

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
	flags.StringArrayVar(&opts.schemas, testSchemaFlag, nil, "Restrict the desired schema to these schema names")

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

	desired, err := resolveTestDesiredSchema(ctx, opts.rootDir, opts.schemas)
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

// resolveTestDesiredSchema turns the desired-schema source into a schema,
// restricted to schemas when a schema selection is present.
//
// Without a selection a directory keeps the historical Go-annotation path and is
// left to the runner so its error wording does not change. A .sql or .hcl file
// goes through the shared loader every other schema-consuming verb already uses
// -- `schema diff --to file://schema.sql` and `--to file://schema.hcl` both
// work, and only this verb was restricted to Go annotations.
//
// A schema selection has to be applied here rather than inside the runner,
// because the runner's own parse produces the schema it immediately provisions.
// Resolving the directory eagerly on that path is the price of the selection;
// the parse is the same [goschema.ParseDir] the runner would have run.
func resolveTestDesiredSchema(ctx context.Context, source string, schemas []string) (*goschema.Database, error) {
	selection := schemascope.SplitNames(schemas)
	info, err := os.Stat(source)
	if err != nil || info.IsDir() {
		if len(selection) == 0 {
			// Leave a missing path to the runner, which names it in its own error.
			return nil, nil //nolint:nilerr // the runner reports this source
		}
		parsed, parseErr := goschema.ParseDir(source)
		if parseErr != nil {
			return nil, fmt.Errorf("parse desired schema from %s: %w", source, parseErr)
		}
		return scopeTestDesiredSchema(parsed, selection)
	}
	database, err := schemaload.LoadContext(ctx, schemaload.Options{SchemaFiles: []string{source}})
	if err != nil {
		return nil, fmt.Errorf("load desired schema from %s: %w", source, err)
	}
	return scopeTestDesiredSchema(database, selection)
}

// scopeTestDesiredSchema restricts database to the selected schemas through the
// same allow-list every other schema-scoped verb uses.
//
// A selection that keeps no tables out of a schema that had some is refused. The
// alternative is provisioning an empty database and letting every case fail on a
// missing relation, which reports the symptom and hides the cause -- the
// zero-match false green that stokaro/ptah#979 exists to prevent.
func scopeTestDesiredSchema(database *goschema.Database, selection []string) (*goschema.Database, error) {
	if len(selection) == 0 || database == nil {
		return database, nil
	}
	scoped := schemascope.FilterGenerated(database, selection)
	if len(database.Tables) > 0 && len(scoped.Tables) == 0 {
		return nil, fmt.Errorf(
			"--%s %s selects no tables out of the desired schema",
			testSchemaFlag, strings.Join(selection, ","))
	}
	return scoped, nil
}
