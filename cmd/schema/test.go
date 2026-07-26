package schema

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/migration/dbtest"
)

const (
	testDirFlag     = "dir"
	testRootDirFlag = "root-dir"
	testDBURLFlag   = "db-url"
	testReportFlag  = "report"

	testReportFormatText = "text"
)

type testOptions struct {
	dir     string
	rootDir string
	dbURL   string
	report  string
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

The desired schema is parsed from Go annotations under --root-dir, rendered to
CREATE DDL, and applied to a fresh database before each case's steps run. Each
test file is a YAML document with a top-level cases: list. A case is a named,
ordered list of steps; each step performs exactly one action:

  - exec:   run raw SQL.
  - assert: run a query and check one of row_count, scalar, or error_contains.

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
	flags.StringVar(&opts.rootDir, testRootDirFlag, "./models", "Root directory to scan for Go schema annotations")
	flags.StringVar(&opts.dbURL, testDBURLFlag, "", "Throwaway database URL (optional). An ephemeral SQLite database is used when empty.")
	flags.StringVar(&opts.report, testReportFlag, testReportFormatText, "Report format (only \"text\" is supported)")

	cmdutil.ConfigureCommand(cmd)
	return cmd
}

func runSchemaTest(ctx context.Context, out io.Writer, opts testOptions) error {
	if opts.report != "" && opts.report != testReportFormatText {
		return fmt.Errorf("unsupported report format %q: only %q is supported", opts.report, testReportFormatText)
	}

	cases, err := dbtest.LoadCases(opts.dir)
	if err != nil {
		return fmt.Errorf("failed to load test cases: %w", err)
	}
	if len(cases) == 0 {
		return fmt.Errorf("no test cases found in %s", opts.dir)
	}

	report, err := dbtest.RunSchemaTest(ctx, dbtest.SchemaOptions{
		Cases:   cases,
		RootDir: opts.rootDir,
		DBURL:   opts.dbURL,
	})
	if err != nil {
		return err
	}

	fmt.Fprint(out, report.Text())
	if report.Failed() {
		return fmt.Errorf("schema tests failed")
	}
	return nil
}
