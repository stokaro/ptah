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
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/devdocker"
	"go.5x5.cz/ptah/internal/schemaload"
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
	testVarFlag     = "var"

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
	vars    []string
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

--schema restricts the desired schema to the named schemas before it is applied,
so the cases run against that subset only. Repeated and comma-separated values
union. A selection that keeps nothing is refused rather than run, because an
empty desired schema makes every case fail on a missing object instead of
reporting the selection as the cause.

A migrate_to step is not valid in a schema test (use "ptah migrations test").

When --db-url is omitted, an ephemeral SQLite database is provisioned in a
temporary directory and removed afterwards. When --db-url is set it must point
at a throwaway database, because tests mutate schema and data.

A database --root-dir must share the dialect of the throwaway database the cases
run against, so a non-SQLite source requires an explicit --db-url. Roles and
grants introspected from a database source are dropped before the schema is
applied, because a schema test must not mutate cluster-scoped security state;
the omission is reported on stderr, so stdout carries only the report.

The command exits non-zero if any case fails.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSchemaTest(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), opts)
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
	flags.StringArrayVar(&opts.schemas, testSchemaFlag, nil, "Restrict the desired schema to these schema names")
	flags.StringArrayVar(&opts.vars, testVarFlag, nil,
		"Supply a value for a variable block of an HCL schema file, as name=value (repeatable)")

	cmdutil.ConfigureCommand(cmd)
	return cmd
}

// runSchemaTest writes the rendered report to out and every diagnostic to diag.
// The two must stay separate: --report json and --report html are consumed by
// machines, and a note interleaved with them makes a passing run unparseable
// while still exiting 0.
func runSchemaTest(ctx context.Context, out, diag io.Writer, opts testOptions) error {
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

	desired, err := resolveTestDesiredSchema(ctx, diag, opts)
	if err != nil {
		return err
	}

	// The test database is provisioned here, after the cases and the desired
	// schema are known, so a run that was going to be refused for its inputs is
	// refused without starting a container. See the same shape in
	// cmd/migrationstest.
	dbURL, releaseDev, err := devdocker.Resolve(ctx, opts.dbURL, devdocker.Options{})
	if err != nil {
		return err
	}
	defer releaseDev()

	report, err := dbtest.RunSchemaTest(ctx, dbtest.SchemaOptions{
		Cases:   cases,
		RootDir: opts.rootDir,
		Desired: desired,
		SeedDir: opts.seedDir,
		DBURL:   dbURL,
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

// resolveTestDesiredSchema classifies the desired-schema source, resolves it,
// and restricts it to the selected schemas when a selection is present.
//
// A directory keeps the historical Go-annotation path and is left to the runner
// so its error wording does not change. A .sql or .hcl file goes through the
// shared loader every other schema-consuming verb already uses -- `schema diff
// --to file://schema.sql` and `--to file://schema.hcl` both work. A database URL
// is introspected live through the same resolver `schema apply`, `schema diff`,
// `schema inspect` and `migrate diff` already route their desired state through.
//
// Classification deliberately does not govern the file and directory branches:
// it calls a plain directory a local schema file and would hand it to the
// schema-file loader, which since stokaro/ptah#940 reads a directory of .sql or
// .hcl files as one schema. A directory of Go annotations holds neither, so that
// loader would refuse it; a Go-annotation directory must keep reaching
// goschema.ParseDir.
//
// A schema selection has to be applied here rather than inside the runner,
// because the runner's own parse produces the schema it immediately provisions.
// That is why the directory branch, which would otherwise defer to the runner,
// resolves eagerly as soon as a selection is present; the parse is the same
// [goschema.ParseDir] the runner would have run. Every branch funnels through
// scopeTestDesiredSchema so the selection cannot apply to some sources and be
// silently ignored on others.
func resolveTestDesiredSchema(ctx context.Context, diag io.Writer, opts testOptions) (*goschema.Database, error) {
	selection := schemascope.SplitNames(opts.schemas)
	set, err := atlassource.ClassifySet("--"+testRootDirFlag, []string{opts.rootDir}, atlassource.ProjectEnv{})
	if err == nil && set.Kind == atlassource.KindDatabase {
		return resolveTestDesiredDatabase(ctx, diag, opts, set, selection)
	}
	info, statErr := os.Stat(opts.rootDir)
	if statErr != nil || info.IsDir() {
		if len(selection) == 0 {
			// Leave a missing path to the runner, which names it in its own error.
			return nil, nil //nolint:nilerr // the runner reports this source
		}
		parsed, parseErr := goschema.ParseDir(opts.rootDir)
		if parseErr != nil {
			return nil, fmt.Errorf("parse desired schema from %s: %w", opts.rootDir, parseErr)
		}
		return scopeTestDesiredSchema(parsed, selection, "")
	}
	database, err := schemaload.LoadContext(ctx, schemaload.Options{
		SchemaFiles: []string{opts.rootDir},
		Vars:        opts.vars,
	})
	if err != nil {
		return nil, fmt.Errorf("load desired schema from %s: %w", opts.rootDir, err)
	}
	return scopeTestDesiredSchema(database, selection, "")
}

// scopeTestDesiredSchema restricts database to the selected schemas through the
// same allow-list every other schema-scoped verb uses.
//
// defaultSchema names the schema that unqualified objects belong to. Objects
// parsed from Go annotations or a schema file carry their own schema name and
// pass "", but a live introspection leaves the default schema's objects
// unqualified, so matching them against `--schema main` (or `public`, or `dbo`)
// requires naming that default -- the same pairing internal/atlasmigrate/diff.go
// makes for `migrate diff`. Without it every selection over a database source
// would filter everything out and report the zero-match refusal below, turning
// a supported combination into a refusal.
//
// A selection that keeps no tables out of a schema that had some is refused. The
// alternative is provisioning an empty database and letting every case fail on a
// missing relation, which reports the symptom and hides the cause -- the
// zero-match false green that stokaro/ptah#979 exists to prevent.
func scopeTestDesiredSchema(
	database *goschema.Database,
	selection []string,
	defaultSchema string,
) (*goschema.Database, error) {
	if len(selection) == 0 || database == nil {
		return database, nil
	}
	scoped := schemascope.FilterGeneratedWithDefaultSchema(database, selection, defaultSchema)
	if len(database.Tables) > 0 && len(scoped.Tables) == 0 {
		return nil, fmt.Errorf(
			"--%s %s selects no tables out of the desired schema",
			testSchemaFlag, strings.Join(selection, ","))
	}
	return scoped, nil
}

// resolveTestDesiredDatabase introspects a database desired-state source. The
// dialect gate runs before any connection is opened, so a mismatched source
// fails on the mismatch rather than on whatever the connection would have done.
//
// The schema selection is applied here rather than by the caller because only
// this branch knows the dialect, and the dialect is what names the default
// schema an introspection leaves objects unqualified in.
func resolveTestDesiredDatabase(
	ctx context.Context,
	diag io.Writer,
	opts testOptions,
	set atlassource.Set,
	selection []string,
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
	if err := dropClusterScopedTestState(diag, state.Schema); err != nil {
		return nil, err
	}
	return scopeTestDesiredSchema(state.Schema, selection, identifier.ForDialect(devDialect).DefaultSchema)
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
//
// It is reported on diag, never on the report stream. --report json and
// --report html are consumed by machines, and a note printed alongside them
// leaves a passing run unparseable while still exiting 0.
func dropClusterScopedTestState(diag io.Writer, schema *goschema.Database) error {
	roles, grants := len(schema.Roles), len(schema.Grants)
	if roles == 0 && grants == 0 {
		return nil
	}
	schema.Roles = nil
	schema.Grants = nil
	if _, err := fmt.Fprintf(diag,
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
