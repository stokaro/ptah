package atlas

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/devdocker"
	"go.5x5.cz/ptah/internal/schemafile"
	"go.5x5.cz/ptah/migration/dbtest"
	"go.5x5.cz/ptah/migration/migrator"
)

// atlasSchemaPlanTestVerb is the Atlas command name, for diagnostics.
const atlasSchemaPlanTestVerb = "atlas schema plan test"

type atlasSchemaPlanTestOptions struct {
	devURL string
	run    string
	paths  []string
}

// newAtlasSchemaPlanTestCommand implements `atlas schema plan test`.
//
// # Evidence
//
// Flag set, per the published Atlas CLI reference: `--dev-url` and `--run`,
// with the test files given positionally. The pinned community binary aborts
// the whole `schema plan` path, so it establishes that CE gates this command
// and nothing about how it behaves.
//
// Behavior: the LOCAL workflow the Atlas documentation describes -- establish a
// starting state, apply a saved plan file, assert what it did. Where public
// documentation does not settle a detail, this implements an explicit Ptah
// behavior and says so rather than implying parity nobody measured
// (stokaro/ptah#1211).
//
// # Why the case reuses everything
//
// The plan is read by the same reader `schema apply --plan` uses and executed
// as the same statements, and the starting state is applied through the same
// convergence path a schema test uses. Recomputing the plan here instead would
// test the planner against itself: a plan file that stopped matching what the
// planner now produces would still pass, which is the one failure a plan test
// exists to catch.
func newAtlasSchemaPlanTestCommand() *cobra.Command {
	opts := atlasSchemaPlanTestOptions{}
	cmd := &cobra.Command{
		Use:   "test [paths]",
		Short: "Run schema plan tests",
		Long: `Atlas ` + "`atlas schema plan test`" + ` command path.

Runs ` + "`test \"plan\"`" + ` cases from Atlas ` + "`.test.hcl`" + ` files against a throwaway
database. Each case establishes a starting state, applies a saved plan file,
and asserts what the plan did:

    test "plan" "add_email" {
      schema {
        url = "file://snapshots/v1.sql"
      }

      exec {
        sql = "INSERT INTO users (id, name) VALUES (1, 'Ada')"
      }

      apply {
        url = "file://plans/add_email.plan.hcl"
      }

      exec {
        sql    = "SELECT name FROM users WHERE id = 1"
        output = "Ada"
      }
    }

Steps run in the order they are written, which is what makes the case mean
anything: a plan describes a transition FROM a state, so the schema block has
to establish that state before the plan is applied.

The plan is read and executed exactly as ` + "`schema apply --plan`" + ` reads and
executes it, rather than recomputed. A plan file that stopped matching what the
planner now produces is precisely what this command exists to catch, and
recomputing would hide it.

--dev-url names the throwaway database. Without one a SQLite database is
provisioned per case and removed afterwards, so a SQLite project needs no
flag; every other dialect requires it, because there is nothing safe to
default to. --run filters cases by name.

` + "`test \"schema\"`" + ` and ` + "`test \"migrate\"`" + ` cases in the same files are ignored
here: they are run by ` + "`schema test`" + ` and ` + "`migrate test`" + `, and a kind
running under the wrong verb would execute work its author did not ask this
command for.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.paths = args
			return runAtlasSchemaPlanTest(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.devURL, "dev-url", "", "Dev database URL the tests run against")
	flags.StringVar(&opts.run, "run", "", "Run only the cases whose name matches")
	return cmd
}

func runAtlasSchemaPlanTest(cmd *cobra.Command, opts atlasSchemaPlanTestOptions) error {
	dir := "."
	if len(opts.paths) > 0 {
		dir = opts.paths[0]
	}
	if len(opts.paths) > 1 {
		return cmdutil.Fail(cmd, fmt.Errorf(
			"%s takes at most one path, got %d", atlasSchemaPlanTestVerb, len(opts.paths)))
	}

	cases, err := dbtest.LoadCasesOfKind(dir, dbtest.AtlasTestKindPlan)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("load plan test cases: %w", err))
	}
	cases, err = dbtest.FilterCases(cases, opts.run)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if len(cases) == 0 {
		if opts.run != "" {
			return cmdutil.Fail(cmd, fmt.Errorf("no test cases match --run %q", opts.run))
		}
		return cmdutil.Fail(cmd, fmt.Errorf(`no test "plan" cases found in %s`, dir))
	}

	// The dev database is provisioned after the cases are known, so a run that
	// was going to be refused for its inputs is refused without starting a
	// container -- the same shape cmd/schema/test.go uses. It is also what
	// makes `--dev-url docker://postgres/16/dev` work here rather than reaching
	// the connector and being answered `unsupported database dialect: docker`.
	devURL, releaseDev, err := devdocker.Resolve(cmd.Context(), opts.devURL, devdocker.Options{})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	defer releaseDev()

	report, err := dbtest.RunSchemaTest(cmd.Context(), dbtest.SchemaOptions{
		Cases:         cases,
		DBURL:         devURL,
		ReportKind:    "PLAN",
		ResolveSchema: atlasPlanTestSchemaResolver(dir),
		ApplyPlan:     atlasPlanTestPlanApplier(dir),
	})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	rendered, renderErr := report.Render("")
	if renderErr != nil {
		return cmdutil.Fail(cmd, renderErr)
	}
	fmt.Fprint(cmd.OutOrStdout(), rendered)
	if report.Failed() {
		return exitcode.New(1, fmt.Errorf("schema plan tests failed"))
	}
	return nil
}

// atlasPlanTestSchemaResolver loads the desired state a `schema` block names.
//
// Paths resolve against the directory holding the test files, which is what
// makes a case portable: the snapshot it names travels with it.
func atlasPlanTestSchemaResolver(dir string) func(string) (*goschema.Database, error) {
	return func(url string) (*goschema.Database, error) {
		path, err := atlasPlanTestLocalPath(dir, url, "schema")
		if err != nil {
			return nil, err
		}
		return schemafile.Load(path, schemafile.Options{})
	}
}

// atlasPlanTestPlanApplier reads and executes the plan file an `apply` block
// names.
//
// The plan's recorded from-state is verified against the database the case
// established, which is the "verify plan From against initial state" step of
// the documented workflow. A plan describes a transition FROM a state, so a
// case whose snapshot has drifted away from the one the plan was computed for
// is testing a plan against a state it was never meant for -- and would report
// whatever the statements happened to do there.
//
// Only a plan carrying Ptah's own sha256 fingerprint is verified. An
// Atlas-authored plan's hashes have no local recipe, so there is nothing to
// compare and the case runs on its assertions alone.
//
// The statements are the plan's own, never replanned: a plan file that stopped
// matching what the planner now produces is the failure this verb exists to
// catch.
func atlasPlanTestPlanApplier(
	dir string,
) func(context.Context, *dbschema.DatabaseConnection, string) error {
	return func(ctx context.Context, conn *dbschema.DatabaseConnection, url string) error {
		path, err := atlasPlanTestLocalPath(dir, url, "apply")
		if err != nil {
			return err
		}
		plan, _, err := atlasschema.ReadPlanDocument(path)
		if err != nil {
			return err
		}
		if atlasschema.IsNativeFingerprint(plan.FromFingerprint) {
			if err := atlasschema.VerifyPlanTarget(conn, plan); err != nil {
				return err
			}
		}
		statements := atlasschema.SplitApplyStatements(plan.SQL(), conn.Info().Dialect)
		if len(statements) == 0 {
			return fmt.Errorf("plan file %s carries no statement", path)
		}
		txMode, err := atlasPlanTestTxMode(path, plan)
		if err != nil {
			return err
		}
		conn.SchemaWriter().SetDryRun(false)
		return atlasschema.ApplyStatements(ctx, conn, txMode, statements)
	}
}

// atlasPlanTestTxMode honors a transaction-mode directive the plan carries, so
// a plan is executed under the mode it was reviewed with. A plan holding a
// CONCURRENTLY statement runs the way it will run in production or not at all.
func atlasPlanTestTxMode(path string, plan atlasschema.PlanFile) (migrator.MigrationTxMode, error) {
	fileMode, err := atlasschema.PlanTxMode(path, plan.SQL())
	if err != nil {
		return "", err
	}
	return migrator.ResolveAtlasDirectiveTxMode(migrator.MigrationTxModeFile, fileMode, path)
}

// atlasPlanTestLocalPath resolves a `file://` URL against the test directory
// and refuses anything else.
//
// A registry URL is refused by name rather than by a parse failure: Ptah has no
// plan registry, and an operator who wrote one is asking for a feature rather
// than making a typo.
func atlasPlanTestLocalPath(dir, url, block string) (string, error) {
	trimmed := strings.TrimSpace(url)
	if strings.Contains(trimmed, "://") && !strings.HasPrefix(trimmed, "file://") {
		return "", fmt.Errorf(
			"%s block url %q: only file:// URLs are supported; Ptah has no plan registry", block, url)
	}
	path, err := schemafile.LocalFilePath(trimmed)
	if err != nil {
		return "", fmt.Errorf("%s block url %q: %w", block, url, err)
	}
	if !isAbsoluteLocalPath(path) {
		return dir + "/" + path, nil
	}
	return path, nil
}

func isAbsoluteLocalPath(path string) bool {
	return strings.HasPrefix(path, "/") ||
		(len(path) > 2 && path[1] == ':' && slices.Contains([]byte{'\\', '/'}, path[2]))
}
