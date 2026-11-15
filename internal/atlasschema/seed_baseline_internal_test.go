package atlasschema

// White-box testing required: the baseline a rehearsal writes is reached
// publicly only through a live dev database, which cannot say what it was
// sent or what a refusal kept from it. These drive the unexported baseline
// write against a dev database that records each statement, and the
// rehearsal core against SQLite, for the refusal of a schema every reset
// leaves as it found it, the guard on the statements the closure adds, the
// rename of a sequence an added table owns, and the added objects a failed
// plan names.

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/devclean"
	"ptah.run/internal/devseed"
	"ptah.run/migration/migrator"
)

// recordingExecutor is a dev database that executes every statement it is
// sent and records it.
type recordingExecutor struct {
	executed []string
}

func (e *recordingExecutor) ExecuteSQL(_ context.Context, statement string, _ ...any) error {
	e.executed = append(e.executed, statement)
	return nil
}

func (*recordingExecutor) IsDryRun() bool { return false }

// postgresDevInfo is a PostgreSQL 18 dev database whose URL pins public, on a
// server the run does not own.
func postgresDevInfo() catalog.ServerInfo {
	info := catalog.ServerInfo{Dialect: "postgres", Schema: "public", Capabilities: capability.Postgres18()}
	info.IdentifierSemantics.SearchPath = []string{"$user", "public"}
	return info
}

// seedRender stands in for the planner: one statement per table, function and
// policy, spelled so the names they read are in their code.
func seedRender(state *catalog.Database) ([]string, error) {
	var statements []string
	for _, table := range state.Tables {
		statements = append(statements, "CREATE TABLE "+catalog.QualifyTableName(table.Schema, table.Name)+" (id integer)")
	}
	for _, policy := range state.RLSPolicies {
		statements = append(statements, fmt.Sprintf("CREATE POLICY %s ON %s USING (%s)", policy.Name, policy.Table, policy.UsingExpression))
	}
	for _, function := range state.Functions {
		statements = append(statements, fmt.Sprintf("CREATE FUNCTION %s.%s() RETURNS uuid LANGUAGE sql AS $$%s$$",
			function.Schema, function.Name, function.Body))
	}
	return statements, nil
}

// devOf is the dev database executor reaches, read as read after the
// baseline is written, with leftAsFound as the schemas its resets keep.
func devOf(executor *recordingExecutor, read *catalog.Database, leftAsFound ...string) baselineDev {
	info := postgresDevInfo()
	return baselineDev{
		info:        info,
		guard:       devclean.NewBaselineGuard(info),
		executor:    executor,
		read:        func(context.Context) (*catalog.Database, error) { return read, nil },
		leftAsFound: leftAsFound,
	}
}

// targetHolding is a read of the target outside the compared scope that holds
// read whatever it is asked for.
func targetHolding(read *catalog.Database) devseed.Target {
	return func([]string) (*catalog.Database, error) { return read, nil }
}

// tasksReferencingAccounts is the compared state of a plan that adds a foreign
// key from public.tasks to auth.accounts, a table the selection leaves out,
// with the plan's statement and the target's read of auth.
func tasksReferencingAccounts() (*catalog.Database, rehearsalOutside) {
	current := &catalog.Database{Tables: []catalog.Table{{Name: "tasks", Columns: []catalog.Column{{Name: "owner", DataType: "integer"}}}}}
	read := &catalog.Database{
		Schemas: []catalog.Schema{{Name: "auth"}, {Name: "public"}},
		Tables:  []catalog.Table{{Schema: "auth", Name: "accounts", Columns: []catalog.Column{{Name: "id", DataType: "integer"}}}},
	}
	executed := []string{`ALTER TABLE "public"."tasks" ADD CONSTRAINT "tasks_owner_fk" FOREIGN KEY ("owner") REFERENCES "auth"."accounts" ("id")`}
	return current, rehearsalOutside{target: targetHolding(read), executed: executed}
}

// addedAccounts is what the closure adds for tasksReferencingAccounts.
var addedAccounts = []devseed.Object{{Kind: "table", Schema: "auth", Name: "accounts"}, {Kind: "schema", Name: "auth"}}

// TestSeedBaseline_HappyPath writes a baseline that adds an object to a
// schema a reset either drops or did not keep: a schema the claim did not
// find, or one it found that already holds the object, so the baseline plans
// nothing there.
func TestSeedBaseline_HappyPath(t *testing.T) {
	accounts := &catalog.Database{Tables: []catalog.Table{{Schema: "auth", Name: "accounts"}}}
	tests := []struct {
		name        string
		leftAsFound []string
		devCurrent  *catalog.Database
	}{
		{name: "a dev database that held no other schema"},
		{name: "a dev database that held another schema", leftAsFound: []string{"util"}},
		{name: "a schema that already holds the object", leftAsFound: []string{"auth"}, devCurrent: accounts},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			current, outside := tasksReferencingAccounts()
			executor := &recordingExecutor{}

			added, err := seedBaseline(c.Context(), devOf(executor, &catalog.Database{}, test.leftAsFound...), current, test.devCurrent, outside, seedRender)

			c.Assert(err, qt.IsNil)
			c.Assert(added, qt.DeepEquals, addedAccounts)
			c.Assert(executor.executed, qt.DeepEquals, []string{"CREATE TABLE tasks (id integer)", "CREATE TABLE auth.accounts (id integer)"})
		})
	}
}

// TestSeedBaseline_RefusesASchemaEveryResetKeeps refuses a baseline that adds
// an object to a schema the dev database held at the claim, which every reset
// leaves as it finds it, before any statement runs. The refusal names the
// schema and the objects, whatever case the schema is listed in.
func TestSeedBaseline_RefusesASchemaEveryResetKeeps(t *testing.T) {
	tests := []struct {
		name        string
		leftAsFound []string
		wantErr     string
	}{
		{
			name:        "one schema",
			leftAsFound: []string{"auth"},
			wantErr: `the dev database held schema "auth" before the run, and every reset leaves it as the claim found it, ` +
				`so what the rehearsal writes there would outlive the run: table "auth.accounts"; ` +
				`point --dev-url at a database that does not hold it; ` +
				`the rehearsal recreates these objects from outside the compared scope because the planned statements need them: ` +
				`table "auth.accounts", schema "auth"`,
		},
		{
			name:        "two schemas, one of them written to",
			leftAsFound: []string{"AUTH", "util"},
			wantErr:     `the dev database held schema "AUTH" before the run, .*: table "auth.accounts"; .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			current, outside := tasksReferencingAccounts()
			executor := &recordingExecutor{}

			added, err := seedBaseline(c.Context(), devOf(executor, &catalog.Database{}, test.leftAsFound...), current, nil, outside, seedRender)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(added, qt.DeepEquals, addedAccounts)
			c.Assert(executor.executed, qt.IsNil)
		})
	}
}

// TestSeedBaseline_GuardsTheStatementsTheClosureAdds runs the baseline guard
// over the statements the closure adds as well as the state's own: a policy
// that calls auth.uid() adds the function, which a dev database the run does
// not own refuses, so nothing runs.
func TestSeedBaseline_GuardsTheStatementsTheClosureAdds(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{
		Tables:      []catalog.Table{{Name: "notes"}},
		RLSPolicies: []catalog.RLSPolicy{{Name: "notes_owner", Table: "notes", UsingExpression: "owner = auth.uid()"}},
	}
	read := &catalog.Database{
		Schemas:   []catalog.Schema{{Name: "auth"}, {Name: "public"}},
		Functions: []catalog.Function{{Schema: "auth", Name: "uid", Returns: "uuid", Language: "sql", Body: "SELECT NULL::uuid"}},
	}
	executor := &recordingExecutor{}

	_, err := seedBaseline(c.Context(), devOf(executor, &catalog.Database{}), current, nil, rehearsalOutside{target: targetHolding(read)}, seedRender)

	c.Assert(err, qt.ErrorMatches, `(?s)baseline statement 3 \(CREATE FUNCTION auth\.uid\(\).*\) cannot be rehearsed: `+
		`postgres rehearsal baseline refuses CREATE routine definition .*; the rehearsal recreates these objects .*: function "auth.uid", schema "auth"`)
	c.Assert(executor.executed, qt.IsNil)
}

// TestSeedBaseline_NamesTheSequenceOfAnAddedTableAsTheTarget names the
// sequence a serial column of an added table owns as the target names it.
// The dev database creates it under the name PostgreSQL gives it, and a plan
// that names the target's sequence fails on the dev database without the
// rename. The compared state does not hold the table; the recreated state does.
func TestSeedBaseline_NamesTheSequenceOfAnAddedTableAsTheTarget(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{Tables: []catalog.Table{{Name: "tasks", Columns: []catalog.Column{{Name: "parent_id", DataType: "integer"}}}}}
	parents := func(sequence string) *catalog.Database {
		return &catalog.Database{
			Schemas: []catalog.Schema{{Name: "app"}, {Name: "public"}},
			Tables: []catalog.Table{{Schema: "app", Name: "parents", Columns: []catalog.Column{
				{Name: "id", DataType: "integer", OwnedSequence: sequence},
			}}},
		}
	}
	outside := rehearsalOutside{
		target:   targetHolding(parents("legacy_parents_seq")),
		executed: []string{`ALTER TABLE "public"."tasks" ADD CONSTRAINT "tasks_parent_fk" FOREIGN KEY ("parent_id") REFERENCES "app"."parents" ("id")`},
	}
	executor := &recordingExecutor{}

	_, err := seedBaseline(c.Context(), devOf(executor, parents("parents_id_seq")), current, nil, outside, seedRender)

	c.Assert(err, qt.IsNil)
	c.Assert(executor.executed, qt.DeepEquals, []string{
		"CREATE TABLE tasks (id integer)",
		"CREATE TABLE app.parents (id integer)",
		`ALTER SEQUENCE "app"."parents_id_seq" RENAME TO "ptah_rehearsal_seq_0"`,
		`ALTER SEQUENCE "app"."ptah_rehearsal_seq_0" RENAME TO "legacy_parents_seq"`,
	})
}

// TestRehearsalCoreNamesTheAddedObjectsAPlanFailsAgainst rehearses on SQLite
// a plan that fails after the baseline added a table the compared state
// references and does not hold. The plan-stage failure names the table, since
// the plan ran against it.
func TestRehearsalCoreNamesTheAddedObjectsAPlanFailsAgainst(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	targetPath := filepath.Join(dir, "target.db")
	target, err := sql.Open("sqlite", targetPath)
	c.Assert(err, qt.IsNil)
	_, err = target.Exec(`CREATE TABLE parents (id INTEGER PRIMARY KEY);
		CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents (id))`)
	c.Assert(err, qt.IsNil)
	c.Assert(target.Close(), qt.IsNil)
	targetConn := connectSQLiteForWiring(c, targetPath)
	devConn := connectSQLiteForWiring(c, filepath.Join(dir, "dev.db"))
	read, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), targetConn, nil)
	c.Assert(err, qt.IsNil)
	current := *read
	current.Tables = slices.DeleteFunc(slices.Clone(read.Tables), func(table catalog.Table) bool { return table.Name == "parents" })

	err = rehearseStatementsOnDev(c.Context(), targetConn, devConn, devclean.Baseline{}, &current, targetHolding(read),
		migrator.MigrationTxModeNone, []string{"INSERT INTO missing (id) VALUES (1)"}, must.Must(builtin.New()))

	c.Assert(err, qt.ErrorMatches, `(?s)dev database simulation failed during plan: .*no such table: missing.*; `+
		`the rehearsal recreates these objects from outside the compared scope because the planned statements need them: table "parents"; `+
		`the plan was not applied to the target database`)
}
