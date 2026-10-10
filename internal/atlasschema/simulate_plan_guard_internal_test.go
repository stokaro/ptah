package atlasschema

// White-box testing required: the guard a plan rehearsal runs, and its place
// in the rehearsal core, are unexported; the command path that reaches them
// needs a live dev server, and the live tests cover that end.

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/engine/builtin"
	"ptah.run/internal/devclean"
	"ptah.run/internal/devdocker"
	"ptah.run/migration/migrator"
)

// namedPostgresDev is a dev database on a server the operator named, which
// may hold other databases and roles.
var namedPostgresDev = catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public", URL: "postgres://ptah@guard-named-server:5432/r3_dev"}

// devDatabaseEscapes are plan statements whose effect the cleanup of a dev
// database leaves behind on the server.
var devDatabaseEscapes = []struct {
	name      string
	statement string
}{
	{name: "a role", statement: "CREATE ROLE r3_probe_role NOLOGIN"},
	{name: "a routine body", statement: "CREATE FUNCTION touch() RETURNS void LANGUAGE plpgsql AS $$ BEGIN PERFORM 1; END $$"},
}

// TestGuardRehearsedPlan_DevDatabase_FailurePath refuses a plan statement
// whose effect outlives the cleanup of a dev database the operator named, and
// names the two ways to a server the run owns.
//
// The rehearsal held the baseline to this guard and not the plan, which comes
// from the less trusted desired schema: a CREATE ROLE ran for real on the
// shared dev server, outlived the run, and failed the real apply on the role
// it had left (stokaro/ptah#4294).
func TestGuardRehearsedPlan_DevDatabase_FailurePath(t *testing.T) {
	for _, test := range devDatabaseEscapes {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := guardRehearsedPlan([]string{"CREATE TABLE items (id bigint PRIMARY KEY)", test.statement}, namedPostgresDev)

			c.Assert(err, qt.ErrorMatches, `(?s)statement 2 cannot be rehearsed on the dev database: `+
				`postgres migration replay rejects .* because its effects cannot be confined to the disposable database realm; `+
				`if nothing else uses this server, declare it disposable with PTAH_DEV_SERVER_DISPOSABLE=1, `+
				`or use a docker:// or docker\+<driver>:// dev URL`)
		})
	}
}

// TestGuardRehearsedPlan_DevDatabase_KeptComment refuses a comment the reset
// of a dev database keeps, on a server the operator declared disposable as
// well: that server outlives the run, so only a server the run provisions
// lifts it, and the refusal says so.
func TestGuardRehearsedPlan_DevDatabase_KeptComment(t *testing.T) {
	c := qt.New(t)
	disposable := namedPostgresDev
	disposable.URL = "postgres://ptah@guard-disposable-dev-database:5432/r3_dev"
	_, release, err := devdocker.Resolve(c.Context(), disposable.URL, devdocker.Options{DeclaredDisposable: true})
	c.Assert(err, qt.IsNil)
	c.Cleanup(release)
	want := `statement 1 cannot be rehearsed on the dev database: ` +
		`postgres migration replay rejects COMMENT ON global metadata because its effects cannot be confined to the disposable database realm; ` +
		`use a docker:// or docker\+<driver>:// dev URL, since a server declared disposable keeps this after the run`

	c.Assert(guardRehearsedPlan([]string{"COMMENT ON SCHEMA public IS 'kept'"}, namedPostgresDev), qt.ErrorMatches, want)
	c.Assert(guardRehearsedPlan([]string{"COMMENT ON SCHEMA public IS 'kept'"}, disposable), qt.ErrorMatches, want)
}

// TestGuardRehearsedPlan_DevDatabase_HappyPath is the control: what stays in
// the dev database is rehearsed on a named server, and a server the operator
// declared the run's own rehearses the statements above too.
func TestGuardRehearsedPlan_DevDatabase_HappyPath(t *testing.T) {
	t.Run("a table on a named server", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(guardRehearsedPlan([]string{"CREATE TABLE items (id bigint PRIMARY KEY)"}, namedPostgresDev), qt.IsNil)
	})
	for _, test := range devDatabaseEscapes {
		t.Run(test.name+" on an owned server", func(t *testing.T) {
			c := qt.New(t)
			owned := namedPostgresDev
			owned.URL = "postgres://ptah@guard-owned-dev-database:5432/r3_dev"
			_, release, err := devdocker.Resolve(c.Context(), owned.URL, devdocker.Options{DeclaredDisposable: true})
			c.Assert(err, qt.IsNil)
			c.Cleanup(release)

			c.Assert(guardRehearsedPlan([]string{test.statement}, owned), qt.IsNil)
		})
	}
}

// TestRehearsalCoreGuardsThePlanOnADevDatabase pins the guard's call site in
// the rehearsal core for a plan re-scoped onto one dev database, the branch
// that ran no guard. The lint is taken away, so the refusal can only be the
// guard's; the engine would refuse the ATTACH too, with another message.
func TestRehearsalCoreGuardsThePlanOnADevDatabase(t *testing.T) {
	c := qt.New(t)
	original := checkPlanStatements
	checkPlanStatements = func([]string, string) error { return nil }
	c.Cleanup(func() { checkPlanStatements = original })
	dir := t.TempDir()
	devConn := connectSQLiteForWiring(c, filepath.Join(dir, "dev.db"))
	targetConn := connectSQLiteForWiring(c, filepath.Join(dir, "target.db"))

	err := rehearseStatementsOnDev(context.Background(), targetConn, devConn, devclean.Baseline{}, nil, migrator.MigrationTxModeNone,
		[]string{fmt.Sprintf("ATTACH DATABASE '%s' AS victim", filepath.Join(dir, "victim.db"))}, must.Must(builtin.New()))

	c.Assert(err, qt.ErrorMatches, `statement 1 cannot be rehearsed on the dev database: sqlite migration replay rejects ATTACH .*`)
}
