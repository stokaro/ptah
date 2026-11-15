//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
	"ptah.run/internal/replaytrust"
)

// A command that loads a desired-state file on the dev database writes it
// there from its model: schema inspect of the file, and schema diff with the
// file on either side. A role the file declares, as a bare CREATE ROLE or as
// the role-bootstrap DO block the SQL reader turns into one, is the server's,
// and the dev database cleanup does not reach it. So on a server the run
// neither owns nor trusts the SQL on, the load refuses before it writes
// anything and names the role and the remedies. Without the refusal, schema
// inspect in both binaries creates the role on such a server and leaves it
// there for every later run to meet.

// declaredRoleShapes are the two spellings of a role in a desired-state SQL
// file. Each grants the role a privilege, so an inspection that read the dev
// database back names the role only if the load created it.
var declaredRoleShapes = []struct {
	name string
	sql  func(role string) string
}{
	{
		name: "a bare CREATE ROLE",
		sql: func(role string) string {
			return fmt.Sprintf("CREATE ROLE %[1]s NOLOGIN;\nCREATE TABLE t (id int);\nGRANT SELECT ON t TO %[1]s;\n", role)
		},
	},
	{
		name: "a role-bootstrap DO block",
		sql: func(role string) string {
			return fmt.Sprintf(`DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '%[1]s') THEN
    CREATE ROLE %[1]s NOLOGIN;
  END IF;
END
$$;
CREATE TABLE t (id int);
GRANT SELECT ON t TO %[1]s;
`, role)
		},
	},
}

// materializedRoleFixture is a desired-state file, a scratch dev database and
// a scratch target on the PostgreSQL test server, the role the file declares,
// and an admin connection that reads the server's roles back.
type materializedRoleFixture struct {
	schema, devURL, targetURL, role string
	admin                           *dbschema.DatabaseConnection
}

// newMaterializedRoleFixture writes the file sql renders for a fresh role,
// with both declarations unset. The role is dropped after the scratch
// databases, whatever the run left.
func newMaterializedRoleFixture(c *qt.C, sql func(role string) string) materializedRoleFixture {
	c.Helper()
	role := fmt.Sprintf("ptah_materialized_%d", time.Now().UnixNano())
	admin, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), `DROP ROLE IF EXISTS "`+role+`"`)
		c.Check(dropErr, qt.IsNil)
		dbschema.CloseAndWarn(admin)
	})
	schema := filepath.Join(c.TempDir(), "desired.sql")
	c.Assert(os.WriteFile(schema, []byte(sql(role)), 0o600), qt.IsNil)
	devURL, _ := scratchReplayDatabase(c)
	targetURL, _ := scratchReplayDatabase(c)
	envbooltest.Unset(devdocker.DisposableServerEnvVar)(c)
	envbooltest.Unset(replaytrust.EnvVar)(c)
	return materializedRoleFixture{schema: schema, devURL: devURL, targetURL: targetURL, role: role, admin: admin}
}

// roles counts the roles on the server named fx.role.
func (fx materializedRoleFixture) roles(c *qt.C) int {
	c.Helper()
	var count int
	c.Assert(fx.admin.QueryRowContext(c.Context(),
		"SELECT count(*) FROM pg_roles WHERE rolname = $1", fx.role,
	).Scan(&count), qt.IsNil)
	return count
}

// materializingCommand is one command that loads the file on the dev
// database. prefix is what its error says before the refusal.
type materializingCommand struct {
	name   string
	run    func(args ...string) (string, error)
	args   func(fx materializedRoleFixture) []string
	prefix string
}

// compatMaterializingCommands and nativeMaterializingCommands are the
// commands of each binary that load the file.
var (
	compatMaterializingCommands = []materializingCommand{
		{
			name: "ptah-compat schema inspect",
			run:  runCompatVerb,
			args: func(fx materializedRoleFixture) []string {
				return []string{"schema", "inspect", "-u", "file://" + fx.schema, "--dev-url", fx.devURL, "--format", "{{ sql . }}"}
			},
			prefix: "materialize schema on dev database: ",
		},
		{
			name: "ptah-compat schema diff",
			run:  runCompatVerb,
			args: func(fx materializedRoleFixture) []string {
				return []string{"schema", "diff", "--from", "file://" + fx.schema, "--to", fx.targetURL, "--dev-url", fx.devURL}
			},
			prefix: "load --from schema: materialize schema on dev database: ",
		},
	}
	nativeMaterializingCommands = []materializingCommand{
		{
			name: "ptah schema inspect",
			run:  runPtahNativeWithError,
			args: func(fx materializedRoleFixture) []string {
				return []string{"schema", "inspect", "--schema-file", fx.schema, "--dev-url", fx.devURL, "--format", "sql"}
			},
			prefix: "materialize schema on dev database: ",
		},
		{
			name: "ptah schema diff",
			run:  runPtahNativeWithError,
			args: func(fx materializedRoleFixture) []string {
				return []string{"schema", "diff", "--from", "file://" + fx.schema, "--to", fx.targetURL, "--dev-url", fx.devURL}
			},
			prefix: "load --from schema: materialize schema on dev database: ",
		},
	}
	materializingCommands = slices.Concat(compatMaterializingCommands, nativeMaterializingCommands)
)

// unconfinedRoleRefusal is the refusal for role, after the command's prefix.
func unconfinedRoleRefusal(role string) string {
	return fmt.Sprintf(`the desired schema declares role %q, which the dev server does not hold, `+
		`and creating it there cannot be confined to the dev database realm: `+
		`a role belongs to the server, not to the dev database, and outlives the run's cleanup; `+
		`if nothing else uses this server, declare it disposable with PTAH_DEV_SERVER_DISPOSABLE=1, `+
		`or use a docker:// or docker+<driver>:// dev URL; `+
		`if you trust this SQL to run with the dev connection's privileges, set PTAH_TRUST_REPLAY_SQL=1`, role)
}

// TestDevMaterializationRefusesARoleOnASharedServerE2E pins the strict
// default: each command refuses in the same words for both spellings, and the
// role is not on the server afterwards.
func TestDevMaterializationRefusesARoleOnASharedServerE2E(t *testing.T) {
	for _, shape := range declaredRoleShapes {
		for _, test := range materializingCommands {
			t.Run(shape.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				fx := newMaterializedRoleFixture(c, shape.sql)

				out, err := test.run(test.args(fx)...)

				c.Assert(err, qt.ErrorMatches, `(?s).*`+regexp.QuoteMeta(test.prefix+unconfinedRoleRefusal(fx.role)), qt.Commentf("%s", out))
				c.Assert(fx.roles(c), qt.Equals, 0)
			})
		}
	}
}

// TestDevMaterializationCreatesAndKeepsATrustedRoleE2E pins the trusted run:
// PTAH_TRUST_REPLAY_SQL set and the server not disposable. Each command creates
// the role, and the role stays on the server, since trust widens nothing the
// cleanup drops.
func TestDevMaterializationCreatesAndKeepsATrustedRoleE2E(t *testing.T) {
	for _, shape := range declaredRoleShapes {
		for _, test := range materializingCommands {
			t.Run(shape.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				fx := newMaterializedRoleFixture(c, shape.sql)
				envbooltest.Set(replaytrust.EnvVar, "1")(c)

				out, err := test.run(test.args(fx)...)

				c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
				c.Assert(fx.roles(c), qt.Equals, 1)
			})
		}
	}
}

// TestDevMaterializationCreatesAndKeepsARoleTrustedByTheFlagE2E is the same
// for the native flag, with the variable absent. The inspection names the
// grant, which it read back from the dev database.
func TestDevMaterializationCreatesAndKeepsARoleTrustedByTheFlagE2E(t *testing.T) {
	for _, shape := range declaredRoleShapes {
		for _, test := range nativeMaterializingCommands {
			t.Run(shape.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				fx := newMaterializedRoleFixture(c, shape.sql)

				out, err := test.run(append(test.args(fx), "--trust-replay-sql")...)

				c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
				c.Assert(fx.roles(c), qt.Equals, 1)
			})
		}
	}
}

// TestDevMaterializationInspectReadsTheCreatedRoleBackE2E pins that the role
// was created on the dev server rather than only rendered: the inspection is
// read back from the dev database, and it holds the grant to the role, which
// the server accepts only for a role it holds. It runs on a disposable server,
// where the role is then dropped with the run, and on a trusted one, where it
// stays.
func TestDevMaterializationInspectReadsTheCreatedRoleBackE2E(t *testing.T) {
	states := []struct {
		name      string
		setup     []func(testing.TB)
		wantRoles int
	}{
		{"disposable", []func(testing.TB){envbooltest.Set(devdocker.DisposableServerEnvVar, "1")}, 0},
		{"trusted", []func(testing.TB){envbooltest.Set(replaytrust.EnvVar, "1")}, 1},
		{"disposable and trusted", []func(testing.TB){
			envbooltest.Set(devdocker.DisposableServerEnvVar, "1"),
			envbooltest.Set(replaytrust.EnvVar, "1"),
		}, 0},
	}
	inspect := []materializingCommand{compatMaterializingCommands[0], nativeMaterializingCommands[0]}
	for _, state := range states {
		for _, shape := range declaredRoleShapes {
			for _, test := range inspect {
				t.Run(state.name+"/"+shape.name+"/"+test.name, func(t *testing.T) {
					c := qt.New(t)
					fx := newMaterializedRoleFixture(c, shape.sql)
					for _, setup := range state.setup {
						setup(c)
					}

					out, err := test.run(test.args(fx)...)

					c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
					c.Assert(out, qt.Contains, fmt.Sprintf(`GRANT SELECT ON TABLE "t" TO %q;`, fx.role))
					c.Assert(fx.roles(c), qt.Equals, state.wantRoles)
				})
			}
		}
	}
}

// TestDevMaterializationOnADisposableServerDropsTheRoleE2E pins the owned
// server for every command: the load creates the role, and the cleanup drops
// it with the run.
func TestDevMaterializationOnADisposableServerDropsTheRoleE2E(t *testing.T) {
	for _, shape := range declaredRoleShapes {
		for _, test := range materializingCommands {
			t.Run(shape.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				fx := newMaterializedRoleFixture(c, shape.sql)
				envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(c)

				out, err := test.run(test.args(fx)...)

				c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
				c.Assert(fx.roles(c), qt.Equals, 0)
			})
		}
	}
}

// devDatabaseObjects is a desired state with routines, a policy and a view and
// no role: everything it creates lives in the dev database and is dropped with
// it, so it needs no declaration on any server.
const devDatabaseObjects = `CREATE SCHEMA app;
CREATE TABLE app.orders (id bigint PRIMARY KEY, total int NOT NULL);
CREATE FUNCTION app.order_count() RETURNS bigint LANGUAGE sql STABLE AS $$ SELECT count(*) FROM app.orders $$;
ALTER TABLE app.orders ENABLE ROW LEVEL SECURITY;
CREATE POLICY orders_visible ON app.orders USING (app.order_count() >= 0);
CREATE VIEW app.order_totals AS SELECT app.order_count() AS n;
`

// TestDevMaterializationLoadsDatabaseObjectsWithoutADeclarationE2E is the
// control for the refusal: a file that declares routines, a policy and a view
// and no role loads on a server the run neither owns nor trusts the SQL on.
func TestDevMaterializationLoadsDatabaseObjectsWithoutADeclarationE2E(t *testing.T) {
	for _, test := range materializingCommands {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fx := newMaterializedRoleFixture(c, func(string) string { return devDatabaseObjects })

			out, err := test.run(test.args(fx)...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "order_count")
		})
	}
}

// TestSchemaPlanDoesNotMaterializeARoleE2E pins that `ptah schema plan` is
// outside the rule: it plans the role without loading the file on the dev
// database, so it passes on a server the run neither owns nor trusts the SQL
// on, and the server holds no role afterwards. The command takes no trust
// flag, and a set variable changes nothing.
func TestSchemaPlanDoesNotMaterializeARoleE2E(t *testing.T) {
	states := []struct {
		name  string
		setup func(testing.TB)
	}{
		{"variable absent", envbooltest.Unset(replaytrust.EnvVar)},
		{"variable set", envbooltest.Set(replaytrust.EnvVar, "1")},
	}
	for _, state := range states {
		t.Run(state.name, func(t *testing.T) {
			c := qt.New(t)
			fx := newMaterializedRoleFixture(c, declaredRoleShapes[1].sql)
			state.setup(c)

			out, err := runPtahNativeWithError("schema", "plan", "--db-url", fx.targetURL,
				"--schema-file", fx.schema, "--dev-url", fx.devURL, "--dry-run")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, fmt.Sprintf(`CREATE ROLE \"%s\"`, fx.role))
			c.Assert(fx.roles(c), qt.Equals, 0)
		})
	}
}
