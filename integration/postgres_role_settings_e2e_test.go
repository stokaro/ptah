//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	"github.com/jackc/pgx/v5"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// roleSettingsPassword is the password every role of these tests logs in
// with. It protects nothing: the roles live as long as one test.
const roleSettingsPassword = "Settings1password"

// roleSettingsServer is a scratch database on the PostgreSQL test server, an
// admin connection to the server, and the roles a test creates there, which
// are server objects and outlive the database.
type roleSettingsServer struct {
	target string
	admin  *dbschema.DatabaseConnection
}

// newRoleSettingsServer registers the removal of roles before the scratch
// database is created, so the database, and everything a role owns in it, is
// gone by the time the roles are dropped.
func newRoleSettingsServer(c *qt.C, roles ...string) roleSettingsServer {
	c.Helper()
	admin, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		for _, role := range roles {
			_, dropErr := admin.ExecContext(context.Background(), `DROP ROLE IF EXISTS `+quoteRole(role))
			c.Check(dropErr, qt.IsNil, qt.Commentf("drop role %s", role))
		}
		dbschema.CloseAndWarn(admin)
	})
	target, _ := scratchReplayDatabase(c)
	return roleSettingsServer{target: target, admin: admin}
}

func quoteRole(role string) string { return `"` + strings.ReplaceAll(role, `"`, `""`) + `"` }

// uniqueRole is a role name no other run on the shared server takes.
func uniqueRole(prefix string) string { return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano()) }

// storedSettings is what pg_db_role_setting keeps for role in every
// database, each entry `name=value` as the server wrote it, sorted.
func (s roleSettingsServer) storedSettings(c *qt.C, role string) []string {
	c.Helper()
	var stored []string
	rows, err := s.admin.QueryContext(c.Context(), `
		SELECT entry FROM (
			SELECT unnest(s.setconfig) AS entry
			FROM pg_catalog.pg_db_role_setting s JOIN pg_catalog.pg_roles r ON r.oid = s.setrole
			WHERE r.rolname = $1 AND s.setdatabase = 0
		) held
		ORDER BY entry COLLATE "C"`, role)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	for rows.Next() {
		var entry string
		c.Assert(rows.Scan(&entry), qt.IsNil)
		stored = append(stored, entry)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return stored
}

// roleExists reports whether the server holds role.
func (s roleSettingsServer) roleExists(c *qt.C, role string) bool {
	c.Helper()
	var exists bool
	c.Assert(s.admin.QueryRowContext(c.Context(), `SELECT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = $1)`, role).Scan(&exists), qt.IsNil)
	return exists
}

// login opens a new session authenticated as role, which loads the role's
// defaults the way an application's connection does. SET ROLE would not.
func (s roleSettingsServer) login(c *qt.C, role string) *pgx.Conn {
	c.Helper()
	parsed, err := url.Parse(s.target)
	c.Assert(err, qt.IsNil)
	parsed.User = url.UserPassword(role, roleSettingsPassword)
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, parsed.String())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// setting is what a session reads for name: the value SHOW prints, or the
// empty string for a custom parameter the session does not hold.
func setting(c *qt.C, conn *pgx.Conn, name string) string {
	c.Helper()
	var value string
	c.Assert(conn.QueryRow(c.Context(), `SELECT coalesce(current_setting($1, true), '')`, name).Scan(&value), qt.IsNil)
	return value
}

// writeSchema writes body to a file named name in a new directory and
// returns its path.
func writeSchema(c *qt.C, name, body string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), name)
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	return path
}

// roleSettingsYAML declares role, which logs in, with settings and resets in
// the attribute spelling.
func roleSettingsYAML(c *qt.C, role, settings, resets string) string {
	c.Helper()
	body := "roles:\n  r:\n    name: " + role + "\n    login: true\n    password: " + roleSettingsPassword + "\n"
	body += "    settings: \"" + settings + "\"\n"
	body += "    reset_settings: \"" + resets + "\"\n"
	return writeSchema(c, "schema.yaml", body)
}

const synced = "Schema is synced, no changes to be made."

// TestPostgresRoleSettingsLive_Lifecycle drives one role through what a
// declaration can do to its settings: store them, change one, reset one by
// name, and converge after each step. A setting the declaration does not name
// is set out of band and survives every apply. The values are read back from
// pg_db_role_setting and from SHOW in a new session authenticated as the
// role; a session opened before a change keeps the values it started with,
// and SET ROLE loads none of them.
func TestPostgresRoleSettingsLive_Lifecycle(t *testing.T) {
	c := qt.New(t)
	role := uniqueRole("m5973_life")
	server := newRoleSettingsServer(c, role)

	first := roleSettingsYAML(c, role, `statement_timeout = '30s'; search_path = '$user', public; app.tenant = 'acme'`, "work_mem")
	out, err := runPtahNativeWithError("schema", "apply", "--schema-file", first, "--db-url", server.target, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("first apply:\n%s", out))
	c.Assert(server.storedSettings(c, role), qt.DeepEquals, []string{`app.tenant=acme`, `search_path="$user", public`, `statement_timeout=30s`})
	before := server.login(c, role)
	c.Assert([]string{setting(c, before, "statement_timeout"), setting(c, before, "search_path"), setting(c, before, "app.tenant")},
		qt.DeepEquals, []string{"30s", `"$user", public`, "acme"})
	out, err = runPtahNativeWithError("schema", "apply", "--schema-file", first, "--db-url", server.target, "--dry-run")
	c.Assert(err, qt.IsNil, qt.Commentf("converged dry run:\n%s", out))
	c.Assert(out, qt.Contains, synced)

	_, err = server.admin.ExecContext(c.Context(), `ALTER ROLE `+quoteRole(role)+` SET lock_timeout = '5s'`)
	c.Assert(err, qt.IsNil)
	second := roleSettingsYAML(c, role, `statement_timeout = '45s'; search_path = '$user', public`, "app.tenant, work_mem")
	out, err = runPtahNativeWithError("schema", "apply", "--schema-file", second, "--db-url", server.target, "--dry-run")
	c.Assert(err, qt.IsNil, qt.Commentf("second dry run:\n%s", out))
	c.Assert(out, qt.Contains, `ALTER ROLE "`+role+`" RESET "app"."tenant";`)
	c.Assert(out, qt.Contains, `ALTER ROLE "`+role+`" SET "statement_timeout" = '45s';`)
	c.Assert(out, qt.Not(qt.Contains), "lock_timeout")
	c.Assert(out, qt.Not(qt.Contains), "work_mem")
	out, err = runPtahNativeWithError("schema", "apply", "--schema-file", second, "--db-url", server.target, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("second apply:\n%s", out))
	c.Assert(server.storedSettings(c, role), qt.DeepEquals, []string{`lock_timeout=5s`, `search_path="$user", public`, `statement_timeout=45s`})
	after := server.login(c, role)
	c.Assert([]string{setting(c, after, "statement_timeout"), setting(c, after, "app.tenant"), setting(c, after, "lock_timeout")},
		qt.DeepEquals, []string{"45s", "", "5s"})
	c.Assert([]string{setting(c, before, "statement_timeout"), setting(c, before, "app.tenant")}, qt.DeepEquals, []string{"30s", "acme"})
	out, err = runPtahNativeWithError("schema", "apply", "--schema-file", second, "--db-url", server.target, "--dry-run")
	c.Assert(err, qt.IsNil, qt.Commentf("converged dry run:\n%s", out))
	c.Assert(out, qt.Contains, synced)

	var own, assumed string
	c.Assert(server.admin.QueryRowContext(c.Context(), `SELECT current_setting('statement_timeout')`).Scan(&own), qt.IsNil)
	tx, err := server.admin.BeginTx(c.Context(), nil)
	c.Assert(err, qt.IsNil)
	_, err = tx.ExecContext(c.Context(), `SET LOCAL ROLE `+quoteRole(role))
	c.Assert(err, qt.IsNil)
	c.Assert(tx.QueryRowContext(c.Context(), `SELECT current_setting('statement_timeout')`).Scan(&assumed), qt.IsNil)
	c.Assert(tx.Rollback(), qt.IsNil)
	c.Assert(assumed, qt.Equals, own)
}

// TestPostgresRoleSettingsLive_EverySourceFormat applies one declaration in
// each format that spells settings, each for a role whose name needs quotes,
// and reads back what the server stores for it. A second run finds nothing to
// do.
func TestPostgresRoleSettingsLive_EverySourceFormat(t *testing.T) {
	sqlRole, goRole, yamlRole := uniqueRole(`M5973 "Sql"`), uniqueRole("M5973 Go"), uniqueRole("M5973.Yaml")
	tests := []struct {
		name, role, flag, file, body string
	}{
		{name: "sql", role: sqlRole, flag: "--schema-file", file: "schema.sql", body: "CREATE ROLE " + quoteRole(sqlRole) + " LOGIN;\n" +
			"ALTER ROLE " + quoteRole(sqlRole) + " SET statement_timeout TO '30s';\n" +
			"ALTER ROLE " + quoteRole(sqlRole) + " SET search_path = \"$user\", public;\n" +
			"ALTER USER " + quoteRole(sqlRole) + " SET \"App\".\"Tenant\" = 'acme';\n" +
			"ALTER ROLE " + quoteRole(sqlRole) + " RESET work_mem;\n"},
		{name: "go", role: goRole, flag: "--root-dir", file: "roles.go", body: "package roles\n\n" +
			`//ptah:schema:role name="` + goRole + `" login="true" settings="statement_timeout = '30s'; search_path = '$user', public; \"App\".\"Tenant\" = 'acme'" reset_settings="work_mem"` +
			"\ntype Role struct{}\n"},
		{name: "yaml", role: yamlRole, flag: "--schema-file", file: "schema.yaml", body: "roles:\n  r:\n    name: \"" + yamlRole + "\"\n    login: true\n" +
			"    settings: \"statement_timeout = '30s'; search_path = '$user', public; \\\"App\\\".\\\"Tenant\\\" = 'acme'\"\n    reset_settings: work_mem\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			server := newRoleSettingsServer(c, test.role)
			source := writeSchema(c, test.file, test.body)
			argument := map[string]string{"--schema-file": source, "--root-dir": filepath.Dir(source)}[test.flag]

			out, err := runPtahNativeWithError("schema", "apply", test.flag, argument, "--db-url", server.target, "--auto-approve")
			c.Assert(err, qt.IsNil, qt.Commentf("apply:\n%s", out))
			c.Assert(server.storedSettings(c, test.role), qt.DeepEquals, []string{`App.Tenant=acme`, `search_path="$user", public`, `statement_timeout=30s`})
			out, err = runPtahNativeWithError("schema", "apply", test.flag, argument, "--db-url", server.target, "--dry-run")
			c.Assert(err, qt.IsNil, qt.Commentf("dry run:\n%s", out))
			c.Assert(out, qt.Contains, synced)
		})
	}
}

// TestPostgresRoleSettingsLive_TheServersSpellingPlansOnce sets a value the
// declaration spells another way. The server keeps the spelling a statement
// gives it, so the first comparison plans the declared spelling and the
// second finds the role converged rather than planning it on every run.
func TestPostgresRoleSettingsLive_TheServersSpellingPlansOnce(t *testing.T) {
	c := qt.New(t)
	role := uniqueRole("m5973_spelling")
	server := newRoleSettingsServer(c, role)
	_, err := server.admin.ExecContext(c.Context(), `CREATE ROLE `+quoteRole(role)+` LOGIN PASSWORD '`+roleSettingsPassword+`'`)
	c.Assert(err, qt.IsNil)
	_, err = server.admin.ExecContext(c.Context(), `ALTER ROLE `+quoteRole(role)+` SET statement_timeout = 30000`)
	c.Assert(err, qt.IsNil)
	schema := roleSettingsYAML(c, role, `statement_timeout = '30s'`, "work_mem")

	planned, err := runPtahNativeWithError("schema", "apply", "--schema-file", schema, "--db-url", server.target, "--dry-run")
	c.Assert(err, qt.IsNil, qt.Commentf("dry run:\n%s", planned))
	applied, err := runPtahNativeWithError("schema", "apply", "--schema-file", schema, "--db-url", server.target, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("apply:\n%s", applied))
	again, err := runPtahNativeWithError("schema", "apply", "--schema-file", schema, "--db-url", server.target, "--dry-run")
	c.Assert(err, qt.IsNil, qt.Commentf("dry run:\n%s", again))

	c.Assert(planned, qt.Contains, `ALTER ROLE "`+role+`" SET "statement_timeout" = '30s';`)
	c.Assert(server.storedSettings(c, role), qt.DeepEquals, []string{`statement_timeout=30s`})
	c.Assert(again, qt.Contains, synced)
	c.Assert(setting(c, server.login(c, role), "statement_timeout"), qt.Equals, "30s")
}

// TestPostgresRoleSettingsLive_UnmodeledRowsAreReported stores two defaults
// Ptah does not model in the scratch database: one for the role there alone,
// and one for every role there. A comparison names both and leaves both in
// place. Neither is set for every role in every database, which would reach
// every session on the shared server.
func TestPostgresRoleSettingsLive_UnmodeledRowsAreReported(t *testing.T) {
	c := qt.New(t)
	role := uniqueRole("m5973_unmodeled")
	server := newRoleSettingsServer(c, role)
	schema := roleSettingsYAML(c, role, `statement_timeout = '30s'`, "work_mem")
	out, err := runPtahNativeWithError("schema", "apply", "--schema-file", schema, "--db-url", server.target, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("apply:\n%s", out))
	database := strings.TrimPrefix(must.Must(url.Parse(server.target)).Path, "/")
	_, err = server.admin.ExecContext(c.Context(), `ALTER ROLE `+quoteRole(role)+` IN DATABASE "`+database+`" SET work_mem = '8MB'`)
	c.Assert(err, qt.IsNil)
	_, err = server.admin.ExecContext(c.Context(), `ALTER DATABASE "`+database+`" SET lock_timeout = '7s'`)
	c.Assert(err, qt.IsNil)

	out, err = runPtahNativeWithError("schema", "apply", "--schema-file", schema, "--db-url", server.target, "--auto-approve")

	c.Assert(err, qt.IsNil, qt.Commentf("apply:\n%s", out))
	c.Assert(out, qt.Contains, `role setting "work_mem for role `+role+` in database `+database+`" in the database`)
	c.Assert(out, qt.Contains, `role setting "lock_timeout for every role in database `+database+`" in the database`)
	var kept int
	c.Assert(server.admin.QueryRowContext(c.Context(), `
		SELECT count(*) FROM pg_catalog.pg_db_role_setting s JOIN pg_catalog.pg_database d ON d.oid = s.setdatabase
		WHERE d.datname = $1`, database).Scan(&kept), qt.IsNil)
	c.Assert(kept, qt.Equals, 2)
	c.Assert(setting(c, server.login(c, role), "work_mem"), qt.Equals, "8MB")
}

// TestPostgresRoleSettingsLive_PermissionFailureIsReported applies as a role
// that may create roles but not set a parameter only a superuser sets. The
// server's refusal is the command's error, and the transaction it ran in
// leaves the role uncreated.
func TestPostgresRoleSettingsLive_PermissionFailureIsReported(t *testing.T) {
	c := qt.New(t)
	manager, audited := uniqueRole("m5973_manager"), uniqueRole("m5973_audited")
	server := newRoleSettingsServer(c, manager, audited)
	_, err := server.admin.ExecContext(c.Context(), `CREATE ROLE `+quoteRole(manager)+` LOGIN CREATEROLE PASSWORD '`+roleSettingsPassword+`'`)
	c.Assert(err, qt.IsNil)
	database := strings.TrimPrefix(must.Must(url.Parse(server.target)).Path, "/")
	_, err = server.admin.ExecContext(c.Context(), `GRANT CREATE ON DATABASE "`+database+`" TO `+quoteRole(manager))
	c.Assert(err, qt.IsNil)
	parsed := must.Must(url.Parse(server.target))
	parsed.User = url.UserPassword(manager, roleSettingsPassword)
	schema := writeSchema(c, "schema.yaml", "roles:\n  "+audited+":\n    settings: \"log_statement = 'all'\"\n")

	out, err := runPtahNativeWithError("schema", "apply", "--schema-file", schema, "--db-url", parsed.String(), "--auto-approve")

	c.Assert(err, qt.ErrorMatches, `(?s).*permission denied to set parameter "log_statement" \(SQLSTATE 42501\).*`)
	c.Assert(err, qt.ErrorMatches, `(?s).*ALTER ROLE "`+audited+`" SET "log_statement" = 'all'.*`, qt.Commentf("output:\n%s", out))
	c.Assert(server.roleExists(c, audited), qt.IsFalse)
}

// TestPostgresRoleSettingsLive_RefusedFormsChangeNothing hands the command
// each form Ptah refuses to model. Each is refused by name while the schema is
// read, and the role the file also declares is not created.
func TestPostgresRoleSettingsLive_RefusedFormsChangeNothing(t *testing.T) {
	tests := []struct{ name, statement, want string }{
		{name: "in database", statement: "ALTER ROLE %s IN DATABASE postgres SET work_mem = '1MB'", want: `(?s).*IN DATABASE stores a default for one database.*`},
		{name: "every role", statement: "ALTER ROLE ALL SET work_mem = '1MB'", want: `(?s).*ALTER ROLE ALL stores a default for every role.*`},
		{name: "from current", statement: "ALTER ROLE %s SET work_mem FROM CURRENT", want: `(?s).*FROM CURRENT stores the value of the session.*`},
		{name: "reset all", statement: "ALTER ROLE %s RESET ALL", want: `(?s).*RESET ALL takes away settings the document does not name.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			role := uniqueRole("m5973_refused")
			server := newRoleSettingsServer(c, role)
			body := "CREATE ROLE " + role + ";\n" + strings.ReplaceAll(test.statement, "%s", role) + ";\n"
			schema := writeSchema(c, "schema.sql", body)

			out, err := runPtahNativeWithError("schema", "apply", "--schema-file", schema, "--db-url", server.target, "--auto-approve")

			c.Assert(err, qt.ErrorMatches, test.want, qt.Commentf("output:\n%s", out))
			c.Assert(server.roleExists(c, role), qt.IsFalse)
		})
	}
}
