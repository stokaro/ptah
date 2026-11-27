package postgresreplay_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devclean/replay"
)

// sessionCatalog is a dev session's answer about its catalog: the names its
// search path reaches, by class and namespace, as PostgreSQL 18, YugabyteDB
// and CockroachDB give them. public is a schema the user owns, so the guard
// keeps none of its names, and it is the schema the session creates in.
const sessionCatalog = `[
	["r", "pg_catalog", ["pg_settings", "pg_authid", "pg_database", "pg_description", "pg_class", "pg_class_oid_index"]],
	["r", "pg_extension", ["spatial_ref_sys"]],
	["r", "public", ["users", "pg_shadow_copy"]],
	["f", "pg_catalog", ["now", "pg_read_file", "sum"]],
	["f", "public", ["app_helper"]],
	["t", "pg_catalog", ["int4", "money", "point", "box"]],
	["c", "pg_catalog", ["C", "unicode"]],
	["v", "pg_catalog", ["utf8_to_iso_8859_1"]],
	["o", "pg_catalog", ["+"]],
	["k", "pg_catalog", ["int4_ops"]],
	["s", "pg_catalog", ["english"]]
]`

// sessionBoundGuard is the guard of dialect as it comes back from reading a
// dev session whose catalog holds [sessionCatalog] and that creates in public.
func sessionBoundGuard(c *qt.C, dialect string) replay.Guard {
	c.Helper()
	return creatingGuard(c, dialect, "public")
}

// creatingGuard is the guard of dialect bound to a dev session whose catalog
// holds [sessionCatalog] and whose search path lists the schemas of path that
// exist; none is a path that names no schema.
func creatingGuard(c *qt.C, dialect string, path ...string) replay.Guard {
	c.Helper()
	session := reportedSession{encoding: "UTF8", conforming: "on", catalog: sessionCatalog, path: path, asked: new(int)}
	bound, err := sessionGuardFor(c, dialect).ValidateSession(c.Context(), session, replay.Scope{Database: "ptah_dev"})
	c.Assert(err, qt.IsNil)
	return bound
}

const (
	refusalEnd = ` because its effects cannot be confined to the disposable database realm`
	qualify    = `; qualify the name with the schema of the object it means`
)

// The server looks an unqualified name up in pg_catalog before any schema of
// the search path, so a name the catalog holds there reaches the catalog's
// object. Measured on PostgreSQL 18, `UPDATE pg_settings SET setting = 'off'`
// turns standard_conforming_strings off, and the next statement the guard
// reads as one SELECT creates a role. Each statement the guard reads a name
// in is refused for a name of the class it names, written without a schema,
// and the refusal says to qualify it.
func TestGuard_UnqualifiedCatalogName_FailurePath(t *testing.T) {
	tests := []struct {
		name, dialect, statement, wantErr string
	}{
		{"UPDATE", "postgres", `UPDATE pg_settings SET setting = 'off' WHERE name = 'standard_conforming_strings'`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_settings"` + refusalEnd + qualify},
		{"UPDATE in a CTE", "postgres", `WITH x AS (UPDATE pg_settings SET setting = 'off' RETURNING name) SELECT name FROM x`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_settings"` + refusalEnd + qualify},
		{"INSERT", "postgres", `INSERT INTO pg_description VALUES (1, 1, 1, 'x')`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_description"` + refusalEnd + qualify},
		{"DELETE", "postgres", `DELETE FROM pg_description`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_description"` + refusalEnd + qualify},
		{"MERGE", "postgres", `MERGE INTO pg_description AS d USING app.src AS s ON true WHEN MATCHED THEN DELETE`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_description"` + refusalEnd + qualify},
		{"TRUNCATE", "postgres", `TRUNCATE app.events, pg_description`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_description"` + refusalEnd + qualify},
		{"ALTER TABLE", "postgres", `ALTER TABLE pg_authid ADD COLUMN x int`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_authid"` + refusalEnd + qualify},
		{"ALTER VIEW", "postgres", `ALTER VIEW pg_settings OWNER TO app_owner`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_settings"` + refusalEnd + qualify},
		{"ALTER INDEX", "postgres", `ALTER INDEX pg_class_oid_index SET (fillfactor = 50)`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_class_oid_index"` + refusalEnd + qualify},
		{"DROP", "postgres", `DROP TABLE IF EXISTS app.events, pg_description`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_description"` + refusalEnd + qualify},
		{"COMMENT ON TABLE", "postgres", `COMMENT ON TABLE pg_authid IS 'x'`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_authid"` + refusalEnd + qualify},
		{"COMMENT ON COLUMN", "postgres", `COMMENT ON COLUMN pg_authid.rolname IS 'x'`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_authid"` + refusalEnd + qualify},
		{"SECURITY LABEL", "postgres", `SECURITY LABEL ON TABLE pg_authid IS 'x'`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_authid"` + refusalEnd + qualify},
		{"GRANT with no kind", "postgres", `GRANT SELECT ON pg_authid TO PUBLIC`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_authid"` + refusalEnd + qualify},
		{"GRANT ON TABLE", "postgres", `GRANT UPDATE ON TABLE pg_database TO PUBLIC`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_database"` + refusalEnd + qualify},
		{"REVOKE", "postgres", `REVOKE SELECT ON pg_authid FROM PUBLIC`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_authid"` + refusalEnd + qualify},
		{"LOCK", "postgres", `LOCK TABLE app.events, pg_authid IN SHARE MODE`,
			`postgres migration replay rejects LOCK of protected namespace "pg_catalog" through the unqualified name "pg_authid"` + refusalEnd + qualify},
		{"COPY FROM", "postgres", `COPY pg_description FROM STDIN`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_description"` + refusalEnd + qualify},
		{"COPY FROM with columns", "postgres", `COPY pg_description (objoid, description) FROM STDIN`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_description"` + refusalEnd + qualify},
		{"CREATE INDEX ON", "postgres", `CREATE INDEX i ON pg_description (objoid)`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_description"` + refusalEnd + qualify},
		{"CREATE TRIGGER ON", "postgres", `CREATE TRIGGER t AFTER INSERT ON pg_description FOR EACH ROW EXECUTE FUNCTION app.f()`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_description"` + refusalEnd + qualify},
		{"CREATE POLICY ON", "postgres", `CREATE POLICY p ON pg_authid USING (true)`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_authid"` + refusalEnd + qualify},
		{"CREATE RULE TO", "postgres", `CREATE RULE r AS ON INSERT TO pg_description DO INSTEAD NOTHING`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_description"` + refusalEnd + qualify},
		{"CREATE STATISTICS FROM", "postgres", `CREATE STATISTICS app.s ON objoid, classoid FROM pg_description`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_description"` + refusalEnd + qualify},
		{"CREATE INDEX of a builtin's name ON", "postgres", `CREATE INDEX pg_class_oid_index ON pg_description (objoid)`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_description"` + refusalEnd + qualify},
		{"REINDEX", "postgres", `REINDEX TABLE pg_class`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_class"` + refusalEnd + qualify},
		{"a routine", "postgres", `GRANT EXECUTE ON FUNCTION pg_read_file(text) TO PUBLIC`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_read_file"` + refusalEnd + qualify},
		{"a routine without the prefix", "postgres", `COMMENT ON FUNCTION now() IS 'x'`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "now"` + refusalEnd + qualify},
		{"a type", "postgres", `COMMENT ON TYPE int4 IS 'x'`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "int4"` + refusalEnd + qualify},
		{"a domain", "postgres", `DROP DOMAIN int4`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "int4"` + refusalEnd + qualify},
		{"a collation", "postgres", `COMMENT ON COLLATION "C" IS 'x'`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "C"` + refusalEnd + qualify},
		{"a conversion", "postgres", `COMMENT ON CONVERSION utf8_to_iso_8859_1 IS 'x'`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "utf8_to_iso_8859_1"` + refusalEnd + qualify},
		{"an operator", "postgres", `COMMENT ON OPERATOR + (int4, int4) IS 'x'`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "\+"` + refusalEnd + qualify},
		{"an operator class", "postgres", `COMMENT ON OPERATOR CLASS int4_ops USING btree IS 'x'`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "int4_ops"` + refusalEnd + qualify},
		{"a text search configuration", "postgres", `COMMENT ON TEXT SEARCH CONFIGURATION english IS 'x'`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "english"` + refusalEnd + qualify},
		{"a quoted name in another case", "postgres", `UPDATE "PG_SETTINGS" SET setting = 'off'`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "PG_SETTINGS"` + refusalEnd + qualify},
		{"a Unicode escape", "postgres", `UPDATE U&"pg\005fsettings" SET setting = 'off'`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_settings"` + refusalEnd + qualify},
		{"CockroachDB pg_extension", "cockroachdb", `UPDATE spatial_ref_sys SET srtext = 'x'`,
			`cockroachdb migration replay rejects protected namespace "pg_extension" mutation through the unqualified name "spatial_ref_sys"` + refusalEnd + qualify},
		{"YugabyteDB", "yugabytedb", `UPDATE pg_settings SET setting = 'off' WHERE name = 'standard_conforming_strings'`,
			`yugabytedb migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_settings"` + refusalEnd + qualify},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sessionBoundGuard(c, test.dialect).Validate(test.statement, replay.Scope{Database: "ptah_dev"}),
				qt.ErrorMatches, test.wantErr)
		})
	}
}

// The refusal holds in every scope: a server the run owns and a run that
// trusts its SQL keep the mutation of a protected namespace refused, written
// with the namespace or without it.
func TestGuard_UnqualifiedCatalogName_FailurePathEveryScope(t *testing.T) {
	scopes := []struct {
		name  string
		scope replay.Scope
	}{
		{name: "database", scope: replay.Scope{Database: "ptah_dev"}},
		{name: "server", scope: replay.Scope{Database: "ptah_dev", Server: true}},
		{name: "provisioned server", scope: replay.Scope{Database: "ptah_dev", Server: true, Provisioned: true}},
		{name: "trusted", scope: replay.Scope{Database: "ptah_dev", TrustedSQL: true}},
	}
	statements := []string{
		`UPDATE pg_authid SET rolcreatedb = true WHERE rolname = 'app'`,
		`GRANT EXECUTE ON FUNCTION pg_read_file(text) TO PUBLIC`,
	}
	for _, scope := range scopes {
		for _, statement := range statements {
			t.Run(scope.name+"/"+statement, func(t *testing.T) {
				c := qt.New(t)
				c.Assert(sessionBoundGuard(c, "postgres").Validate(statement, scope.scope), qt.ErrorMatches,
					`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name .*`+qualify)
			})
		}
	}
}

// A name written with a schema the server owns is refused as it is written,
// and the refusal says nothing about qualifying it. COPY ... FROM and ALTER
// INDEX write to the table and the index they name, and a Unicode escape is
// read as the namespace it spells.
func TestGuard_QualifiedCatalogName_FailurePath(t *testing.T) {
	tests := []struct {
		name, statement string
	}{
		{"UPDATE", `UPDATE pg_catalog.pg_settings SET setting = 'off'`},
		{"COPY FROM", `COPY pg_catalog.pg_description FROM STDIN`},
		{"ALTER INDEX", `ALTER INDEX pg_catalog.pg_class_oid_index SET (fillfactor = 50)`},
		{"a Unicode escape", `UPDATE U&"\0070g_catalog".pg_settings SET setting = 'off'`},
		{"a Unicode escape with its escape character", `UPDATE U&"!0070g_catalog" UESCAPE '!'.pg_settings SET setting = 'off'`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sessionBoundGuard(c, "postgres").Validate(test.statement, replay.Scope{Database: "ptah_dev"}),
				qt.ErrorMatches, `postgres migration replay rejects protected namespace "pg_catalog" mutation`+refusalEnd)
		})
	}
}

// A name the catalog does not hold in a namespace the server owns reaches the
// user's own object, and a name qualified with the user's schema reaches that
// schema's object, however it is spelled: the rule is the catalog's names,
// not their pg_ prefix. Reading a catalog, and COPY ... TO, write nothing.
func TestGuard_UnqualifiedCatalogName_HappyPath(t *testing.T) {
	tests := []struct {
		name, statement string
	}{
		{"a table of the user's", `UPDATE users SET name = 'x'`},
		{"a pg_ name the catalog does not hold", `UPDATE pg_shadow_copy SET name = 'x'`},
		{"a catalog's name in the user's schema", `UPDATE app.pg_settings SET setting = 'x'`},
		{"the same, quoted", `UPDATE "public"."pg_authid" SET rolname = 'x'`},
		{"a column of the user's table named like a catalog", `COMMENT ON COLUMN app.pg_authid.rolname IS 'x'`},
		{"a routine of the user's", `DROP FUNCTION IF EXISTS app_helper(int)`},
		{"a qualified routine named like a catalog's", `COMMENT ON FUNCTION app.now() IS 'x'`},
		{"a privilege on the user's table", `GRANT SELECT ON users TO app_reader`},
		{"a catalog type a column uses", `CREATE TABLE app.counters (n int4 NOT NULL)`},
		{"a catalog read", `SELECT name, setting FROM pg_settings`},
		{"COPY TO", `COPY pg_description TO STDOUT`},
		{"COPY of a query", `COPY (SELECT relname FROM pg_class) TO STDOUT`},
		{"a lock on the user's table", `LOCK TABLE users IN SHARE MODE`},
		{"a Unicode escape of the user's", `CREATE TABLE app.U&"caf\00e9" (id int)`},
		{"a bitwise operator over two columns", `SELECT u & "mask" FROM app.flags`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sessionBoundGuard(c, "postgres").Validate(test.statement, replay.Scope{Database: "ptah_dev"}), qt.IsNil)
		})
	}
}

// A guard that read no session reads an unqualified name by the prefixes the
// relations of each supported line begin with.
func TestGuard_UnqualifiedCatalogNameWithoutSession_FailurePath(t *testing.T) {
	tests := []struct {
		name, dialect, statement, wantErr string
	}{
		{"PostgreSQL", "postgres", `UPDATE pg_settings SET setting = 'off'`,
			`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "pg_settings"` + refusalEnd + qualify},
		{"a YugabyteDB view", "yugabytedb", `COMMENT ON VIEW yb_servers_metrics IS 'x'`,
			`yugabytedb migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "yb_servers_metrics"` + refusalEnd + qualify},
		{"a CockroachDB PostGIS table", "cockroachdb", `UPDATE spatial_ref_sys SET srtext = 'x'`,
			`cockroachdb migration replay rejects protected namespace "pg_extension" mutation through the unqualified name "spatial_ref_sys"` + refusalEnd + qualify},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(guardFor(c, test.dialect).Validate(test.statement, replay.Scope{Database: "ptah_dev"}), qt.ErrorMatches, test.wantErr)
		})
	}
}

// The prefixes belong to the member that uses them: PostGIS keeps
// spatial_ref_sys in a schema the user owns on PostgreSQL, and yb_ is a name
// like any other there.
func TestGuard_UnqualifiedCatalogNameWithoutSession_HappyPath(t *testing.T) {
	for _, statement := range []string{
		`UPDATE spatial_ref_sys SET srtext = 'x'`,
		`COMMENT ON VIEW yb_servers_metrics IS 'x'`,
	} {
		t.Run(statement, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(guardFor(c, "postgres").Validate(statement, replay.Scope{Database: "ptah_dev"}), qt.IsNil)
		})
	}
}

// A cluster control function is refused by its name, written with pg_catalog
// or without it, quoted, or as a Unicode escape that spells it: the server
// finds each in pg_catalog, and each reloads the server's configuration or
// ends a session the run does not own.
func TestGuard_ControlFunctionName_FailurePath(t *testing.T) {
	dialects := []string{"postgres", "yugabytedb", "cockroachdb"}
	statements := []string{
		`SELECT pg_reload_conf()`,
		`SELECT pg_catalog.pg_reload_conf()`,
		`SELECT "pg_reload_conf"()`,
		`SELECT U&"pg_reload_con\0066"()`,
		`SELECT pg_terminate_backend(42)`,
		`SELECT pg_catalog.pg_terminate_backend(42)`,
		`SELECT "pg_terminate_backend"(42)`,
		`SELECT U&"pg_terminate_backen\0064"(42)`,
	}
	for _, dialect := range dialects {
		for _, statement := range statements {
			t.Run(dialect+"/"+statement, func(t *testing.T) {
				c := qt.New(t)
				c.Assert(sessionBoundGuard(c, dialect).Validate(statement, replay.Scope{Database: "ptah_dev"}), qt.ErrorMatches,
					dialect+` migration replay rejects cluster control function`+refusalEnd)
			})
		}
	}
}

// An escape the server refuses spells no name, and the guard refuses the
// statement rather than read one the server would not.
func TestGuard_UnicodeEscapedIdentifier_FailurePath(t *testing.T) {
	for _, statement := range []string{
		`UPDATE U&"\zz" SET a = 1`,
		`UPDATE U&"!0070" UESCAPE 'a' SET a = 1`,
	} {
		t.Run(statement, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sessionBoundGuard(c, "postgres").Validate(statement, replay.Scope{Database: "ptah_dev"}), qt.ErrorMatches,
				`postgres migration replay rejects a U&"\.\.\." identifier whose escapes the guard cannot decode`+refusalEnd)
		})
	}
}
