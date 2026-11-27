package postgresreplay_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devclean/replay"
)

var trustedScope = replay.Scope{Database: "ptah_dev", TrustedSQL: true}

// check_foreign_key runs its column arguments as SQL, so a function a trigger
// argument calls runs when the trigger's table is written. Measured on
// PostgreSQL 18 and YugabyteDB 2026.1, the first argument below turns
// standard_conforming_strings off when a row of app.events is deleted. A call
// the guard refuses in a statement is refused in a trigger's argument.
func TestGuard_TriggerArgumentCall_FailurePath(t *testing.T) {
	tests := []struct {
		name, argument, want string
	}{
		{
			name:     "set_config in a column argument",
			argument: `'pid = pid AND pg_catalog.set_config(''standard_conforming_strings'', ''off'', false) IS NOT NULL AND pid'`,
			want:     `cluster control function "set_config"`,
		},
		{
			name:     "a control function in an escape string",
			argument: `E'id = pg\137terminate_backend(1)::int'`,
			want:     `cluster control function "pg_terminate_backend"`,
		},
		{
			name:     "a server file function",
			argument: `'id = length(pg_read_file(''/etc/hostname''))'`,
			want:     `server file read "pg_read_file"`,
		},
	}
	for _, dialect := range []string{"postgres", "yugabytedb"} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				statement := `CREATE TRIGGER events_fk AFTER DELETE ON app.events FOR EACH ROW EXECUTE FUNCTION check_foreign_key(1, 'cascade', ` +
					test.argument + `, 'app.notes', 'event_id')`
				c.Assert(sessionBoundGuard(c, dialect).Validate(statement, databaseScope), qt.ErrorMatches,
					dialect+` migration replay rejects `+regexp.QuoteMeta(test.want+` called by the trigger argument `+test.argument)+refusalEnd)
			})
		}
	}
}

// A trigger argument that calls a function the guard lets a statement call is
// accepted, as is a server file function under trust.
func TestGuard_TriggerArgumentCall_HappyPath(t *testing.T) {
	tests := []struct {
		name, argument string
		scope          replay.Scope
	}{
		{"a column argument that calls lower", `'id = lower(id)'`, databaseScope},
		{"a server file function under trust", `'id = length(pg_read_file(''/etc/hostname''))'`, trustedScope},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statement := `CREATE TRIGGER events_fk AFTER DELETE ON app.events FOR EACH ROW EXECUTE FUNCTION check_foreign_key(1, 'cascade', ` +
				test.argument + `, 'app.notes', 'event_id')`
			c.Assert(sessionBoundGuard(c, "postgres").Validate(statement, test.scope), qt.IsNil)
		})
	}
}

// A function that reads, lists or changes a file of the server's host, and a
// COPY that reads or writes one, reach past the dev database. Measured on
// PostgreSQL 18, `SELECT pg_read_file('/etc/hostname')` and `COPY t FROM
// '/etc/hostname'` read the host's file into the run. The large object
// functions stay refused under trust; pg_import_system_collations, whose
// argument the guard cannot read, does too.
func TestGuard_ServerFileAccess_FailurePath(t *testing.T) {
	tests := []struct {
		statement, want string
		scope           replay.Scope
	}{
		{`SELECT pg_read_file('/etc/hostname')`, "server file read", databaseScope},
		{`SELECT pg_catalog.pg_read_binary_file('/etc/hostname')`, "server file read", databaseScope},
		{`SELECT (pg_stat_file('postmaster.pid')).size`, "server file status read", databaseScope},
		{`SELECT * FROM pg_ls_dir('.')`, "server directory listing", databaseScope},
		{`SELECT * FROM pg_ls_waldir()`, "server directory listing", databaseScope},
		{`SELECT * FROM pg_ls_logdir()`, "server directory listing", databaseScope},
		{`SELECT pg_file_write('x', 'y', false)`, "server file write", databaseScope},
		{`INSERT INTO app.files VALUES (pg_read_file('/etc/hostname'))`, "server file read", databaseScope},
		{`COPY app.files FROM '/etc/hostname'`, "external COPY", databaseScope},
		{`COPY app.files (line) FROM '/etc/hostname' WITH (FORMAT text)`, "external COPY", databaseScope},
		{`COPY "from" FROM '/etc/hostname'`, "external COPY", databaseScope},
		{`COPY app.files FROM E'/etc/hostname'`, "external COPY", databaseScope},
		{`COPY app.files FROM $$/etc/hostname$$`, "external COPY", databaseScope},
		{`COPY app.files TO '/tmp/files'`, "external COPY", databaseScope},
		{`COPY app.files TO 'stdout'`, "external COPY", databaseScope},
		{`COPY (SELECT 1) TO '/tmp/one'`, "external COPY", databaseScope},
		{`COPY app.files FROM PROGRAM 'cat /etc/hostname'`, "external COPY", databaseScope},
		{`SELECT lo_import('/etc/hostname')`, "large object operation", trustedScope},
		{`SELECT lo_export(16400, '/tmp/x')`, "large object operation", trustedScope},
		{`SELECT pg_import_system_collations('app'::regnamespace)`, "collation import", databaseScope},
		{`SELECT pg_catalog.pg_import_system_collations('pg_catalog'::regnamespace)`, "collation import", trustedScope},
	}
	for _, test := range tests {
		t.Run(test.statement, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sessionBoundGuard(c, "postgres").Validate(test.statement, test.scope), qt.ErrorMatches,
				`postgres migration replay rejects `+test.want+refusalEnd)
		})
	}
}

// A COPY of the client's streams stays in the run, and trust lifts a server
// file function and a COPY of a file, as it lifts a COPY to a program.
func TestGuard_ServerFileAccess_HappyPath(t *testing.T) {
	tests := []struct {
		statement string
		scope     replay.Scope
	}{
		{`COPY app.files FROM STDIN`, databaseScope},
		{`COPY app.files (line) FROM STDIN WITH (FORMAT csv, DELIMITER ',')`, databaseScope},
		{`COPY app.files TO STDOUT`, databaseScope},
		{`COPY (SELECT line FROM app.files WHERE line SIMILAR TO 'a%') TO STDOUT`, databaseScope},
		{`SELECT pg_read_file('/etc/hostname')`, trustedScope},
		{`SELECT * FROM pg_ls_dir('.')`, trustedScope},
		{`COPY app.files FROM '/etc/hostname'`, trustedScope},
	}
	for _, test := range tests {
		t.Run(test.statement, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sessionBoundGuard(c, "postgres").Validate(test.statement, test.scope), qt.IsNil)
		})
	}
}

// A table that inherits from a relation of a namespace the server owns, or is
// its partition, changes the catalog's own relation, which records the child,
// and a write to that relation reaches the child. Measured on PostgreSQL 18,
// `CREATE TABLE t () INHERITS (pg_catalog.pg_authid)` succeeds for a
// superuser. It is refused in every scope.
func TestGuard_InheritCatalogTable_FailurePath(t *testing.T) {
	tests := []struct {
		statement, want string
		scope           replay.Scope
	}{
		{`CREATE TABLE app.t () INHERITS (pg_catalog.pg_authid)`, `protected namespace "pg_catalog" mutation` + refusalEnd, databaseScope},
		{`CREATE UNLOGGED TABLE app.t () INHERITS (app.base, pg_authid)`,
			`protected namespace "pg_catalog" mutation through the unqualified name "pg_authid"` + refusalEnd + qualify, databaseScope},
		{`CREATE TABLE app.t PARTITION OF pg_catalog.pg_class FOR VALUES IN (1)`, `protected namespace "pg_catalog" mutation` + refusalEnd, databaseScope},
		{`ALTER TABLE app.t INHERIT pg_catalog.pg_description`, `protected namespace "pg_catalog" mutation` + refusalEnd, databaseScope},
		{`ALTER TABLE app.t NO INHERIT pg_catalog.pg_description`, `protected namespace "pg_catalog" mutation` + refusalEnd, databaseScope},
		{`ALTER TABLE app.p ATTACH PARTITION information_schema.sql_features FOR VALUES IN (1)`,
			`protected namespace "information_schema" mutation` + refusalEnd, databaseScope},
		{`CREATE TABLE app.t () INHERITS (pg_catalog.pg_authid)`, `protected namespace "pg_catalog" mutation` + refusalEnd, trustedScope},
	}
	for _, test := range tests {
		t.Run(test.statement, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sessionBoundGuard(c, "postgres").Validate(test.statement, test.scope), qt.ErrorMatches,
				`postgres migration replay rejects `+test.want)
		})
	}
}

func TestGuard_InheritCatalogTable_HappyPath(t *testing.T) {
	for _, statement := range []string{
		`CREATE TABLE app.t () INHERITS (app.base)`,
		`ALTER TABLE app.t NO INHERIT app.base`,
		`CREATE TABLE app.p1 PARTITION OF app.p FOR VALUES IN (1)`,
		`ALTER TABLE app.p ATTACH PARTITION app.p2 FOR VALUES IN (2)`,
	} {
		t.Run(statement, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sessionBoundGuard(c, "postgres").Validate(statement, databaseScope), qt.IsNil)
		})
	}
}

// CockroachDB reads a name of three parts as database, schema and object, and
// a name of two parts whose first part is no schema of the current database
// as a database and an object of its public schema. Measured on 26.4, `INSERT
// INTO other.t` and `INSERT INTO other.public.t` from a session of the dev
// database write the table t of the database other, which the cleanup does
// not touch. A schema the replay dropped is no schema of the dev database.
func TestGuard_CrossDatabaseName_FailurePath(t *testing.T) {
	tests := []struct {
		setup             []string
		statement, quoted string
	}{
		{nil, `INSERT INTO other.t VALUES (1)`, "other.t"},
		{nil, `INSERT INTO other.public.t VALUES (1)`, "other.public.t"},
		{nil, `UPSERT INTO other.public.t VALUES (1)`, "other.public.t"},
		{nil, `UPDATE other.public.t SET id = 2`, "other.public.t"},
		{nil, `DELETE FROM other.t WHERE id = 1`, "other.t"},
		{nil, `WITH x AS (INSERT INTO other.t VALUES (1) RETURNING 1) SELECT * FROM x`, "other.t"},
		{nil, `TRUNCATE app.t, other.public.t`, "other.public.t"},
		{nil, `COPY other.public.t FROM STDIN`, "other.public.t"},
		{nil, `CREATE TABLE other.public.t (id int)`, "other.public.t"},
		{nil, `CREATE TABLE IF NOT EXISTS other.t (id int)`, "other.t"},
		{nil, `CREATE INDEX t_id ON other.public.t (id)`, "other.public.t"},
		{nil, `DROP TABLE app.t, other.public.t`, "other.public.t"},
		{nil, `ALTER TABLE other.public.t ADD COLUMN note text`, "other.public.t"},
		{nil, `CREATE SCHEMA other.s`, "other.s"},
		{nil, `COMMENT ON COLUMN other.public.t.id IS 'x'`, "other.public.t"},
		{nil, `COMMENT ON TABLE other.t IS 'x'`, "other.t"},
		{nil, `GRANT SELECT ON TABLE other.public.t TO app_reader`, "other.public.t"},
		{[]string{`CREATE SCHEMA s`, `DROP SCHEMA s CASCADE`}, `CREATE TABLE s.t (id int)`, "s.t"},
	}
	for _, test := range tests {
		t.Run(test.statement, func(t *testing.T) {
			c := qt.New(t)
			guard := sessionBoundGuard(c, "cockroachdb")
			acceptAll(c, guard, test.setup)
			c.Assert(guard.Validate(test.statement, databaseScope), qt.ErrorMatches,
				`cockroachdb migration replay rejects name "`+regexp.QuoteMeta(test.quoted)+`" of another database`+refusalEnd)
		})
	}
}

// A name of the dev database's schemas, of the schemas the replay creates,
// and of the dev database itself stays in the dev database, and so does a
// read of another database. Trust lifts the rule, and PostgreSQL reads a name
// of two parts as a schema and refuses one of another database itself.
func TestGuard_CrossDatabaseName_HappyPath(t *testing.T) {
	tests := []struct {
		dialect    string
		statements []string
		scope      replay.Scope
	}{
		{"cockroachdb", []string{`INSERT INTO app.t VALUES (1)`}, databaseScope},
		{"cockroachdb", []string{`INSERT INTO ptah_dev.app.t VALUES (1)`}, databaseScope},
		{"cockroachdb", []string{`INSERT INTO ptah_dev.t VALUES (1)`}, databaseScope},
		{"cockroachdb", []string{`COMMENT ON COLUMN app.t.id IS 'x'`}, databaseScope},
		{"cockroachdb", []string{`CREATE SCHEMA s`, `CREATE TABLE s.t (id int)`, `INSERT INTO s.t VALUES (1)`}, databaseScope},
		{"cockroachdb", []string{`INSERT INTO app.t SELECT id FROM other.public.t`}, databaseScope},
		{"cockroachdb", []string{`INSERT INTO other.t VALUES (1)`}, trustedScope},
		{"postgres", []string{`INSERT INTO other.t VALUES (1)`}, databaseScope},
	}
	for _, test := range tests {
		t.Run(test.dialect+"/"+test.statements[len(test.statements)-1], func(t *testing.T) {
			c := qt.New(t)
			guard := sessionBoundGuard(c, test.dialect)
			for _, statement := range test.statements {
				c.Assert(guard.Validate(statement, test.scope), qt.IsNil, qt.Commentf("%s", statement))
			}
		})
	}
}
