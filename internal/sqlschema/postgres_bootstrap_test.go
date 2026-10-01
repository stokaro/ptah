package sqlschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

func TestPostgresRoleBootstrap(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		roles []schemamodel.Role
	}{
		{
			name:  "conditional role with attributes",
			sql:   `DO $bootstrap$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'reader') THEN CREATE ROLE reader NOLOGIN NOINHERIT CREATEDB; END IF; END; $bootstrap$;`,
			roles: []schemamodel.Role{{Name: "reader", CreateDB: true}},
		},
		{
			name:  "repeat guard keeps the first declaration",
			sql:   `CREATE ROLE reader NOLOGIN; DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'reader') THEN CREATE ROLE reader LOGIN; END IF; END $$;`,
			roles: []schemamodel.Role{{Name: "reader", Inherit: true}},
		},
		{
			name:  "executed branch and nested block",
			sql:   `DO $$ BEGIN IF FALSE THEN CREATE ROLE skipped; ELSE BEGIN IF TRUE THEN CREATE ROLE selected LOGIN; END IF; END; END IF; END $$;`,
			roles: []schemamodel.Role{{Name: "selected", Login: true, Inherit: true}},
		},
		{
			name:  "existence depends on preceding declarations",
			sql:   `DO $$ BEGIN CREATE ROLE first; IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'first') THEN CREATE ROLE second; END IF; END $$;`,
			roles: []schemamodel.Role{{Name: "first", Inherit: true}, {Name: "second", Inherit: true}},
		},
		{
			name:  "quoted name and escaped literal",
			sql:   `DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'Reader''s "Role"') THEN CREATE ROLE "Reader's ""Role""" NOLOGIN; END IF; END $$ LANGUAGE plpgsql;`,
			roles: []schemamodel.Role{{Name: `Reader's "Role"`, Inherit: true}},
		},
		{
			name:  "continued role-name literal",
			sql:   "CREATE ROLE reader; DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rea'\n'der') THEN CREATE ROLE reader LOGIN; END IF; END $$;",
			roles: []schemamodel.Role{{Name: "reader", Inherit: true}},
		},
		{
			name:  "case-folded identifier",
			sql:   `DO $$ BEGIN CREATE ROLE Reader; IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'reader') THEN CREATE ROLE unwanted; END IF; END $$;`,
			roles: []schemamodel.Role{{Name: "reader", Inherit: true}},
		},
		{
			name:  "comments are trivia",
			sql:   `DO $$ BEGIN IF NOT /* CREATE ROLE fake; */ EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'reader') THEN CREATE ROLE reader; END IF; END $$;`,
			roles: []schemamodel.Role{{Name: "reader", Inherit: true}},
		},
		{
			name:  "empty block",
			roles: make([]schemamodel.Role, 0),
			sql:   `DO $$ BEGIN NULL; END $$;`,
		},
		{
			name:  "missing application role condition is false",
			roles: make([]schemamodel.Role, 0),
			sql:   `DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'absent') THEN CREATE ROLE skipped; END IF; END $$;`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, _, err := sqlschema.Read([]byte(test.sql), "postgres")
			c.Assert(err, qt.IsNil)
			c.Assert(db.Roles, qt.DeepEquals, test.roles)
		})
	}
}

func TestPostgresRoleBootstrapRefusesUnknownSemantics(t *testing.T) {
	tests := []struct{ name, sql string }{
		{name: "contradictory options", sql: `DO $$ BEGIN CREATE ROLE r LOGIN NOLOGIN; END $$;`},
		{name: "misplaced WITH", sql: `DO $$ BEGIN CREATE ROLE r LOGIN WITH; END $$;`},
		{name: "built-in role declaration", sql: `DO $$ BEGIN CREATE ROLE pg_hidden; END $$;`},
		{name: "unterminated comment", sql: `DO $$ BEGIN CREATE ROLE r; END; /* missing end $$;`},
		{name: "nested comment", sql: `DO $$ BEGIN /* outer /* nested */ comment */ NULL; END $$;`},
		{name: "hash is not a comment", sql: "DO $$ BEGIN NULL; END; # invalid\n$$;"},
		{name: "non-PostgreSQL identifier quote", sql: "DO $$ BEGIN CREATE ROLE `reader`; END $$;"},
		{name: "single-quoted identifier", sql: `DO $$ BEGIN CREATE ROLE 'reader'; END $$;`},
		{name: "alter role", sql: `DO $$ BEGIN CREATE ROLE r; ALTER ROLE r LOGIN; END $$;`},
		{name: "drop role", sql: `DO $$ BEGIN CREATE ROLE r; DROP ROLE r; END $$;`},
		{name: "oversized identifier", sql: `DO $$ BEGIN CREATE ROLE ` + strings.Repeat("x", 64) + `; END $$;`},
		{name: "deep nesting", sql: `DO $$ ` + strings.Repeat("BEGIN ", 66) + `NULL; ` + strings.Repeat("END; ", 66) + `$$;`},
		{name: "dynamic SQL", sql: `DO $$ BEGIN EXECUTE 'CREATE ROLE hidden'; END $$;`},
		{name: "unknown branch condition", sql: `DO $$ BEGIN IF current_user = 'postgres' THEN CREATE ROLE r; END IF; END $$;`},
		{name: "unsupported dead branch", sql: `DO $$ BEGIN IF FALSE THEN EXECUTE 'CREATE ROLE hidden'; END IF; END $$;`},
		{name: "unknown statement", sql: `DO $$ BEGIN PERFORM 1; END $$;`},
		{name: "non-role DDL", sql: `DO $$ BEGIN CREATE TABLE hidden (id int); END $$;`},
		{name: "exception handler", sql: `DO $$ BEGIN CREATE ROLE r; EXCEPTION WHEN duplicate_object THEN NULL; END $$;`},
		{name: "variables", sql: `DO $$ DECLARE r text; BEGIN NULL; END $$;`},
		{name: "other language", sql: `DO $$ BEGIN NULL; END $$ LANGUAGE sql;`},
		{name: "built-in role condition", sql: `DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pg_monitor') THEN CREATE ROLE r; END IF; END $$;`},
		{name: "duplicate executed role", sql: `DO $$ BEGIN CREATE ROLE r; CREATE ROLE r; END $$;`},
		{name: "extra predicate", sql: `DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'r' AND rolcanlogin) THEN CREATE ROLE r; END IF; END $$;`},
		{name: "escape literal", sql: `DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = E'r') THEN CREATE ROLE r; END IF; END $$;`},
		{name: "trailing statement inside body", sql: `DO $$ BEGIN NULL; END; CREATE ROLE hidden; $$;`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, _, err := sqlschema.Read([]byte(test.sql), "postgres")
			c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
			c.Assert(db, qt.DeepEquals, schemamodel.Database{})
		})
	}
}

func TestPostgresRoleBootstrapRedactsInvalidRole(t *testing.T) {
	c := qt.New(t)
	_, _, err := sqlschema.Read([]byte(`DO $$ BEGIN CREATE ROLE reader PASSWORD 'do-not-leak' unsupported; END $$;`), "postgres")
	c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
	c.Assert(err.Error(), qt.Not(qt.Contains), "do-not-leak")
}

func TestPostgresRoleBootstrapDuplicateBoundaries(t *testing.T) {
	tests := []struct{ name, sql string }{
		{name: "block then top-level", sql: `DO $$ BEGIN CREATE ROLE r NOLOGIN; END $$; CREATE ROLE r LOGIN;`},
		{name: "top-level then block", sql: `CREATE ROLE r NOLOGIN; DO $$ BEGIN CREATE ROLE r LOGIN; END $$;`},
		{name: "two top-level declarations", sql: `CREATE ROLE r NOLOGIN; CREATE ROLE r LOGIN;`},
		{name: "normalized identifier", sql: `DO $$ BEGIN CREATE ROLE Reader; END $$; CREATE ROLE "reader";`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := sqlschema.Read([]byte(test.sql), "postgres")
			c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
			c.Assert(err.Error(), qt.Contains, "already declared")
			c.Assert(database, qt.DeepEquals, schemamodel.Database{})
		})
	}
}
