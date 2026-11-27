package postgresreplay_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devclean/replay"
)

// rulesGuard is the guard of dialect bound to a dev session whose catalog
// holds [sessionCatalog], that creates in public, and whose database holds the
// rules rules, a JSON array of [namespace, relation, definition] as
// pg_get_ruledef writes a definition.
func rulesGuard(c *qt.C, dialect, rules string) replay.Guard {
	c.Helper()
	session := reportedSession{
		encoding: "UTF8", conforming: "on", catalog: sessionCatalog, path: []string{"public"}, rules: rules, asked: new(int),
	}
	bound, err := sessionGuardFor(c, dialect).ValidateSession(c.Context(), session, databaseScope)
	c.Assert(err, qt.IsNil)
	return bound
}

// acceptAll validates statements in order on guard, as a run reads them, and
// asserts each is accepted.
func acceptAll(c *qt.C, guard replay.Guard, statements []string) {
	c.Helper()
	for _, statement := range statements {
		c.Assert(guard.Validate(statement, databaseScope), qt.IsNil, qt.Commentf("%s", statement))
	}
}

// A view whose query reads a relation of pg_catalog is accepted, and a write
// through it is refused: measured on PostgreSQL 18, `UPDATE v SET rolcreatedb
// = true` over `CREATE VIEW v AS SELECT * FROM pg_authid` sets rolcreatedb in
// pg_authid. The view is found under every name a later statement reaches it
// by, and a view over it reaches pg_catalog too. A write that names the
// relation without a schema is told to qualify it, since the relation the
// server finds for the name depends on the search path.
func TestGuard_WriteThroughCatalogView_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		setup    []string
		refused  string
		relation string
		remedy   string
	}{
		{
			name:     "an update through a view of every column",
			setup:    []string{`CREATE VIEW v AS SELECT * FROM pg_authid`},
			refused:  `UPDATE v SET rolcreatedb = true WHERE rolname = 'r'`,
			relation: "v",
			remedy:   qualify,
		},
		{
			name:     "an insert through a qualified view of a qualified relation",
			setup:    []string{`CREATE VIEW public.v AS SELECT * FROM pg_catalog.pg_authid`},
			refused:  `INSERT INTO public.v (rolname) VALUES ('r')`,
			relation: "v",
		},
		{
			name:     "a delete through a view written in another case",
			setup:    []string{`CREATE OR REPLACE VIEW Roles AS SELECT oid, rolname FROM pg_authid`},
			refused:  `DELETE FROM "roles" WHERE rolname = 'r'`,
			relation: "roles",
			remedy:   qualify,
		},
		{
			name:     "a merge through a view",
			setup:    []string{`CREATE VIEW v AS SELECT * FROM pg_authid`},
			refused:  `MERGE INTO v USING app.users AS u ON v.rolname = u.name WHEN MATCHED THEN UPDATE SET rolcreatedb = true`,
			relation: "v",
			remedy:   qualify,
		},
		{
			name:     "a write in a common table expression",
			setup:    []string{`CREATE VIEW v AS SELECT * FROM pg_authid`},
			refused:  `WITH changed AS (UPDATE v SET rolcreatedb = true RETURNING 1) SELECT count(*) FROM changed`,
			relation: "v",
			remedy:   qualify,
		},
		{
			name:     "a view written as TABLE",
			setup:    []string{`CREATE VIEW v AS TABLE pg_authid`},
			refused:  `UPDATE v SET rolcreatedb = true`,
			relation: "v",
			remedy:   qualify,
		},
		{
			name:     "a view over a view",
			setup:    []string{`CREATE VIEW a AS SELECT * FROM pg_authid`, `CREATE VIEW app.b AS SELECT * FROM a`},
			refused:  `UPDATE app.b SET rolcreatedb = true`,
			relation: "b",
		},
		{
			name:     "a renamed view",
			setup:    []string{`CREATE VIEW app.v AS SELECT * FROM pg_authid`, `ALTER VIEW app.v RENAME TO w`},
			refused:  `UPDATE app.w SET rolcreatedb = true`,
			relation: "w",
		},
		{
			name:     "a view moved to another schema",
			setup:    []string{`CREATE VIEW public.v AS SELECT * FROM pg_authid`, `ALTER VIEW public.v SET SCHEMA app`},
			refused:  `UPDATE app.v SET rolcreatedb = true`,
			relation: "v",
		},
		{
			name:     "a view of the settings, whose update rule sets them",
			setup:    []string{`CREATE VIEW s AS SELECT name, setting FROM pg_settings`},
			refused:  `UPDATE s SET setting = 'off' WHERE name = 'standard_conforming_strings'`,
			relation: "s",
			remedy:   qualify,
		},
		{
			name:     "a rule whose action writes through a view",
			setup:    []string{`CREATE VIEW v AS SELECT * FROM pg_authid`},
			refused:  `CREATE RULE grant_createdb AS ON INSERT TO app.events DO ALSO UPDATE v SET rolcreatedb = true`,
			relation: "v",
			remedy:   qualify,
		},
		{
			name:     "a COPY into a view",
			setup:    []string{`CREATE VIEW v AS SELECT * FROM pg_authid`},
			refused:  `COPY v (rolname) FROM STDIN`,
			relation: "v",
			remedy:   qualify,
		},
	}
	for _, dialect := range []string{"postgres", "yugabytedb"} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				guard := sessionBoundGuard(c, dialect)
				acceptAll(c, guard, test.setup)
				c.Assert(guard.Validate(test.refused, databaseScope), qt.ErrorMatches,
					dialect+` migration replay rejects protected namespace "pg_catalog" mutation through the relation "`+
						test.relation+`"`+refusalEnd+test.remedy)
			})
		}
	}
}

// A view of a relation of information_schema reaches it as a view of
// pg_catalog does: sql_features is a table, so a write through a view of it
// writes information_schema.
func TestGuard_WriteThroughInformationSchemaView_FailurePath(t *testing.T) {
	c := qt.New(t)
	guard := sessionBoundGuard(c, "postgres")
	acceptAll(c, guard, []string{`CREATE VIEW features AS SELECT * FROM information_schema.sql_features`})
	c.Assert(guard.Validate(`DELETE FROM features`, databaseScope), qt.ErrorMatches,
		`postgres migration replay rejects protected namespace "information_schema" mutation through the relation "features"`+refusalEnd+qualify)
}

// A view the dev database holds is read from its rules when the session is
// read, so a write through one the run did not create is refused too. The
// schema it is in decides: a relation of its name in another schema is
// another relation.
func TestGuard_WriteThroughSessionView_FailurePath(t *testing.T) {
	tests := []struct {
		name, rules, refused, remedy string
	}{
		{
			name:    "a view of pg_authid",
			rules:   `[["public", "v", "CREATE RULE \"_RETURN\" AS\n    ON SELECT TO public.v DO INSTEAD  SELECT pg_authid.rolname,\n    pg_authid.rolcreatedb\n   FROM pg_authid;"]]`,
			refused: `UPDATE public.v SET rolcreatedb = true`,
		},
		{
			name:    "a view of pg_authid written without a schema",
			rules:   `[["public", "v", "CREATE RULE \"_RETURN\" AS\n    ON SELECT TO public.v DO INSTEAD  SELECT pg_authid.rolname\n   FROM pg_authid;"]]`,
			refused: `UPDATE v SET rolname = 'x'`,
			remedy:  qualify,
		},
		{
			name: "a view of a view of pg_authid, listed first",
			rules: `[["app", "w", "CREATE RULE \"_RETURN\" AS\n    ON SELECT TO app.w DO INSTEAD  SELECT v.rolname\n   FROM public.v;"],` +
				`["public", "v", "CREATE RULE \"_RETURN\" AS\n    ON SELECT TO public.v DO INSTEAD  SELECT pg_authid.rolname\n   FROM pg_authid;"]]`,
			refused: `UPDATE app.w SET rolname = 'x'`,
		},
		{
			name:    "a table whose insert rule writes pg_authid",
			rules:   `[["app", "events", "CREATE RULE grant_createdb AS\n    ON INSERT TO app.events DO  UPDATE pg_authid SET rolcreatedb = true;"]]`,
			refused: `INSERT INTO app.events VALUES (1)`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(rulesGuard(c, "postgres", test.rules).Validate(test.refused, databaseScope), qt.ErrorMatches,
				`postgres migration replay rejects protected namespace "pg_catalog" mutation through the relation "[a-z]+"`+refusalEnd+test.remedy)
		})
	}
}

// The rules of a database the dev session reads that reach nothing are no
// reason to refuse a write: a view of a user's table, and a view a function
// supplies the rows of, as pg_stat_statements' is. A relation of a reaching
// view's name in another schema is another relation.
func TestGuard_WriteThroughSessionView_HappyPath(t *testing.T) {
	tests := []struct {
		name, rules, accepted string
	}{
		{
			name:     "a view of a user's table",
			rules:    `[["app", "active", "CREATE RULE \"_RETURN\" AS\n    ON SELECT TO app.active DO INSTEAD  SELECT events.id\n   FROM app.events\n  WHERE events.active;"]]`,
			accepted: `UPDATE app.active SET id = 2`,
		},
		{
			name: "a view whose rows a function supplies",
			rules: `[["public", "pg_stat_statements", "CREATE RULE \"_RETURN\" AS\n    ON SELECT TO public.pg_stat_statements DO INSTEAD  SELECT pg_stat_statements.userid,\n` +
				`    pg_stat_statements.query\n   FROM pg_stat_statements(true) pg_stat_statements(userid, query);"]]`,
			accepted: `DELETE FROM public.pg_stat_statements WHERE userid = 10`,
		},
		{
			name:     "a view of pg_authid in another schema",
			rules:    `[["public", "roles", "CREATE RULE \"_RETURN\" AS\n    ON SELECT TO public.roles DO INSTEAD  SELECT pg_authid.rolname\n   FROM pg_authid;"]]`,
			accepted: `INSERT INTO app.roles VALUES ('admin')`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(rulesGuard(c, "postgres", test.rules).Validate(test.accepted, databaseScope), qt.IsNil)
		})
	}
}

// CockroachDB writes through no view and keeps no rules, so its session read
// lists none and every write a run makes through one of its views fails on
// the server. A view of a relation of pg_catalog is read the same way all the
// same.
func TestGuard_WriteThroughCatalogViewOnCockroachDB_FailurePath(t *testing.T) {
	c := qt.New(t)
	guard := sessionBoundGuard(c, "cockroachdb")
	acceptAll(c, guard, []string{`CREATE VIEW v AS SELECT * FROM pg_catalog.pg_class`})
	c.Assert(guard.Validate(`UPDATE v SET relname = 'x'`, databaseScope), qt.ErrorMatches,
		`cockroachdb migration replay rejects protected namespace "pg_catalog" mutation through the relation "v"`+refusalEnd+qualify)
}

// A view that reads the catalog is accepted, as is a read through it, and a
// write through a view that reaches no namespace the server owns. A cast to a
// type of pg_catalog and a column of a user's table qualified with a name the
// server reserves read no relation of it.
func TestGuard_WriteThroughCatalogView_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		statements []string
	}{
		{"a view of a catalog relation", []string{
			`CREATE VIEW app.relations AS SELECT relname FROM pg_class`,
			`SELECT * FROM app.relations`,
			`INSERT INTO app.audit SELECT relname FROM app.relations`,
		}},
		{"a view that joins catalog relations", []string{
			`CREATE VIEW app.columns AS SELECT c.relname, a.attname FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_attribute AS a ON a.attrelid = c.oid`,
		}},
		{"a view that counts a catalog relation", []string{
			`CREATE VIEW app.counts AS SELECT relkind, count(*) FROM pg_class GROUP BY relkind`,
		}},
		{"a write through a view of a user's table", []string{
			`CREATE VIEW app.active AS SELECT * FROM app.events WHERE active`,
			`UPDATE app.active SET active = false`,
		}},
		{"a write through a view that casts to a catalog type", []string{
			`CREATE VIEW app.ids AS SELECT id::pg_catalog.int8 AS id, note::pg_catalog.text FROM app.events`,
			`UPDATE app.ids SET id = 2`,
		}},
		{"a write through a view of a user's table named like a catalog schema", []string{
			`CREATE VIEW app.copy AS SELECT pg_shadow_copy.rolname FROM app.pg_shadow_copy`,
			`UPDATE app.copy SET rolname = 'x'`,
		}},
		{"a write to a table named like a catalog view in another schema", []string{
			`CREATE VIEW public.roles AS SELECT rolname FROM pg_authid`,
			`INSERT INTO app.roles VALUES ('admin')`,
		}},
		{"a rule on a user's table that writes a user's table", []string{
			`CREATE RULE log_event AS ON INSERT TO app.events DO ALSO INSERT INTO app.log VALUES (new.id)`,
			`INSERT INTO app.events VALUES (1)`,
		}},
		{"a trigger whose argument is a text search configuration", []string{
			`CREATE TRIGGER events_tsv BEFORE INSERT OR UPDATE ON app.events FOR EACH ROW EXECUTE FUNCTION tsvector_update_trigger(tsv, 'pg_catalog.english', body)`,
		}},
		{"a trigger whose argument names a user's table", []string{
			`CREATE TRIGGER events_fk AFTER DELETE ON app.events FOR EACH ROW EXECUTE FUNCTION check_foreign_key(1, 'cascade', 'id', 'app.notes', 'event_id')`,
		}},
	}
	for _, dialect := range postgresFamily {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				acceptAll(c, sessionBoundGuard(c, dialect), test.statements)
			})
		}
	}
}

// The trigger function check_foreign_key, from the refint extension, runs its
// arguments as SQL: measured on PostgreSQL 18, a trigger whose arguments name
// pg_catalog.pg_description deletes its rows when a row of the trigger's
// table is deleted. A trigger argument that names a relation of a namespace
// the server owns, or a view that reaches one, is refused, as is one the guard
// cannot read.
func TestGuard_TriggerArgumentRelation_FailurePath(t *testing.T) {
	tests := []struct {
		name, statement, namespace, through string
	}{
		{
			name:      "a qualified catalog relation",
			statement: `CREATE TRIGGER events_fk AFTER DELETE ON app.events FOR EACH ROW EXECUTE FUNCTION check_foreign_key(1, 'cascade', 'id', 'pg_catalog.pg_description', 'objoid')`,
			namespace: "pg_catalog",
			through:   `the trigger argument 'pg_catalog.pg_description'`,
		},
		{
			name:      "an unqualified catalog relation",
			statement: `CREATE TRIGGER events_fk AFTER DELETE ON app.events FOR EACH ROW EXECUTE PROCEDURE check_foreign_key(1, 'cascade', 'id', 'pg_description', 'objoid')`,
			namespace: "pg_catalog",
			through:   `the trigger argument 'pg_description'`,
		},
		{
			name:      "an escape string",
			statement: `CREATE TRIGGER events_fk AFTER DELETE ON app.events FOR EACH ROW EXECUTE FUNCTION check_foreign_key(1, 'cascade', 'id', E'pg\137catalog.pg_description', 'objoid')`,
			namespace: "pg_catalog",
			through:   `the trigger argument E'pg\\137catalog.pg_description'`,
		},
		{
			name:      "a quoted identifier with a Unicode escape inside the string",
			statement: `CREATE CONSTRAINT TRIGGER events_fk AFTER DELETE ON app.events FOR EACH ROW EXECUTE FUNCTION check_foreign_key(1, 'cascade', 'id', 'U&"\0070g_catalog".pg_description', 'objoid')`,
			namespace: "pg_catalog",
			through:   `the trigger argument 'U&"\\0070g_catalog".pg_description'`,
		},
		{
			name:      "a dollar-quoted string",
			statement: `CREATE TRIGGER events_fk AFTER DELETE ON app.events FOR EACH ROW EXECUTE FUNCTION check_foreign_key(1, 'cascade', 'id', $$information_schema.sql_features$$, 'feature_id')`,
			namespace: "information_schema",
			through:   `the trigger argument \$\$information_schema.sql_features\$\$`,
		},
		{
			name:      "a string the guard cannot read",
			statement: `CREATE TRIGGER events_fk AFTER DELETE ON app.events FOR EACH ROW EXECUTE FUNCTION check_foreign_key(1, 'cascade', 'id', E'\xff', 'objoid')`,
			namespace: "pg_catalog",
			through:   `the trigger argument E'\\xff', which the guard cannot read`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sessionBoundGuard(c, "postgres").Validate(test.statement, databaseScope), qt.ErrorMatches,
				`postgres migration replay rejects protected namespace "`+test.namespace+`" mutation through `+test.through+refusalEnd)
		})
	}
}

// A trigger argument that names a view the run created over pg_authid is
// refused as a write through the view would be.
func TestGuard_TriggerArgumentView_FailurePath(t *testing.T) {
	c := qt.New(t)
	guard := sessionBoundGuard(c, "postgres")
	acceptAll(c, guard, []string{`CREATE VIEW app.v AS SELECT * FROM pg_authid`})
	c.Assert(guard.Validate(
		`CREATE TRIGGER events_fk AFTER DELETE ON app.events FOR EACH ROW EXECUTE FUNCTION check_foreign_key(1, 'cascade', 'id', 'app.v', 'oid')`,
		databaseScope,
	), qt.ErrorMatches,
		`postgres migration replay rejects protected namespace "pg_catalog" mutation through the trigger argument 'app.v'`+refusalEnd)
}

// PostgreSQL accepts the name of the current database in front of a schema,
// so a write to `ptah_dev.pg_catalog.pg_authid` writes pg_catalog. A name of
// three parts whose first is no database names a column of a user's table.
func TestGuard_DatabaseQualifiedCatalogName_FailurePath(t *testing.T) {
	for _, statement := range []string{
		`UPDATE ptah_dev.pg_catalog.pg_authid SET rolcreatedb = true`,
		`INSERT INTO PTAH_DEV.pg_catalog.pg_description VALUES (1, 2, 0, 'x')`,
		`CREATE TABLE ptah_dev.pg_catalog.leak (id int)`,
	} {
		t.Run(statement, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sessionBoundGuard(c, "postgres").Validate(statement, databaseScope), qt.ErrorMatches,
				`postgres migration replay rejects protected namespace "pg_catalog" mutation`+refusalEnd)
		})
	}
}

func TestGuard_DatabaseQualifiedCatalogName_HappyPath(t *testing.T) {
	for _, statement := range []string{
		`UPDATE ptah_dev.app.events SET id = 2`,
		`COMMENT ON COLUMN app.pg_shadow_copy.rolname IS 'x'`,
	} {
		t.Run(statement, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sessionBoundGuard(c, "postgres").Validate(statement, databaseScope), qt.IsNil)
		})
	}
}

// An operator, an aggregate, a type and an operator class name the functions
// the server calls when the object is used. Measured on PostgreSQL 18 and
// YugabyteDB 2026.1, `SELECT 193 !!! 0::int8` over an operator whose FUNCTION
// is pg_terminate_backend terminates backend 193, and an aggregate whose
// SFUNC is set_config turns standard_conforming_strings off. A function the
// guard refuses as a call is refused where an object names it.
func TestGuard_ReferencedFunction_FailurePath(t *testing.T) {
	tests := []struct {
		name, statement, want string
	}{
		{
			name:      "the function of an operator",
			statement: `CREATE OPERATOR public.!!! (FUNCTION = pg_terminate_backend, LEFTARG = int, RIGHTARG = int8)`,
			want:      `cluster control function "pg_terminate_backend" named by FUNCTION`,
		},
		{
			name:      "the procedure of an operator, qualified",
			statement: `CREATE OPERATOR app.### (PROCEDURE = pg_catalog.pg_cancel_backend, RIGHTARG = int)`,
			want:      `cluster control function "pg_cancel_backend" named by PROCEDURE`,
		},
		{
			name:      "the restriction estimator of an altered operator",
			statement: `ALTER OPERATOR app.=== (int, int) SET (RESTRICT = pg_reload_conf)`,
			want:      `cluster control function "pg_reload_conf" named by RESTRICT`,
		},
		{
			name:      "the state function of an aggregate",
			statement: `CREATE AGGREGATE app.set_all(text, text) (SFUNC = set_config, STYPE = text)`,
			want:      `cluster control function "set_config" named by SFUNC`,
		},
		{
			name:      "the final function of a replaced aggregate",
			statement: `CREATE OR REPLACE AGGREGATE app.reload(int) (SFUNC = int4pl, STYPE = int, FINALFUNC = "pg_reload_conf")`,
			want:      `cluster control function "pg_reload_conf" named by FINALFUNC`,
		},
		{
			name:      "the moving state function of an aggregate",
			statement: `CREATE AGGREGATE app.notify_all(text, text) (SFUNC = app.keep, STYPE = text, MSFUNC = pg_notify, MSTYPE = text, MINVFUNC = app.keep)`,
			want:      `cluster control function "pg_notify" named by MSFUNC`,
		},
		{
			name:      "the difference function of a range type",
			statement: `CREATE TYPE app.span AS RANGE (SUBTYPE = int4, SUBTYPE_DIFF = pg_terminate_backend)`,
			want:      `cluster control function "pg_terminate_backend" named by SUBTYPE_DIFF`,
		},
		{
			name:      "the support function of an operator class",
			statement: `CREATE OPERATOR CLASS app.int_ops FOR TYPE int USING btree AS OPERATOR 1 <, FUNCTION 1 pg_terminate_backend`,
			want:      `cluster control function "pg_terminate_backend" named by FUNCTION`,
		},
		{
			name:      "a replication function",
			statement: `CREATE OPERATOR app.%%% (FUNCTION = pg_replication_origin_create, RIGHTARG = text)`,
			want:      `replication state operation "pg_replication_origin_create" named by FUNCTION`,
		},
		{
			name:      "a large object function",
			statement: `CREATE OPERATOR app.~~~ (FUNCTION = lo_unlink, RIGHTARG = oid)`,
			want:      `large object operation "lo_unlink" named by FUNCTION`,
		},
		{
			name:      "a session advisory lock",
			statement: `CREATE OPERATOR app.@@@ (FUNCTION = pg_advisory_lock, RIGHTARG = int8)`,
			want:      `session advisory lock "pg_advisory_lock" named by FUNCTION`,
		},
		{
			name:      "a dblink function",
			statement: `CREATE OPERATOR app.&&& (FUNCTION = dblink_exec, LEFTARG = text, RIGHTARG = text)`,
			want:      `external dblink operation "dblink_exec" named by FUNCTION`,
		},
		{
			name:      "an option written as a quoted identifier",
			statement: `CREATE OPERATOR public.!!! ("function" = pg_terminate_backend, leftarg = int, rightarg = int8)`,
			want:      `cluster control function "pg_terminate_backend" named by FUNCTION`,
		},
		{
			name:      "a state function written as a quoted identifier",
			statement: `CREATE AGGREGATE app.set_all(text, text) ("sfunc" = set_config, "stype" = text)`,
			want:      `cluster control function "set_config" named by SFUNC`,
		},
		{
			name:      "a state function written as a string",
			statement: `CREATE AGGREGATE app.set_all(text, text) (SFUNC = 'set_config', STYPE = text)`,
			want:      `cluster control function "set_config" named by SFUNC`,
		},
		{
			name:      "the old spelling of a state function",
			statement: `CREATE AGGREGATE app.set_all (BASETYPE = text, SFUNC1 = set_config, STYPE1 = text, INITCOND1 = '')`,
			want:      `cluster control function "set_config" named by SFUNC1`,
		},
		{
			name:      "an aggregate in the old syntax",
			statement: `CREATE AGGREGATE app.reload (BASETYPE = int, SFUNC = pg_reload_conf, STYPE = int)`,
			want:      `cluster control function "pg_reload_conf" named by SFUNC`,
		},
		{
			name:      "the combine function of an aggregate",
			statement: `CREATE AGGREGATE app.cancel_all(int) (SFUNC = int4pl, STYPE = int, COMBINEFUNC = pg_cancel_backend)`,
			want:      `cluster control function "pg_cancel_backend" named by COMBINEFUNC`,
		},
		{
			name:      "the join estimator of an operator",
			statement: `CREATE OPERATOR app.=== (FUNCTION = app.equals, LEFTARG = int, RIGHTARG = int, JOIN = pg_reload_conf)`,
			want:      `cluster control function "pg_reload_conf" named by JOIN`,
		},
		{
			name:      "an option the server does not list",
			statement: `CREATE OPERATOR app.=== (FUNCTION = app.equals, LEFTARG = int, RIGHTARG = int, SUPPORT = pg_reload_conf)`,
			want:      `cluster control function "pg_reload_conf" named by SUPPORT`,
		},
		{
			name:      "the input function of a base type",
			statement: `CREATE TYPE app.t (INPUT = pg_terminate_backend, OUTPUT = textout)`,
			want:      `cluster control function "pg_terminate_backend" named by INPUT`,
		},
		{
			name:      "the receive function of an altered type",
			statement: `ALTER TYPE app.t SET (RECEIVE = pg_reload_conf)`,
			want:      `cluster control function "pg_reload_conf" named by RECEIVE`,
		},
		{
			name:      "the start function of a text search parser",
			statement: `CREATE TEXT SEARCH PARSER app.p (START = pg_reload_conf, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype)`,
			want:      `cluster control function "pg_reload_conf" named by START`,
		},
		{
			name:      "the function of a cast named without its argument types",
			statement: `CREATE CAST (int AS app.t) WITH FUNCTION pg_terminate_backend`,
			want:      `cluster control function "pg_terminate_backend" named by WITH FUNCTION`,
		},
		{
			name:      "the handler of a foreign-data wrapper",
			statement: `CREATE FOREIGN DATA WRAPPER app_fdw HANDLER pg_reload_conf`,
			want:      `cluster control function "pg_reload_conf" named by HANDLER`,
		},
		{
			name:      "a server file function",
			statement: `CREATE OPERATOR app.~~ (FUNCTION = pg_read_file, RIGHTARG = text)`,
			want:      `server file read "pg_read_file" named by FUNCTION`,
		},
	}
	for _, dialect := range []string{"postgres", "yugabytedb"} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				c.Assert(sessionBoundGuard(c, dialect).Validate(test.statement, databaseScope), qt.ErrorMatches,
					dialect+` migration replay rejects `+test.want+refusalEnd)
			})
		}
	}
}

// A function an object names that the guard lets a statement call is
// accepted, and a dblink function is one under trust, as a dblink call is.
func TestGuard_ReferencedFunction_HappyPath(t *testing.T) {
	tests := []struct {
		name, statement string
		scope           replay.Scope
	}{
		{"an operator over a user's function", `CREATE OPERATOR app.=== (FUNCTION = app.equals, LEFTARG = int, RIGHTARG = int)`, databaseScope},
		{"an operator with estimators", `CREATE OPERATOR app.=== (FUNCTION = app.equals, LEFTARG = int, RIGHTARG = int, RESTRICT = eqsel, JOIN = eqjoinsel)`, databaseScope},
		{"an aggregate over builtin functions", `CREATE AGGREGATE app.total(int) (SFUNC = int4pl, STYPE = int, COMBINEFUNC = int4pl, INITCOND = '0')`, databaseScope},
		{"a range type", `CREATE TYPE app.span AS RANGE (SUBTYPE = int4, SUBTYPE_DIFF = app.int_diff)`, databaseScope},
		{"an operator class", `CREATE OPERATOR CLASS app.int_ops FOR TYPE int USING btree AS OPERATOR 1 <, FUNCTION 1 btint4cmp(int, int)`, databaseScope},
		{"an option written as a quoted identifier", `CREATE OPERATOR app.=== ("function" = app.equals, "leftarg" = int, "rightarg" = int)`, databaseScope},
		{"a state function written as a string", `CREATE AGGREGATE app.total(int) (SFUNC = 'int4pl', STYPE = int)`, databaseScope},
		{"a composite type with an attribute named like a control function", `CREATE TYPE app.pair AS (set_config text, b int)`, databaseScope},
		{"an enum with a label named like a control function", `CREATE TYPE app.mood AS ENUM ('pg_reload_conf', 'happy')`, databaseScope},
		{"a base type over a user's functions", `CREATE TYPE app.t (INPUT = app.t_in, OUTPUT = app.t_out, RECEIVE = app.t_recv, SEND = app.t_send)`, databaseScope},
		{"a server file function under trust", `CREATE OPERATOR app.~~ (FUNCTION = pg_read_file, RIGHTARG = text)`, replay.Scope{Database: "ptah_dev", TrustedSQL: true}},
		{"a dblink function under trust", `CREATE OPERATOR app.&&& (FUNCTION = dblink_exec, LEFTARG = text, RIGHTARG = text)`, replay.Scope{Database: "ptah_dev", TrustedSQL: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sessionBoundGuard(c, "postgres").Validate(test.statement, test.scope), qt.IsNil)
		})
	}
}
