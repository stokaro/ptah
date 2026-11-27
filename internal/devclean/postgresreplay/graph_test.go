package postgresreplay_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devclean/replay"
)

// pathGuard is the guard of dialect bound to a dev session whose catalog
// holds [sessionCatalog], whose database holds the rules rules, and whose
// search path column answers searchPath as the session query writes it.
func pathGuard(c *qt.C, dialect, searchPath, rules string) replay.Guard {
	c.Helper()
	session := reportedSession{
		encoding: "UTF8", conforming: "on", catalog: sessionCatalog, searchPath: searchPath, rules: rules, asked: new(int),
	}
	bound, err := sessionGuardFor(c, dialect).ValidateSession(c.Context(), session, databaseScope)
	c.Assert(err, qt.IsNil)
	return bound
}

// userPath is a session whose search path is the server's default followed
// by app, `"$user", public, app`, for the user ptah, in a database that holds
// public and app.
const userPath = `{"existing": ["public", "app"], "setting": "\"$user\", public, app", "user": "ptah", "schemas": ["public", "app"]}`

// A write is judged when it is written, over the relations as the statements
// before it left them, so an object created before what it reaches is
// refused as one created after it is. Measured on PostgreSQL 18, each setup
// below followed by its write sets rolcreatedb in pg_authid or deletes rows
// of pg_description, and was accepted by a guard that judged a relation when
// it was created: a view replaced under a view over it, a rule whose action
// writes through a view replaced after the rule, and a refint trigger whose
// argument names a view created after the trigger. A write carries on to
// the tables whose foreign key cascades from it and to its partitions, and a
// cycle of cascades ends.
func TestGuard_WriteTimeReach_FailurePath(t *testing.T) {
	harmless := []string{
		`CREATE TABLE app.harmless (rolname name, rolcreatedb boolean)`,
		`CREATE VIEW app.inner_v AS SELECT rolname, rolcreatedb FROM app.harmless`,
	}
	replaced := `CREATE OR REPLACE VIEW app.inner_v AS SELECT rolname, rolcreatedb FROM pg_authid`
	descs := `CREATE VIEW app.descs AS SELECT objoid FROM pg_catalog.pg_description`
	tests := []struct {
		name     string
		setup    []string
		refused  string
		relation string
	}{
		{
			name:     "a view over a view replaced after it",
			setup:    slices.Concat(harmless, []string{`CREATE VIEW app.outer_v AS SELECT * FROM app.inner_v`, replaced}),
			refused:  `UPDATE app.outer_v SET rolcreatedb = true`,
			relation: "outer_v",
		},
		{
			name: "a rule whose action writes through a view replaced after it",
			setup: slices.Concat(harmless, []string{
				`CREATE RULE fan AS ON INSERT TO app.events DO ALSO UPDATE app.inner_v SET rolcreatedb = true`, replaced,
			}),
			refused:  `INSERT INTO app.events VALUES (1)`,
			relation: "events",
		},
		{
			name: "a trigger whose argument names a view created after it",
			setup: []string{
				`CREATE TABLE app.events (id oid)`,
				`CREATE TRIGGER events_fk AFTER DELETE ON app.events FOR EACH ROW EXECUTE FUNCTION check_foreign_key(1, 'cascade', 'id', 'app.descs', 'objoid')`,
				descs,
			},
			refused:  `DELETE FROM app.events`,
			relation: "events",
		},
		{
			name: "a cascade into a table whose trigger names a catalog view",
			setup: []string{
				`CREATE TABLE app.parent (id int PRIMARY KEY)`,
				`CREATE TABLE app.child (pid int REFERENCES app.parent (id) ON DELETE CASCADE)`,
				`CREATE TRIGGER child_fk AFTER DELETE ON app.child FOR EACH ROW EXECUTE FUNCTION check_foreign_key(1, 'cascade', 'pid', 'app.descs', 'objoid')`,
				descs,
			},
			refused:  `DELETE FROM app.parent WHERE id = 1`,
			relation: "parent",
		},
		{
			name: "a cycle of cascades",
			setup: []string{
				`CREATE TABLE app.a (id int PRIMARY KEY, b_id int)`,
				`CREATE TABLE app.b (id int PRIMARY KEY, a_id int, FOREIGN KEY (a_id) REFERENCES app.a ON UPDATE SET NULL)`,
				`ALTER TABLE app.a ADD CONSTRAINT a_b FOREIGN KEY (b_id) REFERENCES app.b ON DELETE CASCADE`,
				`CREATE TRIGGER a_fk AFTER DELETE ON app.a FOR EACH ROW EXECUTE FUNCTION check_foreign_key(1, 'cascade', 'id', 'app.descs', 'objoid')`,
				descs,
			},
			refused:  `DELETE FROM app.b`,
			relation: "b",
		},
		{
			name: "a partition whose trigger names a catalog view",
			setup: []string{
				`CREATE TABLE app.p (id int) PARTITION BY RANGE (id)`,
				`CREATE TABLE app.p1 PARTITION OF app.p FOR VALUES FROM (0) TO (10)`,
				`CREATE TRIGGER p1_fk AFTER DELETE ON app.p1 FOR EACH ROW EXECUTE FUNCTION check_foreign_key(1, 'cascade', 'id', 'app.descs', 'objoid')`,
				descs,
			},
			refused:  `DELETE FROM app.p`,
			relation: "p",
		},
		{
			name: "a view over a view renamed after it",
			setup: []string{
				`CREATE VIEW app.a AS SELECT * FROM pg_authid`,
				`CREATE VIEW app.b AS SELECT * FROM app.a`,
				`ALTER VIEW app.a RENAME TO c`,
			},
			refused:  `UPDATE app.b SET rolcreatedb = true`,
			relation: "b",
		},
		{
			name:     "an insert on conflict through a view",
			setup:    []string{`CREATE VIEW app.v AS SELECT * FROM pg_authid`},
			refused:  `INSERT INTO app.v (rolname) VALUES ('x') ON CONFLICT DO NOTHING`,
			relation: "v",
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
						test.relation+`"`+refusalEnd)
			})
		}
	}
}

// What a write reaches follows the relations as they stand: a relation the
// replay dropped, a rule or a trigger it dropped, and a view it replaced with
// one of a user's table reach nothing more, and a cascade or a cycle of
// cascades that reaches no namespace the server owns is accepted.
func TestGuard_WriteTimeReach_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		statements []string
	}{
		{"a table created under the name of a dropped view", []string{
			`CREATE VIEW app.v AS SELECT * FROM pg_authid`,
			`DROP VIEW app.v`,
			`CREATE TABLE app.v (rolname name)`,
			`INSERT INTO app.v VALUES ('x')`,
		}},
		{"a view replaced with one of a user's table", []string{
			`CREATE VIEW app.v AS SELECT rolname FROM pg_authid`,
			`CREATE OR REPLACE VIEW app.v AS SELECT name AS rolname FROM app.users`,
			`UPDATE app.v SET rolname = 'x'`,
		}},
		{"a dropped trigger", []string{
			`CREATE TABLE app.events (id oid)`,
			`CREATE TRIGGER events_fk AFTER DELETE ON app.events FOR EACH ROW EXECUTE FUNCTION check_foreign_key(1, 'cascade', 'id', 'app.descs', 'objoid')`,
			`DROP TRIGGER events_fk ON app.events`,
			`CREATE VIEW app.descs AS SELECT objoid FROM pg_catalog.pg_description`,
			`DELETE FROM app.events`,
		}},
		{"a dropped rule", []string{
			`CREATE VIEW app.inner_v AS SELECT rolname, rolcreatedb FROM app.harmless`,
			`CREATE RULE fan AS ON INSERT TO app.events DO ALSO UPDATE app.inner_v SET rolcreatedb = true`,
			`DROP RULE fan ON app.events`,
			`CREATE OR REPLACE VIEW app.inner_v AS SELECT rolname, rolcreatedb FROM pg_authid`,
			`INSERT INTO app.events VALUES (1)`,
		}},
		{"a cascade into a table whose trigger names a user's table", []string{
			`CREATE TABLE app.parent (id int PRIMARY KEY)`,
			`CREATE TABLE app.child (pid int REFERENCES app.parent ON DELETE CASCADE)`,
			`CREATE TRIGGER child_fk AFTER DELETE ON app.child FOR EACH ROW EXECUTE FUNCTION check_foreign_key(1, 'cascade', 'pid', 'app.notes', 'event_id')`,
			`DELETE FROM app.parent`,
		}},
		{"a cycle of cascades that reaches nothing", []string{
			`CREATE TABLE app.a (id int PRIMARY KEY, b_id int)`,
			`CREATE TABLE app.b (id int PRIMARY KEY, a_id int REFERENCES app.a ON DELETE CASCADE)`,
			`ALTER TABLE app.a ADD FOREIGN KEY (b_id) REFERENCES app.b ON DELETE CASCADE`,
			`DELETE FROM app.a`,
		}},
		{"a foreign key without a referential action", []string{
			`CREATE TABLE app.parent (id int PRIMARY KEY)`,
			`CREATE TABLE app.child (pid int REFERENCES app.parent ON DELETE RESTRICT)`,
			`CREATE TRIGGER child_fk AFTER DELETE ON app.child FOR EACH ROW EXECUTE FUNCTION check_foreign_key(1, 'cascade', 'pid', 'app.descs', 'objoid')`,
			`CREATE VIEW app.descs AS SELECT objoid FROM pg_catalog.pg_description`,
			`DELETE FROM app.parent`,
		}},
	}
	for _, dialect := range []string{"postgres", "yugabytedb"} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				acceptAll(c, sessionBoundGuard(c, dialect), test.statements)
			})
		}
	}
}

// A view the dev database holds is in the relations a write is judged over,
// so replacing the view under it reaches pg_catalog as a view the replay
// created would.
func TestGuard_WriteTimeReachOfSessionView_FailurePath(t *testing.T) {
	c := qt.New(t)
	rules := `[["app", "outer_v", "CREATE RULE \"_RETURN\" AS\n    ON SELECT TO app.outer_v DO INSTEAD  SELECT inner_v.rolname,\n    inner_v.rolcreatedb\n   FROM app.inner_v;"],` +
		`["app", "inner_v", "CREATE RULE \"_RETURN\" AS\n    ON SELECT TO app.inner_v DO INSTEAD  SELECT harmless.rolname,\n    harmless.rolcreatedb\n   FROM app.harmless;"]]`
	guard := rulesGuard(c, "postgres", rules)
	acceptAll(c, guard, []string{
		`UPDATE app.outer_v SET rolcreatedb = false`,
		`CREATE OR REPLACE VIEW app.inner_v AS SELECT rolname, rolcreatedb FROM pg_authid`,
	})
	c.Assert(guard.Validate(`UPDATE app.outer_v SET rolcreatedb = true`, databaseScope), qt.ErrorMatches,
		`postgres migration replay rejects protected namespace "pg_catalog" mutation through the relation "outer_v"`+refusalEnd)
}

// A relation written without a schema is the one the server finds on the
// search path: pg_catalog first, then each schema of the path, $user read as
// the session's user, and a schema the replay creates on the path counts from
// then on. A view of the same name in a schema the path does not reach is
// another relation.
func TestGuard_UnqualifiedWriteTarget_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		statements []string
	}{
		{"a table beside a catalog view of its name in another schema", []string{
			`CREATE SCHEMA admin`,
			`CREATE VIEW admin.users AS SELECT rolname FROM pg_catalog.pg_roles`,
			`CREATE TABLE users (id int, name text)`,
			`INSERT INTO users VALUES (1, 'alice')`,
		}},
		{"a qualified table behind a catalog view of the user's schema", []string{
			`CREATE SCHEMA ptah`,
			`CREATE VIEW ptah.users AS SELECT rolname FROM pg_authid`,
			`CREATE TABLE public.users (rolname name)`,
			`INSERT INTO public.users VALUES ('x')`,
		}},
		{"a table later on the path than a dropped catalog view", []string{
			`CREATE VIEW public.users AS SELECT rolname FROM pg_authid`,
			`DROP VIEW public.users`,
			`CREATE TABLE app.users (rolname name)`,
			`INSERT INTO users VALUES ('x')`,
		}},
		{"a table of the user's schema before a catalog view in public", []string{
			`CREATE VIEW public.users AS SELECT rolname FROM pg_authid`,
			`CREATE SCHEMA ptah`,
			`CREATE TABLE ptah.users (rolname name)`,
			`INSERT INTO users VALUES ('x')`,
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			acceptAll(c, pathGuard(c, "postgres", userPath, ""), test.statements)
		})
	}
}

// A schema the replay creates on the search path comes before public when the
// path lists it first, as `"$user"` does for a schema named for the user, so
// a write without a schema reaches the view there, and the refusal says to
// qualify the name.
func TestGuard_UnqualifiedWriteTarget_FailurePath(t *testing.T) {
	c := qt.New(t)
	guard := pathGuard(c, "postgres", userPath, "")
	acceptAll(c, guard, []string{
		`CREATE TABLE public.users (rolname name)`,
		`CREATE SCHEMA ptah`,
		`CREATE VIEW ptah.users AS SELECT rolname FROM pg_authid`,
	})
	c.Assert(guard.Validate(`INSERT INTO users VALUES ('x')`, databaseScope), qt.ErrorMatches,
		`postgres migration replay rejects protected namespace "pg_catalog" mutation through the relation "users"`+refusalEnd+qualify)
}

// A guard that read no session cannot say which relation the server finds for
// a name without a schema, and refuses a write to a name a view of the catalog
// has in any schema, saying to qualify it.
func TestGuard_UnqualifiedWriteTargetWithoutSession_FailurePath(t *testing.T) {
	c := qt.New(t)
	guard := guardFor(c, "postgres")
	acceptAll(c, guard, []string{`CREATE VIEW admin.users AS SELECT rolname FROM pg_authid`})
	c.Assert(guard.Validate(`INSERT INTO users VALUES ('x')`, databaseScope), qt.ErrorMatches,
		`postgres migration replay rejects protected namespace "pg_catalog" mutation through the relation "users"`+refusalEnd+qualify)
}

// A view the server updates automatically writes its one FROM relation, so a
// subquery of the catalog in its WHERE or in its list is only read: a write
// through it lands on the user's table. Measured on PostgreSQL 18, the two
// first views write app.docs and app.tables_meta.
func TestGuard_WriteThroughUpdatableView_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		statements []string
	}{
		{"a filter on a subquery of pg_roles", []string{
			`CREATE VIEW app.member_docs AS SELECT * FROM app.docs WHERE owner IN (SELECT rolname FROM pg_catalog.pg_roles WHERE rolcanlogin)`,
			`INSERT INTO app.member_docs VALUES (1, 'a6739')`,
			`UPDATE app.member_docs SET id = 2`,
		}},
		{"a scalar subquery of pg_class in the list", []string{
			`CREATE VIEW app.meta AS SELECT id, relname, (SELECT count(*) FROM pg_catalog.pg_class c WHERE c.relname = m.relname) AS n FROM app.tables_meta m`,
			`INSERT INTO app.meta (id, relname) VALUES (1, 'pg_class')`,
		}},
		{"an aliased ONLY relation with a check option", []string{
			`CREATE VIEW app.mine AS SELECT d.* FROM ONLY app.docs AS d (id, owner) WHERE d.owner = ANY (SELECT rolname FROM pg_roles) ORDER BY d.id WITH CASCADED CHECK OPTION`,
			`DELETE FROM app.mine WHERE id = 1`,
		}},
		{"a view the dev database holds", []string{
			`UPDATE app.member_docs SET id = 3`,
		}},
	}
	rules := `[["app", "member_docs", "CREATE RULE \"_RETURN\" AS\n    ON SELECT TO app.member_docs DO INSTEAD  SELECT docs.id,\n    docs.owner\n   FROM app.docs\n` +
		`  WHERE (docs.owner IN ( SELECT pg_roles.rolname\n           FROM pg_roles\n          WHERE pg_roles.rolcanlogin));"]]`
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			acceptAll(c, rulesGuard(c, "postgres", rules), test.statements)
		})
	}
}

// A view the guard cannot read as one the server updates automatically is read
// for every relation its query names, and so is a view whose one FROM
// relation is the catalog's.
func TestGuard_WriteThroughUpdatableView_FailurePath(t *testing.T) {
	for _, view := range []string{
		`CREATE VIEW app.v AS SELECT d.id, a.rolname FROM app.docs d JOIN pg_authid a ON a.rolname = d.owner`,
		`CREATE VIEW app.v AS SELECT d.id, a.rolname FROM app.docs d, pg_authid a`,
		`CREATE VIEW app.v AS SELECT * FROM (SELECT rolname, rolcreatedb FROM pg_authid) AS s`,
		`CREATE VIEW app.v AS SELECT * FROM pg_authid WHERE rolname IN (SELECT owner FROM app.docs)`,
		`CREATE VIEW app.v AS SELECT owner AS rolname FROM app.docs UNION ALL SELECT rolname FROM pg_authid`,
		`CREATE VIEW app.v AS SELECT owner AS rolname FROM app.docs WHERE owner IS NOT NULL UNION SELECT rolname FROM pg_authid`,
		`CREATE VIEW app.v AS SELECT DISTINCT owner AS rolname FROM app.docs WHERE owner IN (SELECT rolname FROM pg_authid)`,
		`CREATE VIEW app.v AS WITH r AS (SELECT * FROM pg_authid) SELECT * FROM r`,
		`CREATE VIEW app.v AS SELECT owner AS rolname FROM app.docs GROUP BY owner HAVING owner IN (SELECT rolname FROM pg_authid)`,
		`CREATE RECURSIVE VIEW app.v (rolname) AS SELECT rolname FROM pg_authid UNION SELECT rolname FROM app.v`,
	} {
		t.Run(view, func(t *testing.T) {
			c := qt.New(t)
			guard := sessionBoundGuard(c, "postgres")
			acceptAll(c, guard, []string{view})
			c.Assert(guard.Validate(`UPDATE app.v SET rolname = 'x'`, databaseScope), qt.ErrorMatches,
				`postgres migration replay rejects protected namespace "pg_catalog" mutation through the relation "v"`+refusalEnd)
		})
	}
}
