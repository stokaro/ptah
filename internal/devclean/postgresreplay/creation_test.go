package postgresreplay_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devclean/replay"
)

// createdWithBuiltinNames are CREATE statements that name the object they
// create after an object pg_catalog holds, one for every kind the guard reads
// the created name of. The server puts each in the session's creation schema,
// not in pg_catalog: measured on PostgreSQL 18 with the default search path,
// `CREATE TYPE point AS (x float8)` makes public.point beside
// pg_catalog.point, and `CREATE TABLE pg_class (id int)` makes
// public.pg_class.
//
// A routine definition is refused in the database realm for its body, so the
// routines are read on a server the run owns, where the namespace is the one
// check left.
var createdWithBuiltinNames = []struct {
	name, statement string
	scope           replay.Scope
}{
	{"a table", `CREATE TABLE pg_class (id int)`, databaseScope},
	{"a table if it does not exist", `CREATE TABLE IF NOT EXISTS pg_settings (id int)`, databaseScope},
	{"an unlogged table", `CREATE UNLOGGED TABLE pg_authid (id int)`, databaseScope},
	{"a table from a query", `CREATE TABLE pg_description AS SELECT 1 AS x`, databaseScope},
	{"a table from SELECT INTO", `SELECT 1 AS x INTO pg_description`, databaseScope},
	{"a table from SELECT INTO TABLE", `SELECT 1 AS x INTO TABLE pg_description`, databaseScope},
	{"a view", `CREATE VIEW pg_settings AS SELECT 1 AS x`, databaseScope},
	{"a replaced view", `CREATE OR REPLACE VIEW pg_settings AS SELECT 1 AS x`, databaseScope},
	{"a materialized view", `CREATE MATERIALIZED VIEW pg_database AS SELECT 1 AS x`, databaseScope},
	{"a sequence", `CREATE SEQUENCE pg_class_oid_index`, databaseScope},
	{"a composite type", `CREATE TYPE "point" AS (x float8, y float8)`, databaseScope},
	{"an enum", `CREATE TYPE box AS ENUM ('small', 'large')`, databaseScope},
	{"a domain", `CREATE DOMAIN "money" AS numeric(12, 2)`, databaseScope},
	{"a function", `CREATE FUNCTION now() RETURNS int LANGUAGE sql AS 'SELECT 1'`, serverScope},
	{"a replaced function", `CREATE OR REPLACE FUNCTION now() RETURNS int LANGUAGE sql AS 'SELECT 1'`, serverScope},
	{"a procedure", `CREATE PROCEDURE pg_read_file() LANGUAGE sql AS 'SELECT 1'`, serverScope},
	{"an aggregate", `CREATE AGGREGATE sum(app.amount) (SFUNC = app.add_amount, STYPE = app.amount)`, databaseScope},
	{"a replaced aggregate", `CREATE OR REPLACE AGGREGATE sum(app.amount) (SFUNC = app.add_amount, STYPE = app.amount)`, databaseScope},
	{"an operator", `CREATE OPERATOR + (LEFTARG = app.amount, RIGHTARG = app.amount, FUNCTION = app.add_amount)`, databaseScope},
	{"an operator class", `CREATE OPERATOR CLASS int4_ops FOR TYPE app.amount USING btree AS OPERATOR 1 app.<`, databaseScope},
	{"a collation", `CREATE COLLATION "unicode" (provider = icu, locale = 'und')`, databaseScope},
	{"a conversion", `CREATE CONVERSION utf8_to_iso_8859_1 FOR 'UTF8' TO 'LATIN1' FROM app.convert_latin1`, databaseScope},
	{"an index", `CREATE INDEX pg_class_oid_index ON app.events (id)`, databaseScope},
	{"a unique index", `CREATE UNIQUE INDEX pg_class_oid_index ON app.events (id)`, databaseScope},
	{"statistics", `CREATE STATISTICS pg_class_oid_index ON id, kind FROM app.events`, databaseScope},
}

var (
	databaseScope = replay.Scope{Database: "ptah_dev"}
	serverScope   = replay.Scope{Database: "ptah_dev", Server: true}
)

// A CREATE puts an object whose name has no schema in the session's creation
// schema, so a builtin's name reaches no object of the catalog's. Each kind
// is accepted on every member of the family.
func TestGuard_CreatedBuiltinName_HappyPath(t *testing.T) {
	for _, dialect := range postgresFamily {
		for _, test := range createdWithBuiltinNames {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				c.Assert(sessionBoundGuard(c, dialect).Validate(test.statement, test.scope), qt.IsNil)
			})
		}
	}
}

// On CockroachDB a name pg_extension holds is created in public all the same.
func TestGuard_CreatedExtensionName_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(sessionBoundGuard(c, "cockroachdb").Validate(`CREATE TABLE spatial_ref_sys (id int)`, replay.Scope{Database: "ptah_dev"}), qt.IsNil)
}

// A search path that names no schema creates nothing: the server refuses a
// CREATE whose name has no schema, so the guard has nothing to refuse.
func TestGuard_CreatedNameWithNoCreationSchema_HappyPath(t *testing.T) {
	for _, test := range createdWithBuiltinNames {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(creatingGuard(c, "postgres").Validate(test.statement, test.scope), qt.IsNil)
		})
	}
}

// A guard that read no session knows no creation schema and accepts a
// CREATE whose name has no schema, as it accepts one with any name.
func TestGuard_CreatedNameWithoutSession_HappyPath(t *testing.T) {
	for _, test := range createdWithBuiltinNames {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(guardFor(c, "postgres").Validate(test.statement, test.scope), qt.IsNil)
		})
	}
}

// A session whose search path lists pg_catalog first creates there, so every
// CREATE whose name has no schema is refused, whatever the name, and the
// refusal says to qualify it. A server the run owns refuses it too: the
// protected namespace is the check it keeps.
func TestGuard_CreatedNameInProtectedCreationSchema_FailurePath(t *testing.T) {
	tests := []struct {
		name, statement, unqualified string
	}{
		{"a table", `CREATE TABLE app_events (id int)`, "app_events"},
		{"a table from SELECT INTO", `SELECT 1 AS x INTO app_events`, "app_events"},
		{"a view", `CREATE OR REPLACE VIEW app_view AS SELECT 1 AS x`, "app_view"},
		{"a type", `CREATE TYPE "point" AS (x float8, y float8)`, "point"},
		{"a domain", `CREATE DOMAIN app_money AS numeric(12, 2)`, "app_money"},
		{"a function", `CREATE FUNCTION app_helper() RETURNS int LANGUAGE sql AS 'SELECT 1'`, "app_helper"},
		{"a replaced function", `CREATE OR REPLACE FUNCTION app_helper() RETURNS int LANGUAGE sql AS 'SELECT 1'`, "app_helper"},
		{"an aggregate", `CREATE AGGREGATE app_sum(int) (SFUNC = app.add, STYPE = int)`, "app_sum"},
		{"an operator", `CREATE OPERATOR + (LEFTARG = app.amount, RIGHTARG = app.amount, FUNCTION = app.add_amount)`, "+"},
		{"an operator of several characters", `CREATE OPERATOR ||| (FUNCTION = int4pl, LEFTARG = int, RIGHTARG = int)`, "|||"},
		{"a collation", `CREATE COLLATION app_collation (provider = icu, locale = 'und')`, "app_collation"},
		{"a conversion", `CREATE CONVERSION app_conversion FOR 'UTF8' TO 'LATIN1' FROM app.convert_latin1`, "app_conversion"},
		{"statistics", `CREATE STATISTICS app_stats ON id, kind FROM app.events`, "app_stats"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(creatingGuard(c, "postgres", "pg_catalog").Validate(test.statement, serverScope), qt.ErrorMatches,
				`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "`+
					regexp.QuoteMeta(test.unqualified)+`"`+refusalEnd+qualify)
		})
	}
}

// The server skips a schema of the search path that does not exist, so a path
// that lists a namespace the server owns after the user's schema creates
// there once the replay drops that schema: measured on PostgreSQL 18, with the
// path public, pg_catalog, `DROP SCHEMA public CASCADE` and then `CREATE
// DOMAIN d AS int` makes pg_catalog.d. The guard reads the path once, so any
// namespace the server owns on it refuses every CREATE whose name has no
// schema, before and after the DROP, on every member of the family. A
// routine is refused under trust as well: `CREATE OR REPLACE FUNCTION now()`
// would replace pg_catalog.now.
func TestGuard_CreatedNameOnPathListingProtectedSchema_FailurePath(t *testing.T) {
	tests := []struct {
		name, statement, unqualified string
		scope                        replay.Scope
	}{
		{"a table", `CREATE TABLE app_events (id int)`, "app_events", databaseScope},
		{"a domain", `CREATE DOMAIN d AS int`, "d", databaseScope},
		{"an operator", `CREATE OPERATOR ||| (FUNCTION = int4pl, LEFTARG = int, RIGHTARG = int)`, "|||", databaseScope},
		{"an enum", `CREATE TYPE e AS ENUM ('a')`, "e", databaseScope},
		{"a function under trust", `CREATE OR REPLACE FUNCTION now() RETURNS int LANGUAGE sql AS 'SELECT 1'`, "now",
			replay.Scope{Database: "ptah_dev", TrustedSQL: true}},
	}
	for _, dialect := range postgresFamily {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				guard := creatingGuard(c, dialect, "public", "pg_catalog")
				c.Assert(guard.Validate(test.statement, test.scope), qt.ErrorMatches,
					dialect+` migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "`+
						regexp.QuoteMeta(test.unqualified)+`"`+refusalEnd+qualify)
			})
		}
	}
}

// A path of the user's schemas alone creates in them whichever one the replay
// drops, and a statement that names its schema is created there on any path.
func TestGuard_CreatedNameOnPathListingProtectedSchema_HappyPath(t *testing.T) {
	tests := []struct {
		name, statement string
		path            []string
	}{
		{"a domain on a path of user schemas", `CREATE DOMAIN d AS int`, []string{"public", "app"}},
		{"the drop of the first schema of the path", `DROP SCHEMA public CASCADE`, []string{"public", "pg_catalog"}},
		{"a qualified domain", `CREATE DOMAIN app.d AS int`, []string{"public", "pg_catalog"}},
		{"a qualified operator", `CREATE OPERATOR app.||| (FUNCTION = int4pl, LEFTARG = int, RIGHTARG = int)`, []string{"public", "pg_catalog"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(creatingGuard(c, "postgres", test.path...).Validate(test.statement, databaseScope), qt.IsNil)
		})
	}
}

// The session's own temporary schema on the path is skipped where the guard
// accepts session tables, so the schema after it decides.
func TestGuard_CreatedNameAfterTemporarySchema_FailurePath(t *testing.T) {
	c := qt.New(t)
	c.Assert(creatingGuard(c, "postgres", "pg_temp_3", "public", "pg_catalog").Validate(`CREATE TABLE scratch (id int)`,
		replay.Scope{Database: "ptah_dev", Session: true}), qt.ErrorMatches,
		`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "scratch"`+refusalEnd+qualify)
}

// A name written with the user's schema is created there whatever the path.
// An index is created in its table's schema, and the table after ON is a
// reference the catalog's names decide, not the creation schema.
func TestGuard_CreatedNameInProtectedCreationSchema_HappyPath(t *testing.T) {
	for _, statement := range []string{
		`CREATE TABLE app.events (id int)`,
		`CREATE TYPE app."point" AS (x float8, y float8)`,
		`CREATE INDEX events_id ON app.events (id)`,
		`CREATE INDEX events_id ON events (id)`,
	} {
		t.Run(statement, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(creatingGuard(c, "postgres", "pg_catalog").Validate(statement, replay.Scope{Database: "ptah_dev"}), qt.IsNil)
		})
	}
}

// The session's own temporary schema first on the path is accepted where the
// guard accepts session tables, and refused where it refuses pg_temp.
func TestGuard_CreatedNameInTemporaryCreationSchema_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(creatingGuard(c, "postgres", "pg_temp_3").Validate(`CREATE TABLE scratch (id int)`,
		replay.Scope{Database: "ptah_dev", Session: true}), qt.IsNil)
}

func TestGuard_CreatedNameInTemporaryCreationSchema_FailurePath(t *testing.T) {
	c := qt.New(t)
	c.Assert(creatingGuard(c, "postgres", "pg_temp_3").Validate(`CREATE TABLE scratch (id int)`,
		replay.Scope{Database: "ptah_dev"}), qt.ErrorMatches,
		`postgres migration replay rejects protected namespace "pg_temp_3" mutation through the unqualified name "scratch"`+refusalEnd+qualify)
}

// On CockroachDB a path that lists pg_extension first creates there.
func TestGuard_CreatedNameInExtensionCreationSchema_FailurePath(t *testing.T) {
	c := qt.New(t)
	c.Assert(creatingGuard(c, "cockroachdb", "pg_extension").Validate(`CREATE TABLE app_events (id int)`,
		replay.Scope{Database: "ptah_dev"}), qt.ErrorMatches,
		`cockroachdb migration replay rejects protected namespace "pg_extension" mutation through the unqualified name "app_events"`+refusalEnd+qualify)
}
