package lint_test

import (
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
)

// tables returns one CREATE TABLE statement per table t<from> to t<to>.
func tables(from, to int) string {
	var b strings.Builder
	for i := from; i <= to; i++ {
		fmt.Fprintf(&b, "CREATE TABLE t%d (id int);\n", i)
	}
	return b.String()
}

// createAndDrop is a table a file creates and drops again.
const createAndDrop = "CREATE TABLE tmp (id int);\nDROP TABLE tmp;\n"

// changeCountOf analyzes a directory under opts and returns how many schema
// changes the named file counts.
func changeCountOf(c *qt.C, files map[string]string, opts lint.Options, name string) int {
	c.Helper()
	analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(files), opts)
	c.Assert(err, qt.IsNil)
	return len(fileByName(c, analysis, name).Changes)
}

// Every expected count in this file is the one the pinned community binary
// v1.3.0 reports for the same directory on SQLite, PostgreSQL or MySQL, under
// `migrate lint --dev-url ... --latest 1` against an empty dev database, except
// where a row says the count is Ptah's own.

// A directory's first file holding more than ten statements counts one change
// per object it leaves behind under the Atlas counts, and one per change each
// statement makes under the statement count and in any later file.
func TestAnalyzeFS_ChangeCountOfALargeOpeningFile(t *testing.T) {
	const opening = "0000000001_init.up.sql"
	tests := []struct {
		name    string
		dialect string
		count   lint.ChangeCount
		files   map[string]string
		file    string
		want    int
	}{
		{
			name: "twelve statements, a table created and dropped", dialect: "sqlite", count: lint.ChangeCountAtlas,
			files: map[string]string{opening: tables(1, 10) + createAndDrop}, file: opening, want: 10,
		},
		{
			name: "the same file under the community count", dialect: "sqlite", count: lint.ChangeCountAtlasCommunity,
			files: map[string]string{opening: tables(1, 10) + createAndDrop}, file: opening, want: 10,
		},
		{
			name: "the same file under the statement count, Ptah's own", dialect: "sqlite", count: lint.ChangeCountStatements,
			files: map[string]string{opening: tables(1, 10) + createAndDrop}, file: opening, want: 12,
		},
		{
			name: "eleven statements", dialect: "sqlite", count: lint.ChangeCountAtlas,
			files: map[string]string{opening: tables(1, 9) + createAndDrop}, file: opening, want: 9,
		},
		{
			name: "ten statements are counted one change per statement", dialect: "sqlite", count: lint.ChangeCountAtlas,
			files: map[string]string{opening: tables(1, 8) + createAndDrop}, file: opening, want: 10,
		},
		{
			name: "an INSERT is one of the eleven", dialect: "sqlite", count: lint.ChangeCountAtlas,
			files: map[string]string{opening: tables(1, 8) + createAndDrop + "INSERT INTO t1 (id) VALUES (1);\n"},
			file:  opening, want: 8,
		},
		{
			name: "eleven tables", dialect: "sqlite", count: lint.ChangeCountAtlas,
			files: map[string]string{opening: tables(1, 11)}, file: opening, want: 11,
		},
		{
			name: "an index is counted with its table", dialect: "sqlite", count: lint.ChangeCountAtlas,
			files: map[string]string{opening: tables(1, 10) + "CREATE INDEX t1_id ON t1 (id);\n"}, file: opening, want: 10,
		},
		{
			name: "an added column is counted with its table", dialect: "sqlite", count: lint.ChangeCountAtlas,
			files: map[string]string{opening: tables(1, 10) + "ALTER TABLE t1 ADD COLUMN x int;\n"}, file: opening, want: 10,
		},
		{
			name: "a renamed table is one table", dialect: "sqlite", count: lint.ChangeCountAtlas,
			files: map[string]string{opening: tables(1, 10) + "ALTER TABLE t1 RENAME TO r1;\n"}, file: opening, want: 10,
		},
		{
			name: "an IF EXISTS drop of a table nothing created", dialect: "sqlite", count: lint.ChangeCountAtlas,
			files: map[string]string{opening: tables(1, 10) + "DROP TABLE IF EXISTS missing;\n"}, file: opening, want: 10,
		},
		{
			name: "an IF EXISTS drop of an index nothing created", dialect: "sqlite", count: lint.ChangeCountAtlas,
			files: map[string]string{opening: tables(1, 10) + "DROP INDEX IF EXISTS nope;\n"}, file: opening, want: 10,
		},
		{
			name: "a view created and dropped", dialect: "sqlite", count: lint.ChangeCountAtlas,
			files: map[string]string{opening: tables(1, 9) + "CREATE VIEW v AS SELECT id FROM t1;\nDROP VIEW v;\n"},
			file:  opening, want: 9,
		},
		{
			name: "a view under the community count", dialect: "sqlite", count: lint.ChangeCountAtlasCommunity,
			files: map[string]string{opening: tables(1, 10) + "CREATE VIEW v AS SELECT id FROM t1;\n"}, file: opening, want: 10,
		},
		{
			name: "a view under the Atlas count, Ptah's own", dialect: "sqlite", count: lint.ChangeCountAtlas,
			files: map[string]string{opening: tables(1, 10) + "CREATE VIEW v AS SELECT id FROM t1;\n"}, file: opening, want: 11,
		},
		{
			name: "a trigger under the community count", dialect: "sqlite", count: lint.ChangeCountAtlasCommunity,
			files: map[string]string{opening: tables(1, 10) +
				"CREATE TRIGGER t1_ai AFTER INSERT ON t1 BEGIN UPDATE t1 SET id = id WHERE id = NEW.id; END;\n"},
			file: opening, want: 10,
		},
		{
			name: "an enum type under the community count", dialect: "postgres", count: lint.ChangeCountAtlasCommunity,
			files: map[string]string{opening: tables(1, 10) + "CREATE TYPE mood AS ENUM ('a', 'b');\n"}, file: opening, want: 11,
		},
		{
			name: "a sequence under the community count", dialect: "postgres", count: lint.ChangeCountAtlasCommunity,
			files: map[string]string{opening: tables(1, 10) + "CREATE SEQUENCE s;\n"}, file: opening, want: 10,
		},
		{
			name: "a large second file", dialect: "sqlite", count: lint.ChangeCountAtlas,
			files: map[string]string{
				"0000000001_base.up.sql":  "CREATE TABLE base (id int);\n",
				"0000000002_large.up.sql": tables(1, 10) + createAndDrop,
			},
			file: "0000000002_large.up.sql", want: 12,
		},
		{
			name: "a large second file after a file that changes no schema", dialect: "sqlite", count: lint.ChangeCountAtlas,
			files: map[string]string{
				"0000000001_base.up.sql":  "SELECT 1;\n",
				"0000000002_large.up.sql": tables(1, 10) + createAndDrop,
			},
			file: "0000000002_large.up.sql", want: 12,
		},
		{
			name: "a large first file when a later one follows", dialect: "sqlite", count: lint.ChangeCountAtlas,
			files: map[string]string{
				opening:                   tables(1, 10) + createAndDrop,
				"0000000002_later.up.sql": "CREATE TABLE later (id int);\n",
			},
			file: opening, want: 10,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := changeCountOf(c, test.files, lint.Options{Dialect: test.dialect, ChangeCount: test.count}, test.file)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// The community count leaves out a change to a kind of object the community
// binary does not read off the dev database. The Atlas count keeps every kind
// Ptah models, so it counts each row once.
func TestAnalyzeFS_ChangeCountOfAKindOfObject(t *testing.T) {
	const second = "0000000002_head.up.sql"
	tests := []struct {
		name      string
		dialect   string
		statement string
		community int
		atlas     int
	}{
		{
			name: "SQLite trigger", dialect: "sqlite",
			statement: "CREATE TRIGGER users_ai AFTER INSERT ON users BEGIN UPDATE users SET name = name WHERE id = NEW.id; END;",
			community: 0, atlas: 1,
		},
		{name: "SQLite view", dialect: "sqlite", statement: "CREATE VIEW v AS SELECT id FROM users;", community: 0, atlas: 1},
		{name: "SQLite view drop", dialect: "sqlite", statement: "DROP VIEW v;", community: 0, atlas: 1},
		{name: "PostgreSQL view", dialect: "postgres", statement: "CREATE VIEW v AS SELECT id FROM users;", community: 0, atlas: 1},
		{
			name: "PostgreSQL materialized view", dialect: "postgres",
			statement: "CREATE MATERIALIZED VIEW mv AS SELECT id FROM users;", community: 0, atlas: 1,
		},
		{
			name: "PostgreSQL function", dialect: "postgres",
			statement: "CREATE FUNCTION f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$;",
			community: 0, atlas: 1,
		},
		{
			name: "PostgreSQL procedure", dialect: "postgres",
			statement: "CREATE PROCEDURE pr() LANGUAGE sql AS $$ SELECT 1 $$;", community: 0, atlas: 1,
		},
		{
			name: "PostgreSQL trigger", dialect: "postgres",
			statement: "CREATE TRIGGER tr BEFORE INSERT ON users FOR EACH ROW EXECUTE FUNCTION f();", community: 0, atlas: 1,
		},
		{name: "PostgreSQL sequence", dialect: "postgres", statement: "CREATE SEQUENCE s;", community: 0, atlas: 1},
		{name: "PostgreSQL policy", dialect: "postgres", statement: "CREATE POLICY p ON users USING (true);", community: 0, atlas: 1},
		{name: "PostgreSQL extension", dialect: "postgres", statement: "CREATE EXTENSION IF NOT EXISTS pgcrypto;", community: 0, atlas: 1},
		{name: "PostgreSQL composite type", dialect: "postgres", statement: "CREATE TYPE comp AS (a int);", community: 0, atlas: 1},
		{name: "PostgreSQL domain", dialect: "postgres", statement: "CREATE DOMAIN d AS int;", community: 0, atlas: 1},
		{
			name: "PostgreSQL row-level security", dialect: "postgres",
			statement: "ALTER TABLE users ENABLE ROW LEVEL SECURITY;", community: 0, atlas: 1,
		},
		{name: "PostgreSQL GRANT", dialect: "postgres", statement: "GRANT SELECT ON users TO PUBLIC;", community: 0, atlas: 1},
		{name: "PostgreSQL REVOKE", dialect: "postgres", statement: "REVOKE SELECT ON users FROM PUBLIC;", community: 0, atlas: 1},
		{name: "PostgreSQL comment on a view", dialect: "postgres", statement: "COMMENT ON VIEW v IS 'x';", community: 0, atlas: 1},
		{name: "PostgreSQL comment on a function", dialect: "postgres", statement: "COMMENT ON FUNCTION g() IS 'x';", community: 0, atlas: 1},
		{name: "PostgreSQL view drop", dialect: "postgres", statement: "DROP VIEW v;", community: 0, atlas: 1},
		{name: "MySQL view", dialect: "mysql", statement: "CREATE VIEW v AS SELECT id FROM users;", community: 0, atlas: 1},
		{
			name: "MySQL trigger", dialect: "mysql",
			statement: "CREATE TRIGGER users_bi BEFORE INSERT ON users FOR EACH ROW SET NEW.name = NEW.name;", community: 0, atlas: 1,
		},
		{name: "MySQL procedure", dialect: "mysql", statement: "CREATE PROCEDURE pr() SELECT 1;", community: 0, atlas: 1},
		{name: "PostgreSQL enum type", dialect: "postgres", statement: "CREATE TYPE mood AS ENUM ('a', 'b');", community: 1, atlas: 1},
		{name: "PostgreSQL comment on a table", dialect: "postgres", statement: "COMMENT ON TABLE users IS 'x';", community: 1, atlas: 1},
		{name: "PostgreSQL comment on a column", dialect: "postgres", statement: "COMMENT ON COLUMN users.name IS 'x';", community: 1, atlas: 1},
		{name: "PostgreSQL comment on an index", dialect: "postgres", statement: "COMMENT ON INDEX users_name_idx IS 'x';", community: 1, atlas: 1},
		{
			name: "PostgreSQL exclusion constraint", dialect: "postgres",
			statement: "ALTER TABLE users ADD CONSTRAINT ex EXCLUDE USING btree (id WITH =);", community: 1, atlas: 1,
		},
		{name: "SQLite index", dialect: "sqlite", statement: "CREATE INDEX users_name_idx ON users (name);", community: 1, atlas: 1},
		{name: "SQLite table", dialect: "sqlite", statement: "CREATE TABLE t (id int);", community: 1, atlas: 1},
		{name: "SQLite column", dialect: "sqlite", statement: "ALTER TABLE users ADD COLUMN age int;", community: 1, atlas: 1},
		{name: "MySQL index", dialect: "mysql", statement: "CREATE INDEX users_name_idx ON users (name);", community: 1, atlas: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			files := map[string]string{
				"0000000001_base.up.sql": "CREATE TABLE users (id int, name varchar(20));\n",
				second:                   test.statement + "\n",
			}
			community := changeCountOf(c, files, lint.Options{Dialect: test.dialect, ChangeCount: lint.ChangeCountAtlasCommunity}, second)
			atlas := changeCountOf(c, files, lint.Options{Dialect: test.dialect, ChangeCount: lint.ChangeCountAtlas}, second)
			c.Assert(community, qt.Equals, test.community)
			c.Assert(atlas, qt.Equals, test.atlas)
		})
	}
}

// usersIndexAt is the index users holds before version 2.
func usersIndexAt() []lintext.BaselineIndex {
	return []lintext.BaselineIndex{{Version: 2, Table: "users", Name: "users_name_idx"}}
}

// An IF EXISTS or IF NOT EXISTS statement on an index or a column that the
// starting state shows the engine skips counts no change, under every count.
// Where the state is missing or cannot say, the change is counted.
func TestIfExistsIndexOrColumnStatement_Counts(t *testing.T) {
	const second = "0000000002_head.up.sql"
	tests := []struct {
		name      string
		dialect   string
		statement string
		indexes   []lintext.BaselineIndex
		columns   []lintext.BaselineColumn
		// scope is the schema the run reviews, "" for every schema.
		scope string
		want  []changeProjection
	}{
		{
			name: "drop of an absent index", dialect: "sqlite", statement: "DROP INDEX IF EXISTS users_name_idx;",
			columns: usersAt(""), want: make([]changeProjection, 0),
		},
		{
			name: "drop of a present index", dialect: "sqlite", statement: "DROP INDEX IF EXISTS users_name_idx;",
			columns: usersAt(""), indexes: usersIndexAt(),
			want: []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "users_name_idx"}},
		},
		{
			name: "drop of an index without a starting state", dialect: "sqlite", statement: "DROP INDEX IF EXISTS users_name_idx;",
			want: []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "users_name_idx"}},
		},
		{
			name: "drop without IF EXISTS", dialect: "sqlite", statement: "DROP INDEX users_name_idx;",
			columns: usersAt(""), want: []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "users_name_idx"}},
		},
		{
			name: "drop qualified against a state that names no schema", dialect: "sqlite",
			statement: "DROP INDEX IF EXISTS main.users_name_idx;", columns: usersAt(""),
			want: []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "main.users_name_idx"}},
		},
		{
			name: "drop of an index the file created", dialect: "sqlite",
			statement: "CREATE INDEX users_name_idx ON users (name);\nDROP INDEX IF EXISTS users_name_idx;",
			columns:   usersAt(""),
			want: []changeProjection{
				{Kind: lint.SchemaChangeAdd, Object: "users_name_idx"},
				{Kind: lint.SchemaChangeDrop, Object: "users_name_idx"},
			},
		},
		{
			name: "two absent index drops and a table", dialect: "sqlite",
			statement: "DROP INDEX IF EXISTS users_name_idx;\nDROP INDEX IF EXISTS other_idx;\nCREATE TABLE t (id int);",
			columns:   usersAt(""), want: []changeProjection{{Kind: lint.SchemaChangeAdd, Object: "t"}},
		},
		{
			name: "creation of a present index", dialect: "sqlite",
			statement: "CREATE INDEX IF NOT EXISTS users_name_idx ON users (name);",
			columns:   usersAt(""), indexes: usersIndexAt(), want: make([]changeProjection, 0),
		},
		{
			name: "creation of an absent index", dialect: "sqlite",
			statement: "CREATE INDEX IF NOT EXISTS users_name_idx ON users (name);",
			columns:   usersAt(""), want: []changeProjection{{Kind: lint.SchemaChangeAdd, Object: "users_name_idx"}},
		},
		{
			name: "creation of a present index without IF NOT EXISTS", dialect: "sqlite",
			statement: "CREATE INDEX users_name_idx ON users (name);",
			columns:   usersAt(""), indexes: usersIndexAt(),
			want: []changeProjection{{Kind: lint.SchemaChangeAdd, Object: "users_name_idx"}},
		},
		{
			name: "addition of a present column", dialect: "postgres",
			statement: "ALTER TABLE users ADD COLUMN IF NOT EXISTS id int;", columns: usersAt("public"),
			want: make([]changeProjection, 0),
		},
		{
			name: "addition of an absent column", dialect: "postgres",
			statement: "ALTER TABLE users ADD COLUMN IF NOT EXISTS name text;", columns: usersAt("public"),
			want: []changeProjection{{Kind: lint.SchemaChangeAdd, Object: "name"}},
		},
		{
			name: "drop of an absent column", dialect: "postgres",
			statement: "ALTER TABLE users DROP COLUMN IF EXISTS nope;", columns: usersAt("public"),
			want: make([]changeProjection, 0),
		},
		{
			name: "drop of a present column", dialect: "postgres",
			statement: "ALTER TABLE users DROP COLUMN IF EXISTS id;", columns: usersAt("public"),
			want: []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "id"}},
		},
		{
			name: "drop of a column of a table the state does not hold", dialect: "postgres",
			statement: "ALTER TABLE other DROP COLUMN IF EXISTS nope;", columns: usersAt("public"),
			want: []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "nope"}},
		},
		{
			name: "drop of a column the file added", dialect: "postgres",
			statement: "ALTER TABLE users ADD COLUMN nope int;\nALTER TABLE users DROP COLUMN IF EXISTS nope;",
			columns:   usersAt("public"),
			want: []changeProjection{
				{Kind: lint.SchemaChangeAdd, Object: "nope"},
				{Kind: lint.SchemaChangeDrop, Object: "nope"},
			},
		},
		{
			name: "drop of a column without a starting state", dialect: "postgres",
			statement: "ALTER TABLE users DROP COLUMN IF EXISTS nope;",
			want:      []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "nope"}},
		},
		{
			name: "creation of a present index on a table the file renamed away and created again", dialect: "postgres",
			statement: "ALTER TABLE users RENAME TO users_old;\nCREATE TABLE users (id int, name text);\n" +
				"CREATE INDEX IF NOT EXISTS users_name_idx ON users (name);",
			columns: usersAt("public"), indexes: usersIndexAt(),
			want: []changeProjection{
				{Kind: lint.SchemaChangeDrop, Object: "users"},
				{Kind: lint.SchemaChangeAdd, Object: "users_old"},
				{Kind: lint.SchemaChangeAdd, Object: "users"},
				{Kind: lint.SchemaChangeAdd, Object: "users_name_idx"},
			},
		},
		{
			name: "creation of a present index on a table the file dropped and created again", dialect: "sqlite",
			statement: "DROP TABLE users;\nCREATE TABLE users (id int, name text);\nCREATE INDEX IF NOT EXISTS users_name_idx ON users (name);",
			columns:   usersAt(""), indexes: usersIndexAt(),
			want: []changeProjection{
				{Kind: lint.SchemaChangeDrop, Object: "users"},
				{Kind: lint.SchemaChangeAdd, Object: "users"},
				{Kind: lint.SchemaChangeAdd, Object: "users_name_idx"},
			},
		},
		{
			name: "creation of a present table after its schema was dropped", dialect: "postgres",
			statement: "DROP SCHEMA app CASCADE;\nCREATE SCHEMA app;\nCREATE TABLE IF NOT EXISTS app.users (id int);",
			columns:   usersAt("app"),
			want: []changeProjection{
				{Kind: lint.SchemaChangeAdd, Object: "app"},
				{Kind: lint.SchemaChangeAdd, Object: "app.users"},
			},
		},
		{
			name: "addition of a present column after a cascade that may have dropped it", dialect: "postgres",
			statement: "DROP TYPE mood CASCADE;\nCREATE TYPE mood AS ENUM ('a');\nALTER TABLE users ADD COLUMN IF NOT EXISTS m mood;",
			columns:   append(usersAt("public"), lintext.BaselineColumn{Version: 2, Schema: "public", Table: "users", Name: "m"}),
			want: []changeProjection{
				{Kind: lint.SchemaChangeDrop, Object: "mood"},
				{Kind: lint.SchemaChangeAdd, Object: "mood"},
				{Kind: lint.SchemaChangeAdd, Object: "m"},
			},
		},
		{
			name: "drop of an index the named table does not hold", dialect: "mariadb",
			statement: "DROP INDEX IF EXISTS users_name_idx ON other;",
			columns:   append(usersAt(""), lintext.BaselineColumn{Version: 2, Table: "other", Name: "id"}), indexes: usersIndexAt(),
			want: make([]changeProjection, 0),
		},
		{
			name: "drop of an index the named table holds", dialect: "mariadb",
			statement: "DROP INDEX IF EXISTS users_name_idx ON users;",
			columns:   append(usersAt(""), lintext.BaselineColumn{Version: 2, Table: "other", Name: "id"}), indexes: usersIndexAt(),
			want: []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "users_name_idx"}},
		},
		{
			name: "drop of an index on a table the state does not hold", dialect: "mariadb",
			statement: "DROP INDEX IF EXISTS users_name_idx ON gone;",
			columns:   usersAt(""), indexes: usersIndexAt(),
			want: []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "users_name_idx"}},
		},
		{
			name: "drop qualified by the reviewed schema", dialect: "postgres",
			statement: "DROP INDEX IF EXISTS public.users_name_idx;", columns: usersAt(""), scope: "public",
			want: make([]changeProjection, 0),
		},
		{
			name: "creation of a present table qualified by the reviewed schema", dialect: "postgres",
			statement: "CREATE TABLE IF NOT EXISTS public.users (id int);", columns: usersAt(""), scope: "public",
			want: make([]changeProjection, 0),
		},
		{
			name: "creation of a table in another database than the state was read from", dialect: "mysql",
			statement: "CREATE TABLE IF NOT EXISTS otherdb.users (id int);", columns: usersAt(""),
			want: []changeProjection{{Kind: lint.SchemaChangeAdd, Object: "otherdb.users"}},
		},
		{
			name: "addition of a column to a table in another database", dialect: "mariadb",
			statement: "ALTER TABLE otherdb.users ADD COLUMN IF NOT EXISTS id int;", columns: usersAt(""),
			want: []changeProjection{{Kind: lint.SchemaChangeAdd, Object: "id"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
				"0000000001_users.up.sql": "CREATE TABLE users (id int);\n",
				second:                    test.statement + "\n",
			}), lint.Options{Dialect: test.dialect, Baseline: test.columns, BaselineIndexes: test.indexes, SchemaScope: test.scope})
			c.Assert(err, qt.IsNil)
			c.Assert(projectChanges(fileByName(c, analysis, second).Changes), qt.DeepEquals, test.want)
		})
	}
}

// firstFileDrop is a first file whose IF EXISTS drop finds nothing on an empty
// dev database.
var firstFileDrop = map[string]string{
	"0000000001_init.up.sql": "DROP TABLE IF EXISTS missing;\nCREATE TABLE users (id int);\n",
}

// A version the caller read and found empty starts from an empty schema, so
// an IF EXISTS drop in it drops nothing: no change is counted, no drop is
// reported, and no rule is named as missing its input.
func TestAnalyzeFS_ReadEmptyStartingState(t *testing.T) {
	c := qt.New(t)
	analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(firstFileDrop), lint.Options{
		Dialect:      "sqlite",
		ChangeCount:  lint.ChangeCountAtlas,
		BaselineRead: []int64{1},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(projectChanges(fileByName(c, analysis, "0000000001_init.up.sql").Changes), qt.DeepEquals, []changeProjection{
		{Kind: lint.SchemaChangeAdd, Object: "users"},
	})
	c.Assert(dsSites(findingSites(analysis.Findings())), qt.HasLen, 0)
	c.Assert(analysis.UnmetInputs(), qt.HasLen, 0)
}

// A qualified drop against a state read empty is decided only where the
// qualifier names the schema the run reviews, the schema the state was read
// from.
func TestAnalyzeFS_ReadEmptyStartingStateOfAQualifiedDrop(t *testing.T) {
	tests := []struct {
		name  string
		scope string
		want  []changeProjection
	}{
		{
			name: "the reviewed schema", scope: "public",
			want: []changeProjection{{Kind: lint.SchemaChangeAdd, Object: "users"}},
		},
		{
			name: "no reviewed schema", scope: "",
			want: []changeProjection{
				{Kind: lint.SchemaChangeDrop, Object: "public.missing"},
				{Kind: lint.SchemaChangeAdd, Object: "users"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
				"0000000001_init.up.sql": "DROP TABLE IF EXISTS public.missing;\nCREATE TABLE users (id int);\n",
			}), lint.Options{
				Dialect:      "postgres",
				ChangeCount:  lint.ChangeCountAtlas,
				SchemaScope:  test.scope,
				BaselineRead: []int64{1},
			})
			c.Assert(err, qt.IsNil)
			c.Assert(projectChanges(fileByName(c, analysis, "0000000001_init.up.sql").Changes), qt.DeepEquals, test.want)
		})
	}
}

// The control: the same file without the read reports the drop and names the
// rules that went without the state.
func TestAnalyzeFS_UnreadStartingState(t *testing.T) {
	c := qt.New(t)
	analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(firstFileDrop), lint.Options{
		Dialect:     "sqlite",
		ChangeCount: lint.ChangeCountAtlas,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(projectChanges(fileByName(c, analysis, "0000000001_init.up.sql").Changes), qt.DeepEquals, []changeProjection{
		{Kind: lint.SchemaChangeDrop, Object: "missing"},
		{Kind: lint.SchemaChangeAdd, Object: "users"},
	})
	c.Assert(dsSites(findingSites(analysis.Findings())), qt.DeepEquals, []string{"0000000001_init.up.sql:1:DS101"})
	c.Assert(analysis.UnmetInputs(), qt.Not(qt.HasLen), 0)
	c.Assert(analysis.BaselineVersions(), qt.DeepEquals, []int64{1})
}

// The Atlas counts ask for the starting state of a version holding a
// statement whose change only that state decides, where no rule does. The
// statement count does not, and neither does a large first file, which counts
// what it leaves behind whatever the state holds.
func TestAnalysis_BaselineVersionsOfTheChangeCount(t *testing.T) {
	tests := []struct {
		name  string
		count lint.ChangeCount
		files map[string]string
		want  []int64
	}{
		{
			name: "a CREATE TABLE IF NOT EXISTS under the Atlas count", count: lint.ChangeCountAtlas,
			files: map[string]string{
				"0000000001_users.up.sql": "CREATE TABLE users (id int);\n",
				"0000000002_head.up.sql":  "CREATE TABLE IF NOT EXISTS users (id int);\n",
			},
			want: []int64{2},
		},
		{
			name: "a CREATE INDEX IF NOT EXISTS under the community count", count: lint.ChangeCountAtlasCommunity,
			files: map[string]string{
				"0000000001_users.up.sql": "CREATE TABLE users (id int);\n",
				"0000000002_head.up.sql":  "CREATE INDEX IF NOT EXISTS users_id ON users (id);\n",
			},
			want: []int64{2},
		},
		{
			name: "a CREATE TABLE IF NOT EXISTS under the statement count", count: lint.ChangeCountStatements,
			files: map[string]string{
				"0000000001_users.up.sql": "CREATE TABLE users (id int);\n",
				"0000000002_head.up.sql":  "CREATE TABLE IF NOT EXISTS users (id int);\n",
			},
			want: nil,
		},
		{
			name: "a small first file", count: lint.ChangeCountAtlas,
			files: map[string]string{"0000000001_init.up.sql": "DROP INDEX IF EXISTS nope;\nCREATE TABLE t1 (id int);\n"},
			want:  []int64{1},
		},
		{
			name: "a large first file", count: lint.ChangeCountAtlas,
			files: map[string]string{"0000000001_init.up.sql": "DROP INDEX IF EXISTS nope;\n" + tables(1, 10)},
			want:  nil,
		},
		{
			name: "an index on a table the file created", count: lint.ChangeCountAtlas,
			files: map[string]string{"0000000001_init.up.sql": "CREATE TABLE t1 (id int);\nCREATE INDEX IF NOT EXISTS t1_id ON t1 (id);\n"},
			want:  nil,
		},
		{
			name: "an index the file created", count: lint.ChangeCountAtlas,
			files: map[string]string{
				"0000000001_users.up.sql": "CREATE TABLE users (id int);\n",
				"0000000002_head.up.sql":  "CREATE INDEX users_id ON users (id);\nDROP INDEX IF EXISTS users_id;\n",
			},
			want: nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(test.files), lint.Options{
				Dialect:     "sqlite",
				ChangeCount: test.count,
			})
			c.Assert(err, qt.IsNil)
			c.Assert(analysis.BaselineVersions(), qt.DeepEquals, test.want)
		})
	}
}

func TestAnalyzeFS_ChangeCount_FailurePath(t *testing.T) {
	c := qt.New(t)
	analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
		"0000000001_init.up.sql": "CREATE TABLE t (id int);\n",
	}), lint.Options{ChangeCount: lint.ChangeCount("statements")})
	c.Assert(err, qt.ErrorMatches, `unsupported schema change count "statements"`)
	c.Assert(analysis.Files(), qt.HasLen, 0)
}
