package sqlschema_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/sqlext"
	"ptah.run/internal/sqlschema"
)

// readNotices reads sql as PostgreSQL and returns the read's error and the
// names it reported as cut.
func readNotices(c *qt.C, dialect, sql string) ([]sqlschema.StoredName, error) {
	c.Helper()
	var cut []sqlschema.StoredName
	_, _, err := sqlschema.ReadWith(context.Background(), []byte(sql), dialect, nil, sqlschema.Options{
		Owners:      sqlext.None(),
		StoredNames: func(name sqlschema.StoredName) { cut = append(cut, name) },
	})
	return cut, err
}

// TestReadStoredNames_ReportsWhatTheServerCuts holds the boundaries measured
// on PostgreSQL 15.19: 62 and 63 bytes are kept whole, 64 and more are cut to
// 63, and a two-byte character that straddles the limit is dropped whole.
func TestReadStoredNames_ReportsWhatTheServerCuts(t *testing.T) {
	tests := []struct {
		name      string
		index     string
		want      []string
		wantBytes []int
	}{
		{name: "62 bytes", index: `"` + strings.Repeat("a", 62) + `"`},
		{name: "63 bytes", index: `"` + strings.Repeat("a", 63) + `"`},
		{name: "64 bytes", index: `"` + strings.Repeat("a", 64) + `"`, want: []string{strings.Repeat("a", 63)}, wantBytes: []int{64}},
		{name: "far past the limit", index: `"` + strings.Repeat("a", 100) + `"`, want: []string{strings.Repeat("a", 63)}, wantBytes: []int{100}},
		{name: "a character straddling the limit", index: `"` + strings.Repeat("a", 62) + `é"`, want: []string{strings.Repeat("a", 62)}, wantBytes: []int{64}},
		{
			// An unquoted name is folded first, as the server folds it.
			name:      "an unquoted name in capitals",
			index:     strings.Repeat("A", 64),
			want:      []string{strings.Repeat("a", 63)},
			wantBytes: []int{64},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cut, err := readNotices(c, platform.Postgres, "CREATE TABLE t (a int);\nCREATE INDEX "+test.index+" ON t (a);\n")

			c.Assert(err, qt.IsNil)
			var stored []string
			var written []int
			for _, name := range cut {
				stored = append(stored, name.Stored)
				written = append(written, len(name.Written))
			}
			c.Assert(stored, qt.DeepEquals, test.want)
			c.Assert(written, qt.DeepEquals, test.wantBytes)
		})
	}
}

// TestReadStoredNames_ReportsAConstraintsIndexOnce holds that a PRIMARY KEY or
// UNIQUE constraint is one cut name. The server stores the constraint and the
// index it builds under the same 63 bytes, but the statement wrote the name
// once, so a run that counts cut names counts it once.
func TestReadStoredNames_ReportsAConstraintsIndexOnce(t *testing.T) {
	long := strings.Repeat("k", 70)
	tests := []struct {
		name string
		sql  string
		kind string
	}{
		{name: "a unique constraint", sql: `CREATE TABLE t (a int, CONSTRAINT "` + long + `" UNIQUE (a));`, kind: "unique constraint"},
		{name: "a primary key", sql: `CREATE TABLE t (a int, CONSTRAINT "` + long + `" PRIMARY KEY (a));`, kind: "primary key"},
		{name: "a unique constraint added", sql: "CREATE TABLE t (a int);\n" + `ALTER TABLE t ADD CONSTRAINT "` + long + `" UNIQUE (a);`, kind: "unique constraint"},
		{name: "a primary key added", sql: "CREATE TABLE t (a int);\n" + `ALTER TABLE t ADD CONSTRAINT "` + long + `" PRIMARY KEY (a);`, kind: "primary key"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cut, err := readNotices(c, platform.Postgres, test.sql)

			c.Assert(err, qt.IsNil)
			c.Assert(cut, qt.DeepEquals, []sqlschema.StoredName{
				{Kind: test.kind, Scope: `table "public.t"`, Written: long, Stored: long[:63], Limit: 63},
			})
		})
	}
}

// triggerFunction is a trigger function for the trigger rows to execute.
const triggerFunction = "CREATE FUNCTION trg() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;\n"

// TestReadStoredNames_ReportsARoutineOrTriggerCut holds that a function,
// procedure or trigger name past 63 bytes is cut, as PostgreSQL 15.19 cuts it
// with a notice, and is reported in the place it is unique in: a routine's
// schema and input argument types, which the scope reads as the catalog
// spells them, and a trigger's table.
func TestReadStoredNames_ReportsARoutineOrTriggerCut(t *testing.T) {
	long := strings.Repeat("k", 63) + "_alpha"
	tests := []struct {
		name  string
		sql   string
		kind  string
		scope string
	}{
		{
			name:  "a function",
			sql:   `CREATE FUNCTION "` + long + `"(a int, b varchar(5)) RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;`,
			kind:  "function",
			scope: `schema "public" with arguments (integer, character varying)`,
		},
		{
			name:  "a procedure",
			sql:   `CREATE PROCEDURE "` + long + `"() LANGUAGE sql AS $$ SELECT 1 $$;`,
			kind:  "procedure",
			scope: `schema "public" with arguments ()`,
		},
		{
			name:  "a trigger",
			sql:   "CREATE TABLE t (a int);\n" + triggerFunction + `CREATE TRIGGER "` + long + `" BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION trg();`,
			kind:  "trigger",
			scope: `table "public.t"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cut, err := readNotices(c, platform.Postgres, test.sql)

			c.Assert(err, qt.IsNil)
			c.Assert(cut, qt.DeepEquals, []sqlschema.StoredName{
				{Kind: test.kind, Scope: test.scope, Written: long, Stored: long[:63], Limit: 63},
			})
		})
	}
}

// TestReadStoredNames_KeepsLegitimateRepetition pins the namespaces the server
// keeps apart, each measured on PostgreSQL 15.19 to accept the file.
func TestReadStoredNames_KeepsLegitimateRepetition(t *testing.T) {
	long := strings.Repeat("k", 63)
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "foreign keys cut to one name on two tables",
			sql: "CREATE TABLE p (id int PRIMARY KEY);\n" +
				`CREATE TABLE c1 (id int PRIMARY KEY, a int, CONSTRAINT "` + long + `_alpha" FOREIGN KEY (a) REFERENCES p (id));` + "\n" +
				`CREATE TABLE c2 (id int PRIMARY KEY, a int, CONSTRAINT "` + long + `_beta" FOREIGN KEY (a) REFERENCES p (id));`,
		},
		{
			name: "indexes cut to one name in two schemas",
			sql: "CREATE SCHEMA s1;\nCREATE SCHEMA s2;\nCREATE TABLE s1.t (a int);\nCREATE TABLE s2.t (a int);\n" +
				`CREATE INDEX "` + long + `_alpha" ON s1.t (a);` + "\n" + `CREATE INDEX "` + long + `_beta" ON s2.t (a);`,
		},
		{
			name: "columns cut to one name on two tables",
			sql:  `CREATE TABLE t1 ("` + long + `_alpha" int);` + "\n" + `CREATE TABLE t2 ("` + long + `_beta" int);`,
		},
		{
			// The server skips the second statement with a notice.
			name: "an index guarded by IF NOT EXISTS, written the same way twice",
			sql: "CREATE TABLE t (a int);\n" + `CREATE INDEX IF NOT EXISTS "` + long + `_alpha" ON t (a);` + "\n" +
				`CREATE INDEX IF NOT EXISTS "` + long + `_alpha" ON t (a);`,
		},
		{
			// pg_proc keeps an overload with other argument types apart.
			name: "functions cut to one name with other argument types",
			sql: `CREATE FUNCTION "` + long + `_alpha"(int) RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;` + "\n" +
				`CREATE FUNCTION "` + long + `_beta"(text) RETURNS int LANGUAGE sql AS $$ SELECT 2 $$;`,
		},
		{
			name: "triggers cut to one name on two tables",
			sql: "CREATE TABLE t1 (a int);\nCREATE TABLE t2 (a int);\n" + triggerFunction +
				`CREATE TRIGGER "` + long + `_alpha" BEFORE INSERT ON t1 FOR EACH ROW EXECUTE FUNCTION trg();` + "\n" +
				`CREATE TRIGGER "` + long + `_beta" BEFORE INSERT ON t2 FOR EACH ROW EXECUTE FUNCTION trg();`,
		},
		{
			// The second statement names the table by the 63 bytes the server
			// kept, so it is the same table, and the server skips it.
			name: "a table guarded under the stored form of its name",
			sql:  `CREATE TABLE "` + long + `_alpha" (a int);` + "\n" + `CREATE TABLE IF NOT EXISTS "` + long + `" (a int, b int);`,
		},
		{
			name: "an index dropped before the next is created",
			sql: "CREATE TABLE t (a int);\n" + `CREATE INDEX "` + long + `_alpha" ON t (a);` + "\n" +
				`DROP INDEX "` + long + `_alpha";` + "\n" + `CREATE INDEX "` + long + `_beta" ON t (a);`,
		},
		{
			// Quoted and unquoted spellings that fold to different names stay
			// two names.
			name: "a quoted name in capitals beside an unquoted one",
			sql: "CREATE TABLE t (a int);\n" + `CREATE INDEX "` + strings.Repeat("K", 64) + `" ON t (a);` + "\n" +
				`CREATE INDEX "` + long + `_x" ON t (a);`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := readNotices(c, platform.Postgres, test.sql)

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestReadStoredNames_ReportsAGuardedRepetitionOnce holds that a statement the
// server skips declares no name of its own: the cut name is the first
// statement's, reported once.
func TestReadStoredNames_ReportsAGuardedRepetitionOnce(t *testing.T) {
	c := qt.New(t)
	long := strings.Repeat("k", 70)

	cut, err := readNotices(c, platform.Postgres, "CREATE TABLE t (a int);\n"+
		`CREATE INDEX IF NOT EXISTS "`+long+`" ON t (a);`+"\n"+`CREATE INDEX IF NOT EXISTS "`+long+`" ON t (a);`)

	c.Assert(err, qt.IsNil)
	c.Assert(cut, qt.DeepEquals, []sqlschema.StoredName{
		{Kind: "index", Scope: `schema "public"`, Written: long, Stored: long[:63], Limit: 63},
	})
}

// TestReadStoredNames_RefusesAGuardedNameCutToOneInUse holds the collisions an
// IF NOT EXISTS or an OR REPLACE hides from the server. PostgreSQL 15.19 runs
// each file: it skips the second index, table and sequence with "already
// exists, skipping", and replaces the first view with the second. The file
// names two objects and the server keeps one, so the reader refuses it. We are
// stricter than the server here, deliberately.
func TestReadStoredNames_RefusesAGuardedNameCutToOneInUse(t *testing.T) {
	long := strings.Repeat("k", 63)
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "an index",
			sql: "CREATE TABLE t (a int, b int);\n" + `CREATE INDEX IF NOT EXISTS "` + long + `_alpha" ON t (a);` + "\n" +
				`CREATE INDEX IF NOT EXISTS "` + long + `_beta" ON t (b);`,
			want: `.*index "` + long + `_alpha" and index "` + long + `_beta" in schema "public" are both stored as "` + long +
				`", and the server keeps only one of them`,
		},
		{
			name: "a table",
			sql:  `CREATE TABLE IF NOT EXISTS "` + long + `_alpha" (a int);` + "\n" + `CREATE TABLE IF NOT EXISTS "` + long + `_beta" (b int);`,
			want: `.*table "` + long + `_alpha" and table "` + long + `_beta" in schema "public" are both stored as "` + long +
				`", and the server keeps only one of them`,
		},
		{
			name: "a sequence",
			sql:  `CREATE SEQUENCE IF NOT EXISTS "` + long + `_alpha";` + "\n" + `CREATE SEQUENCE IF NOT EXISTS "` + long + `_beta";`,
			want: `.*sequence "` + long + `_alpha" and sequence "` + long + `_beta" in schema "public" are both stored as "` + long +
				`", and the server keeps only one of them`,
		},
		{
			name: "a view",
			sql:  `CREATE OR REPLACE VIEW "` + long + `_alpha" AS SELECT 1 AS a;` + "\n" + `CREATE OR REPLACE VIEW "` + long + `_beta" AS SELECT 2 AS a;`,
			want: `.*view "` + long + `_alpha" and view "` + long + `_beta" in schema "public" are both stored as "` + long +
				`", and the server keeps only one of them`,
		},
		{
			// The server replaces the first function with the second.
			name: "a function",
			sql: `CREATE OR REPLACE FUNCTION "` + long + `_alpha"() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;` + "\n" +
				`CREATE OR REPLACE FUNCTION "` + long + `_beta"() RETURNS int LANGUAGE sql AS $$ SELECT 2 $$;`,
			want: `.*function "` + long + `_alpha" and function "` + long + `_beta" in schema "public" with arguments \(\) are both stored as "` +
				long + `", and the server keeps only one of them`,
		},
		{
			name: "a trigger",
			sql: "CREATE TABLE t (a int);\n" + triggerFunction +
				`CREATE OR REPLACE TRIGGER "` + long + `_alpha" BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION trg();` + "\n" +
				`CREATE OR REPLACE TRIGGER "` + long + `_beta" BEFORE UPDATE ON t FOR EACH ROW EXECUTE FUNCTION trg();`,
			want: `.*trigger "` + long + `_alpha" and trigger "` + long + `_beta" in table "public.t" are both stored as "` + long +
				`", and the server keeps only one of them`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cut, err := readNotices(c, platform.Postgres, test.sql)

			c.Assert(err, qt.ErrorIs, sqlschema.ErrStoredNameCollision)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(cut, qt.HasLen, 0)
		})
	}
}

// TestReadStoredNames_RefusesWhatTheServerRefuses holds each collision the
// server refuses, measured on PostgreSQL 15.19 with the message it gave.
func TestReadStoredNames_RefusesWhatTheServerRefuses(t *testing.T) {
	long := strings.Repeat("k", 63)
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			// relation "kkk…" already exists
			name: "two indexes in one schema",
			sql: "CREATE TABLE t (a int, b int);\n" + `CREATE INDEX "` + long + `_alpha" ON t (a);` + "\n" +
				`CREATE INDEX "` + long + `_beta" ON t (b);`,
			want: `.*index "` + long + `_alpha" and index "` + long + `_beta" in schema "public" are both stored as "` + long + `".*`,
		},
		{
			// constraint "kkk…" for relation "c" already exists
			name: "two foreign keys on one table",
			sql: "CREATE TABLE p (id int PRIMARY KEY);\n" + `CREATE TABLE c (id int PRIMARY KEY, a int, b int, CONSTRAINT "` + long +
				`_alpha" FOREIGN KEY (a) REFERENCES p (id), CONSTRAINT "` + long + `_beta" FOREIGN KEY (b) REFERENCES p (id));`,
			want: `.*foreign key "` + long + `_alpha" and foreign key "` + long + `_beta" in table "public.c" are both stored as.*`,
		},
		{
			// A name already at the limit and a longer one cut to it.
			name: "a 63-byte index and a longer one cut to it",
			sql:  "CREATE TABLE t (a int, b int);\n" + `CREATE INDEX "` + long + `" ON t (a);` + "\n" + `CREATE INDEX "` + long + `_beta" ON t (b);`,
			want: `.*index "` + long + `" and index "` + long + `_beta" in schema "public" are both stored as.*`,
		},
		{
			// An index cut to a table's name collides with the table.
			name: "an index cut to the name of a table",
			sql:  `CREATE TABLE "` + long + `" (a int);` + "\n" + `CREATE INDEX "` + long + `_x" ON "` + long + `" (a);`,
			want: `.*table "` + long + `" and index "` + long + `_x" in schema "public" are both stored as.*`,
		},
		{
			// A unique constraint builds an index of its own name.
			name: "a unique constraint and an index",
			sql: "CREATE TABLE t (a int, b int, CONSTRAINT \"" + long + "_alpha\" UNIQUE (a));\n" +
				`CREATE INDEX "` + long + `_beta" ON t (b);`,
			want: `.*index of unique constraint "` + long + `_alpha" and index "` + long + `_beta" in schema "public" are both stored as.*`,
		},
		{
			name: "two columns on one table",
			sql:  `CREATE TABLE t ("` + long + `_alpha" int, "` + long + `_beta" int);`,
			want: `.*column "` + long + `_alpha" and column "` + long + `_beta" in table "public.t" are both stored as.*`,
		},
		{
			// function "kkk…" already exists with same argument types
			name: "two functions with the same argument types",
			sql: `CREATE FUNCTION "` + long + `_alpha"() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;` + "\n" +
				`CREATE FUNCTION "` + long + `_beta"() RETURNS int LANGUAGE sql AS $$ SELECT 2 $$;`,
			want: `.*function "` + long + `_alpha" and function "` + long + `_beta" in schema "public" with arguments \(\) are both stored as.*`,
		},
		{
			// A function and a procedure share pg_proc, and the same message.
			name: "a function and a procedure of one signature",
			sql: `CREATE FUNCTION "` + long + `_alpha"() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;` + "\n" +
				`CREATE PROCEDURE "` + long + `_beta"() LANGUAGE sql AS $$ SELECT 2 $$;`,
			want: `.*function "` + long + `_alpha" and procedure "` + long + `_beta" in schema "public" with arguments \(\) are both stored as.*`,
		},
		{
			// trigger "kkk…" for relation "t" already exists
			name: "two triggers on one table",
			sql: "CREATE TABLE t (a int);\n" + triggerFunction +
				`CREATE TRIGGER "` + long + `_alpha" BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION trg();` + "\n" +
				`CREATE TRIGGER "` + long + `_beta" BEFORE UPDATE ON t FOR EACH ROW EXECUTE FUNCTION trg();`,
			want: `.*trigger "` + long + `_alpha" and trigger "` + long + `_beta" in table "public.t" are both stored as.*`,
		},
		{
			name: "a column added under a name cut to an existing one",
			sql:  `CREATE TABLE t ("` + long + `_alpha" int);` + "\n" + `ALTER TABLE t ADD COLUMN "` + long + `_beta" int;`,
			want: `.*column "` + long + `_alpha" and column "` + long + `_beta" in table "public.t" are both stored as.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := readNotices(c, platform.Postgres, test.sql)

			c.Assert(err, qt.ErrorIs, sqlschema.ErrStoredNameCollision)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// TestReadStoredNames_LeavesAServerThatKeepsLongNamesAlone is the control:
// CockroachDB v26.1.8 keeps a 70-byte name whole, so nothing is cut, reported
// or refused there.
func TestReadStoredNames_LeavesAServerThatKeepsLongNamesAlone(t *testing.T) {
	c := qt.New(t)
	long := strings.Repeat("k", 63)

	cut, err := readNotices(c, platform.CockroachDB, "CREATE TABLE t (a int, b int);\n"+
		`CREATE INDEX "`+long+`_alpha" ON t (a);`+"\n"+`CREATE INDEX "`+long+`_beta" ON t (b);`)

	c.Assert(err, qt.IsNil)
	c.Assert(cut, qt.HasLen, 0)
}

// TestReadStoredNames_RefusesANameDeclaredAgain holds each redeclaration
// PostgreSQL 15.19 refuses with `already exists` or `specified more than once`,
// which the reader used to merge into one object (stokaro/ptah#4914).
func TestReadStoredNames_RefusesANameDeclaredAgain(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{name: "a table created twice", sql: "CREATE TABLE t (a int);\nCREATE TABLE t (b int);", want: `.*table "t" in schema "public" takes the name of table "t".*`},
		{name: "an index created twice", sql: "CREATE TABLE t (a int, b int);\nCREATE INDEX ix ON t (a);\nCREATE INDEX ix ON t (b);", want: `.*index "ix" in schema "public" takes the name of index "ix".*`},
		{name: "an index named like a table", sql: "CREATE TABLE t (a int);\nCREATE INDEX t ON t (a);", want: `.*index "t" in schema "public" takes the name of table "t".*`},
		{name: "a view named like a table", sql: "CREATE TABLE t (a int);\nCREATE VIEW t AS SELECT 1 AS x;", want: `.*view "t" in schema "public" takes the name of table "t".*`},
		{name: "a sequence named like a table", sql: "CREATE TABLE t (a int);\nCREATE SEQUENCE t;", want: `.*sequence "t" in schema "public" takes the name of table "t".*`},
		{name: "an index named like a unique constraint", sql: "CREATE TABLE t (a int, b int, CONSTRAINT u UNIQUE (a));\nCREATE INDEX u ON t (b);", want: `.*index "u" in schema "public" takes the name of index of unique constraint "u".*`},
		{name: "a table named like a primary key's index", sql: "CREATE TABLE t (a int, CONSTRAINT t_pk PRIMARY KEY (a));\nCREATE TABLE t_pk (x int);", want: `.*table "t_pk" in schema "public" takes the name of index of primary key "t_pk".*`},
		{name: "a constraint added under a name in use", sql: "CREATE TABLE p (id int PRIMARY KEY);\nCREATE TABLE c (id int PRIMARY KEY, a int, CONSTRAINT fk FOREIGN KEY (a) REFERENCES p (id));\nALTER TABLE c ADD CONSTRAINT fk CHECK (a > 0);", want: `.*check "fk" in table "public.c" takes the name of foreign key "fk".*`},
		{name: "a column written twice", sql: "CREATE TABLE t (a int, a int);", want: `.*column "a" in table "public.t" takes the name of column "a".*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := readNotices(c, platform.Postgres, test.sql)

			c.Assert(err, qt.ErrorIs, sqlschema.ErrNameInUse)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// TestReadStoredNames_AcceptsWhatTheServerAccepts is the other side of the
// redeclaration rule, each file measured on PostgreSQL 15.19 to succeed.
func TestReadStoredNames_AcceptsWhatTheServerAccepts(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "a table guarded by IF NOT EXISTS", sql: "CREATE TABLE t (a int);\nCREATE TABLE IF NOT EXISTS t (b int);"},
		{name: "an index guarded by IF NOT EXISTS", sql: "CREATE TABLE t (a int, b int);\nCREATE INDEX ix ON t (a);\nCREATE INDEX IF NOT EXISTS ix ON t (b);"},
		{name: "a unique constraint written twice the same way", sql: "CREATE TABLE r3 (a int, CONSTRAINT r3_k UNIQUE (a), CONSTRAINT r3_k UNIQUE (a));"},
		{name: "a quoted name beside its folded spelling", sql: `CREATE TABLE "Docs" (id int);` + "\nCREATE TABLE docs (id int);"},
		{name: "an index dropped and created again", sql: "CREATE TABLE t (a int);\nCREATE INDEX ix ON t (a);\nDROP INDEX ix;\nCREATE INDEX ix ON t (a);"},
		{name: "a view replaced", sql: "CREATE TABLE t (a int);\nCREATE VIEW v AS SELECT a FROM t;\nCREATE OR REPLACE VIEW v AS SELECT a FROM t;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := readNotices(c, platform.Postgres, test.sql)

			c.Assert(err, qt.IsNil)
		})
	}
}
