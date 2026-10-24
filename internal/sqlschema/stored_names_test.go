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
			name: "an index guarded by IF NOT EXISTS",
			sql: "CREATE TABLE t (a int);\n" + `CREATE INDEX IF NOT EXISTS "` + long + `_alpha" ON t (a);` + "\n" +
				`CREATE INDEX IF NOT EXISTS "` + long + `_beta" ON t (a);`,
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
