//go:build integration

package notnullname_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// seventeen is the PostgreSQL 17 server, which the TimescaleDB image carries and which
// stores no NOT NULL name. A move of that image to another release fails here rather
// than leaving these rows comparing two servers that keep names.
func seventeen(c *qt.C, schema string) string {
	c.Helper()
	database := newDatabase(c, dbtarget.TimescaleDB, schema)
	c.Assert(queryString(c, database, `SHOW server_version_num`), qt.Matches, `17\d{4}`)
	return database
}

// notNullColumns names the columns of public.t the catalog holds as NOT NULL, in name
// order. It reads pg_attribute, which every release answers.
func notNullColumns(c *qt.C, database string) string {
	c.Helper()
	return queryString(c, database, `
		SELECT COALESCE(string_agg(attname, ',' ORDER BY attname), '')
		FROM pg_attribute
		WHERE attrelid = 'public.t'::regclass AND attnum > 0 AND NOT attisdropped AND attnotnull`)
}

// TestSchemaDiffFromAReleaseThatKeepsNoName_HappyPath plans from PostgreSQL 17 toward
// PostgreSQL 18, where a read names every NOT NULL. The name a server gives a NOT NULL
// nobody named asks for nothing on the left, so the script says NOT NULL, runs, and the
// next plan is synced.
func TestSchemaDiffFromAReleaseThatKeepsNoName_HappyPath(t *testing.T) {
	tests := []struct {
		name, left, right string
		want              string
	}{
		{
			name:  "an added column",
			left:  `CREATE TABLE t (id int PRIMARY KEY)`,
			right: `CREATE TABLE t (id int PRIMARY KEY, c text NOT NULL)`,
			want:  "c,id",
		},
		{
			name:  "a new table",
			left:  `SELECT 1`,
			right: `CREATE TABLE t (id int PRIMARY KEY, c text NOT NULL)`,
			want:  "c,id",
		},
		{
			name:  "an added column whose default name the server cuts",
			left:  `CREATE TABLE t (id int PRIMARY KEY)`,
			right: `CREATE TABLE t (id int PRIMARY KEY, a_column_name_that_is_much_longer_than_fifty_four_bytes_x text NOT NULL)`,
			want:  "a_column_name_that_is_much_longer_than_fifty_four_bytes_x,id",
		},
	}
	for _, surface := range surfaces {
		for _, test := range tests {
			t.Run(surface.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				left := seventeen(c, test.left)
				right := newDatabase(c, dbtarget.PostgreSQL, test.right)

				script := plan(c, surface.run, left, right)
				execScript(c, left, script)

				c.Assert(script, qt.Not(qt.Contains), "CONSTRAINT")
				c.Assert(notNullColumns(c, left), qt.Equals, test.want)
				assertSynced(c, surface.run, left, right)
			})
		}
	}
}

// TestSchemaDiffFromAReleaseThatKeepsNoName_FailurePath holds the refusal for any other
// name, which PostgreSQL 17 would lose on the way in. A name the server numbered reads
// like one an author wrote: the catalog does not say which it is.
func TestSchemaDiffFromAReleaseThatKeepsNoName_FailurePath(t *testing.T) {
	tests := []struct {
		name, left, right string
		want              string
	}{
		{
			name:  "a chosen name on an added column",
			left:  `CREATE TABLE t (id int PRIMARY KEY)`,
			right: `CREATE TABLE t (id int PRIMARY KEY, c text CONSTRAINT nn NOT NULL)`,
			want:  `(?s).*error rendering add column: postgres column c: this target does not keep a NOT NULL constraint name \(nn\).*`,
		},
		{
			name:  "a chosen name in a new table",
			left:  `SELECT 1`,
			right: `CREATE TABLE t (id int PRIMARY KEY, c text CONSTRAINT nn NOT NULL)`,
			want:  `(?s).*error rendering column c: postgres column c: this target does not keep a NOT NULL constraint name \(nn\).*`,
		},
		{
			name:  "a numbered name",
			left:  `CREATE TABLE t (id int PRIMARY KEY)`,
			right: `CREATE TABLE t (id int PRIMARY KEY, c text CONSTRAINT t_c_not_null1 NOT NULL)`,
			want:  `(?s).*error rendering add column: postgres column c: this target does not keep a NOT NULL constraint name \(t_c_not_null1\).*`,
		},
	}
	for _, surface := range surfaces {
		for _, test := range tests {
			t.Run(surface.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				left := seventeen(c, test.left)
				right := newDatabase(c, dbtarget.PostgreSQL, test.right)

				_, err := surface.run("schema", "diff", "--from", left, "--to", right)

				c.Assert(err, qt.ErrorMatches, test.want)
			})
		}
	}
}

// TestSchemaDiffTowardAReleaseThatKeepsNoName is the other direction: PostgreSQL 18 is
// the left, and the right names nothing. The server gives an added column its own name,
// and a chosen name on the left stays, because a side that names nothing asks for no
// rename.
func TestSchemaDiffTowardAReleaseThatKeepsNoName(t *testing.T) {
	tests := []struct {
		name, left, right string
		want              string
	}{
		{
			name:  "an added column",
			left:  `CREATE TABLE t (id int PRIMARY KEY)`,
			right: `CREATE TABLE t (id int PRIMARY KEY, c text NOT NULL)`,
			want:  "c=t_c_not_null,id=t_id_not_null",
		},
		{
			name:  "a column made NOT NULL",
			left:  `CREATE TABLE t (id int PRIMARY KEY, c text)`,
			right: `CREATE TABLE t (id int PRIMARY KEY, c text NOT NULL)`,
			want:  "c=t_c_not_null,id=t_id_not_null",
		},
	}
	for _, surface := range surfaces {
		for _, test := range tests {
			t.Run(surface.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				left := newDatabase(c, dbtarget.PostgreSQL, test.left)
				right := seventeen(c, test.right)

				execScript(c, left, plan(c, surface.run, left, right))

				c.Assert(notNullNames(c, left), qt.Equals, test.want)
				assertSynced(c, surface.run, left, right)
			})
		}
		t.Run(surface.name+"/a chosen name on the left", func(t *testing.T) {
			c := qt.New(t)
			left := newDatabase(c, dbtarget.PostgreSQL, `CREATE TABLE t (id int PRIMARY KEY, c text CONSTRAINT nn NOT NULL)`)
			right := seventeen(c, `CREATE TABLE t (id int PRIMARY KEY, c text NOT NULL)`)

			assertSynced(c, surface.run, left, right)
			c.Assert(notNullNames(c, left), qt.Equals, "c=nn,id=t_id_not_null")
		})
	}
}
