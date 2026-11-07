//go:build integration

package notnullname_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// TestSchemaDiffTakesTheNameOfAColumnMadeNullable gives a column a name another column
// holds until the plan makes that column nullable, or takes the other way a name that a
// column the plan removes holds. Each script runs on the left database, the names read
// back match the right database, and the next plan is synced.
func TestSchemaDiffTakesTheNameOfAColumnMadeNullable(t *testing.T) {
	tests := []struct {
		name, left, right string
	}{
		{
			name: "a renamed column and a new nullable one under its old name",
			left: `CREATE TABLE t (id int PRIMARY KEY, a text NOT NULL)`,
			right: `CREATE TABLE t (id int PRIMARY KEY, a text NOT NULL);
				ALTER TABLE t RENAME COLUMN a TO b; ALTER TABLE t ADD COLUMN a text`,
		},
		{
			name:  "a chosen name that moves to a new column",
			left:  `CREATE TABLE t (id int PRIMARY KEY, a text CONSTRAINT nn NOT NULL)`,
			right: `CREATE TABLE t (id int PRIMARY KEY, a text, b text CONSTRAINT nn NOT NULL)`,
		},
		{
			name:  "a column made NOT NULL under the name of one made nullable, which sorts after it",
			left:  `CREATE TABLE t (id int PRIMARY KEY, a text, b text NOT NULL)`,
			right: `CREATE TABLE t (id int PRIMARY KEY, a text CONSTRAINT t_b_not_null NOT NULL, b text)`,
		},
		{
			name: "SET NOT NULL, whose default name a column the plan removes holds",
			left: `CREATE TABLE t (id int PRIMARY KEY, a text NOT NULL);
				ALTER TABLE t RENAME COLUMN a TO b; ALTER TABLE t ADD COLUMN a text`,
			right: `CREATE TABLE t (id int PRIMARY KEY, a text NOT NULL)`,
		},
		{
			name:  "a new column left to the server, whose default name a column made nullable holds",
			left:  `CREATE TABLE t (id int PRIMARY KEY, a text CONSTRAINT t_b_not_null NOT NULL)`,
			right: `CREATE TABLE t (id int PRIMARY KEY, a text, b text NOT NULL)`,
		},
	}
	for _, surface := range surfaces {
		for _, test := range tests {
			t.Run(surface.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				left := newDatabase(c, dbtarget.PostgreSQL, test.left)
				right := newDatabase(c, dbtarget.PostgreSQL, test.right)

				execScript(c, left, plan(c, surface.run, left, right))

				c.Assert(notNullNames(c, left), qt.Equals, notNullNames(c, right))
				assertSynced(c, surface.run, left, right)
			})
		}
	}
}

// TestSchemaDiffLeavesAMergedReleaseAsItIs holds the statement ptah-compat writes for the
// shape, which runs as it is: PostgreSQL runs DROP NOT NULL ahead of the additions of one
// ALTER TABLE, measured on 18.6, so it needs no rename, in either direction.
func TestSchemaDiffLeavesAMergedReleaseAsItIs(t *testing.T) {
	const before = `CREATE TABLE t (id int PRIMARY KEY, a text NOT NULL)`
	const after = `CREATE TABLE t (id int PRIMARY KEY, a text NOT NULL);
		ALTER TABLE t RENAME COLUMN a TO b; ALTER TABLE t ADD COLUMN a text`
	tests := []struct {
		name, left, right, want string
	}{
		{
			name:  "forward",
			left:  before,
			right: after,
			want: `-- POSTGRES: ADD COLUMN "b" fails if "t" holds any row; the column is NOT NULL and declares no default to fill it with.
-- Modify "t" table
ALTER TABLE "t" ALTER COLUMN "a" DROP NOT NULL, ADD COLUMN "b" text CONSTRAINT "t_a_not_null" NOT NULL;
`,
		},
		{
			name:  "back",
			left:  after,
			right: before,
			want: `-- POSTGRES: SET NOT NULL fails if any row of "t" holds NULL in "a"; the column declares no default to fill it with.
-- Modify "t" table
ALTER TABLE "t" DROP COLUMN "b", ALTER COLUMN "a" SET NOT NULL;
`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			left := newDatabase(c, dbtarget.PostgreSQL, test.left)
			right := newDatabase(c, dbtarget.PostgreSQL, test.right)

			c.Assert(plan(c, runCompat, left, right), qt.Equals, test.want)
			c.Assert(plan(c, runStrictCompat, left, right), qt.Equals, test.want)
		})
	}
}

// TestAColumnMadeNullableRollsBackToItsName runs what each surface writes forward, back
// and forward. The rollback has to give the column the name it held, whichever column
// holds it by then, and a name the server would give needs no statement of its own.
func TestAColumnMadeNullableRollsBackToItsName(t *testing.T) {
	tests := []struct {
		name, before, after string
		want                namesBothWays
	}{
		{
			name:   "the name the server gave, which a new column takes",
			before: `CREATE TABLE t (id int PRIMARY KEY, a text NOT NULL)`,
			after:  `CREATE TABLE t (id int PRIMARY KEY, a text, b text CONSTRAINT t_a_not_null NOT NULL)`,
			want: namesBothWays{
				before:   "a=t_a_not_null,id=t_id_not_null",
				after:    "b=t_a_not_null,id=t_id_not_null",
				restored: "a=t_a_not_null,id=t_id_not_null",
				again:    "b=t_a_not_null,id=t_id_not_null",
			},
		},
		{
			name:   "a chosen name, which no column takes",
			before: `CREATE TABLE t (id int PRIMARY KEY, a text CONSTRAINT nn NOT NULL)`,
			after:  `CREATE TABLE t (id int PRIMARY KEY, a text)`,
			want: namesBothWays{
				before:   "a=nn,id=t_id_not_null",
				after:    "id=t_id_not_null",
				restored: "a=nn,id=t_id_not_null",
				again:    "id=t_id_not_null",
			},
		},
		{
			name:   "a chosen name, which a new column takes",
			before: `CREATE TABLE t (id int PRIMARY KEY, a text CONSTRAINT nn NOT NULL)`,
			after:  `CREATE TABLE t (id int PRIMARY KEY, a text, b text CONSTRAINT nn NOT NULL)`,
			want: namesBothWays{
				before:   "a=nn,id=t_id_not_null",
				after:    "b=nn,id=t_id_not_null",
				restored: "a=nn,id=t_id_not_null",
				again:    "b=nn,id=t_id_not_null",
			},
		},
		{
			name:   "a column made NOT NULL under the name of one the plan removes",
			before: `CREATE TABLE t (id int PRIMARY KEY, a text, b text NOT NULL)`,
			after:  `CREATE TABLE t (id int PRIMARY KEY, a text CONSTRAINT t_b_not_null NOT NULL)`,
			want: namesBothWays{
				before:   "b=t_b_not_null,id=t_id_not_null",
				after:    "a=t_b_not_null,id=t_id_not_null",
				restored: "b=t_b_not_null,id=t_id_not_null",
				again:    "a=t_b_not_null,id=t_id_not_null",
			},
		},
	}
	for _, test := range tests {
		t.Run("ptah/"+test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(nativeFilesBothWays(c, test.before, test.after), qt.Equals, test.want)
		})
		t.Run("ptah-compat/"+test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(compatFilesBothWays(c, runCompat, test.before, test.after), qt.Equals, test.want)
		})
		t.Run("ptah-compat strict/"+test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(compatFilesBothWays(c, runStrictCompat, test.before, test.after), qt.Equals, test.want)
		})
	}
}
