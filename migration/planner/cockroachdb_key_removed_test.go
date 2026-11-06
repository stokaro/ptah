package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestCockroachDBKeyRemoved_HappyPath pins the plan for a primary key the declaration
// leaves out on CockroachDB, where every table holds one. A DROP CONSTRAINT alone stops
// at the server, so the plan builds the hidden column a table declared without a key
// holds, and moves the key to it in one statement.
func TestCockroachDBKeyRemoved_HappyPath(t *testing.T) {
	const addRowID = `ALTER TABLE "parent" ADD COLUMN "rowid" INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid()`
	tests := []struct {
		name, desired string
		want          []string
	}{
		{
			name:    "the key leaves",
			desired: `CREATE TABLE parent (id bigint NOT NULL, code bigint);`,
			want: []string{
				addRowID,
				`ALTER TABLE "parent" DROP CONSTRAINT "parent_pkey", ADD CONSTRAINT "parent_pkey" PRIMARY KEY ("rowid")`,
			},
		},
		{
			name:    "its column leaves after it",
			desired: `CREATE TABLE parent (code bigint);`,
			want: []string{
				addRowID,
				`ALTER TABLE "parent" DROP CONSTRAINT "parent_pkey", ADD CONSTRAINT "parent_pkey" PRIMARY KEY ("rowid")`,
				`ALTER TABLE "parent" DROP COLUMN "id"`,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := columnKeyPlan(c, "cockroachdb", `CREATE TABLE parent (id bigint PRIMARY KEY, code bigint);`, test.desired, "id")

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestCockroachDBKeyRemoved_FailurePath pins the plans that build no hidden column: the
// table takes another key, the table leaves, or PostgreSQL, which holds a table without
// a key.
func TestCockroachDBKeyRemoved_FailurePath(t *testing.T) {
	const held = `CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);`
	tests := []struct {
		name, dialect, desired string
		want                   []string
	}{
		{
			name:    "the key moves",
			dialect: "cockroachdb",
			desired: `CREATE TABLE parent (id bigint NOT NULL, code bigint PRIMARY KEY);`,
			want:    []string{`ALTER TABLE "parent" DROP CONSTRAINT "parent_pkey", ADD PRIMARY KEY ("code")`},
		},
		{
			name:    "the table leaves",
			dialect: "cockroachdb",
			desired: `CREATE TABLE other (id bigint PRIMARY KEY);`,
			want:    []string{`CREATE TABLE "other" (` + "\n" + `  "id" bigint PRIMARY KEY NOT NULL` + "\n" + `)`, `DROP TABLE "parent"`},
		},
		{
			name:    "PostgreSQL holds a table without a key",
			dialect: "postgres",
			desired: `CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL);`,
			want:    []string{`ALTER TABLE "parent" DROP CONSTRAINT "parent_pkey"`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := columnKeyPlan(c, test.dialect, held, test.desired, "id")

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestCockroachDBKeyRemoved_AnotherConstraintDropsAlone pins a removed constraint that is
// not the table's key: the table keeps its key, so no hidden column comes with the drop.
func TestCockroachDBKeyRemoved_AnotherConstraintDropsAlone(t *testing.T) {
	c := qt.New(t)

	got := columnKeyPlan(c, "cockroachdb",
		`CREATE TABLE parent (id bigint PRIMARY KEY, code bigint, CONSTRAINT code_rule CHECK (code > 0));`,
		`CREATE TABLE parent (id bigint PRIMARY KEY, code bigint);`,
		"id",
	)

	c.Assert(got, qt.DeepEquals, []string{`ALTER TABLE "parent" DROP CONSTRAINT "code_rule"`})
}
