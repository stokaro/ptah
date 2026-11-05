package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestPrimaryKeyRename_HappyPath pins the plan for a declared primary key name over
// the columns the live key holds. Read as the live key, the name reads as synced and
// the two stay apart; read as another key, the plan rebuilds the key and everything
// over it. The statement that keeps the key is a rename.
func TestPrimaryKeyRename_HappyPath(t *testing.T) {
	const (
		rename      = `ALTER TABLE "parent" RENAME CONSTRAINT "parent_pkey" TO "parent_custom"`
		keyedParent = `CREATE TABLE parent (id bigint PRIMARY KEY);`
	)
	tests := []struct {
		name, dialect, held, desired string
		want                         []string
	}{
		{
			name:    "a table's key on PostgreSQL",
			dialect: "postgres",
			held:    keyedParent,
			desired: `CREATE TABLE parent (id bigint NOT NULL, CONSTRAINT parent_custom PRIMARY KEY (id));`,
			want:    []string{rename},
		},
		{
			name:    "a column's key on PostgreSQL",
			dialect: "postgres",
			held:    keyedParent,
			desired: `CREATE TABLE parent (id bigint CONSTRAINT parent_custom PRIMARY KEY);`,
			want:    []string{rename},
		},
		{
			name:    "a table's key on CockroachDB",
			dialect: "cockroachdb",
			held:    keyedParent,
			desired: `CREATE TABLE parent (id bigint NOT NULL, CONSTRAINT parent_custom PRIMARY KEY (id));`,
			want:    []string{rename},
		},
		{
			name:    "an index that leaves holds the name",
			dialect: "postgres",
			held:    `CREATE TABLE parent (id bigint PRIMARY KEY, code bigint); CREATE INDEX parent_custom ON parent (code);`,
			desired: `CREATE TABLE parent (id bigint NOT NULL, code bigint, CONSTRAINT parent_custom PRIMARY KEY (id));`,
			want:    []string{`DROP INDEX "parent_custom"`, rename},
		},
		{
			name:    "no name declared keeps the live one",
			dialect: "postgres",
			held:    keyedParent,
			desired: `CREATE TABLE parent (id bigint NOT NULL, PRIMARY KEY (id));`,
			want:    make([]string, 0),
		},
		{
			name:    "the live name declared",
			dialect: "postgres",
			held:    keyedParent,
			desired: `CREATE TABLE parent (id bigint NOT NULL, CONSTRAINT parent_pkey PRIMARY KEY (id));`,
			want:    make([]string, 0),
		},
		{
			name:    "a key the server names itself",
			dialect: "mysql",
			held:    keyedParent,
			desired: `CREATE TABLE parent (id bigint NOT NULL, CONSTRAINT parent_custom PRIMARY KEY (id));`,
			want:    make([]string, 0),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := columnKeyPlan(c, test.dialect, test.held, test.desired, "id")

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestPrimaryKeyRename_FailurePath pins the declarations a rename cannot serve: the key
// differs beyond its name.
func TestPrimaryKeyRename_FailurePath(t *testing.T) {
	const dropKey = `ALTER TABLE "parent" DROP CONSTRAINT "parent_pkey"`
	tests := []struct {
		name, held, desired string
		want                []string
	}{
		{
			name:    "the key moves to other columns",
			held:    `CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);`,
			desired: `CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, CONSTRAINT parent_custom PRIMARY KEY (code));`,
			want:    []string{dropKey, `ALTER TABLE "parent" ADD CONSTRAINT "parent_custom" PRIMARY KEY ("code")`},
		},
		{
			name:    "the key gains a payload",
			held:    `CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);`,
			desired: `CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, CONSTRAINT parent_custom PRIMARY KEY (id) INCLUDE (code));`,
			want:    []string{dropKey, `ALTER TABLE "parent" ADD CONSTRAINT "parent_custom" PRIMARY KEY ("id") INCLUDE ("code")`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := columnKeyPlan(c, "postgres", test.held, test.desired, "id")

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
