package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
)

// TestCockroachDBKeyMove_HappyPath pins a key's move as one statement. CockroachDB
// refuses the drop in a statement of its own, with `this schema change is disallowed
// because table "t" is locked` on a table of the default schema_locked.
func TestCockroachDBKeyMove_HappyPath(t *testing.T) {
	const (
		held = "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);"
		move = `ALTER TABLE "parent" DROP CONSTRAINT "parent_pkey", ADD PRIMARY KEY ("code")`
	)
	tests := []struct {
		name, held, desired string
		want                []string
	}{
		{
			name: "a table's key", held: held,
			desired: "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, PRIMARY KEY (code));",
			want:    []string{move},
		},
		{
			name: "a column's key", held: held,
			desired: "CREATE TABLE parent (id bigint NOT NULL, code bigint PRIMARY KEY);",
			want:    []string{move},
		},
		{
			name: "a column the plan adds takes its key from the table", held: "CREATE TABLE parent (id bigint PRIMARY KEY);",
			desired: "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, PRIMARY KEY (code));",
			want:    []string{addCodeComment("COCKROACHDB") + `ALTER TABLE "parent" ADD COLUMN "code" bigint NOT NULL`, move},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(columnKeyPlan(c, platform.CockroachDB, test.held, test.desired, "id"), qt.DeepEquals, test.want)
		})
	}
}

// TestCockroachDBKeyMove_PostgreSQLWritesTwoStatements is the control: there the drop
// and the addition stay apart, and the plan can put a column change between them.
func TestCockroachDBKeyMove_PostgreSQLWritesTwoStatements(t *testing.T) {
	c := qt.New(t)
	got := columnKeyPlan(c, platform.Postgres, "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);",
		"CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, PRIMARY KEY (code));", "id")
	c.Assert(got, qt.DeepEquals, []string{
		`ALTER TABLE "parent" DROP CONSTRAINT "parent_pkey"`,
		`ALTER TABLE "parent" ADD PRIMARY KEY ("code")`,
	})
}
