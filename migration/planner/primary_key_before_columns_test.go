package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
)

// TestPrimaryKeyBeforeColumns_LeavesFirst pins the order a column change needs. While
// the key holds it, PostgreSQL answers `ALTER COLUMN DROP NOT NULL` with `column "id"
// is in a primary key`, and an added PRIMARY KEY column with `multiple primary keys for
// table "parent" are not allowed`. An added column under a key declared on the table
// leaves the key to the table, since it carries the column's flag as well.
func TestPrimaryKeyBeforeColumns_LeavesFirst(t *testing.T) {
	const held = `CREATE TABLE parent (id bigint PRIMARY KEY);`
	tests := []struct {
		name          string
		held, desired string
		want          []string
	}{
		{
			name:    "moved, with the old column nullable",
			held:    `CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);`,
			desired: `CREATE TABLE parent (id bigint, code bigint NOT NULL, CONSTRAINT parent_pkey PRIMARY KEY (code));`,
			want: []string{
				`ALTER TABLE "parent" DROP CONSTRAINT "parent_pkey"`,
				`ALTER TABLE "parent" ALTER COLUMN "id" DROP NOT NULL`,
				`ALTER TABLE "parent" ADD PRIMARY KEY ("code")`,
			},
		},
		{
			name:    "dropped, with the column nullable",
			held:    `CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);`,
			desired: `CREATE TABLE parent (id bigint, code bigint NOT NULL);`,
			want: []string{
				`ALTER TABLE "parent" DROP CONSTRAINT "parent_pkey"`,
				`ALTER TABLE "parent" ALTER COLUMN "id" DROP NOT NULL`,
			},
		},
		{
			name:    "moved to a new column that declares it",
			held:    held,
			desired: `CREATE TABLE parent (id bigint NOT NULL, code bigint PRIMARY KEY);`,
			want: []string{
				`ALTER TABLE "parent" DROP CONSTRAINT "parent_pkey"`,
				addCodeComment("POSTGRES") + `ALTER TABLE "parent" ADD COLUMN "code" bigint PRIMARY KEY NOT NULL`,
			},
		},
		{
			name:    "moved to a new column the table's key names",
			held:    held,
			desired: `CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, CONSTRAINT parent_pkey PRIMARY KEY (code));`,
			want: []string{
				`ALTER TABLE "parent" DROP CONSTRAINT "parent_pkey"`,
				addCodeComment("POSTGRES") + `ALTER TABLE "parent" ADD COLUMN "code" bigint NOT NULL`,
				`ALTER TABLE "parent" ADD PRIMARY KEY ("code")`,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(keyPlan(c, platform.Postgres, test.held, test.desired, ""), qt.DeepEquals, test.want)
		})
	}
}

// TestPrimaryKeyBeforeColumns_StaysWhereItWas is the control: with no column change in
// its way the key is replaced where the plan adds it, after the column changes.
func TestPrimaryKeyBeforeColumns_StaysWhereItWas(t *testing.T) {
	c := qt.New(t)

	got := keyPlan(c, platform.Postgres,
		`CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL, x int NOT NULL);`,
		`CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, x int, CONSTRAINT parent_pkey PRIMARY KEY (code));`,
		"")

	c.Assert(got, qt.DeepEquals, []string{
		`ALTER TABLE "parent" ALTER COLUMN "x" DROP NOT NULL`,
		`ALTER TABLE "parent" DROP CONSTRAINT "parent_pkey"`,
		`ALTER TABLE "parent" ADD PRIMARY KEY ("code")`,
	})
}

// TestPrimaryKeyBeforeColumns_AnotherEngineKeepsItsPlan holds the order to the engine
// it was measured on.
func TestPrimaryKeyBeforeColumns_AnotherEngineKeepsItsPlan(t *testing.T) {
	c := qt.New(t)

	got := keyPlan(c, platform.CockroachDB,
		`CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);`,
		`CREATE TABLE parent (id bigint, code bigint NOT NULL, CONSTRAINT parent_pkey PRIMARY KEY (code));`,
		"")

	c.Assert(got[0], qt.Equals, `ALTER TABLE "parent" ALTER COLUMN "id" DROP NOT NULL`)
}
