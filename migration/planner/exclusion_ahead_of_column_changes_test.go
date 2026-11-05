package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// exclusionPlanOf plans desired over held, each a SQL schema, on PostgreSQL in the
// exact style.
func exclusionPlanOf(c *qt.C, held, desired string) []string {
	c.Helper()
	runtime := builtintest.Runtime()
	read := func(sql string) schemamodel.Database {
		schema, _, err := sqlschema.ReadWith(c.Context(), []byte(sql), platform.Postgres, nil, sqlschema.Options{Owners: runtime.SQL()})
		c.Assert(err, qt.IsNil, qt.Commentf("%s", sql))
		return schema
	}
	heldSchema, desiredSchema := read(held), read(desired)
	current, err := goschematodb.ToDBSchema(c.Context(), &heldSchema, platform.Postgres, runtime)
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDialect(c.Context(), &desiredSchema, current, platform.Postgres, runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), must.Must(builtin.New()), diff, platform.Postgres,
		planext.Options{Capabilities: capability.ForDialect(platform.Postgres), Statements: planext.StatementStyleExact})
	c.Assert(err, qt.IsNil)
	return statements
}

// TestExclusionAheadOfColumnChanges_HappyPath pins where an exclusion constraint
// leaves. The server rebuilds it under ALTER COLUMN TYPE and answers a type its
// index method cannot take with `data type text has no default operator class for
// access method "gist"`, so it goes first, and a new definition follows the column
// changes it may read.
func TestExclusionAheadOfColumnChanges_HappyPath(t *testing.T) {
	const held = "CREATE TABLE t (id int PRIMARY KEY, r int4range, s int4range, CONSTRAINT t_ex EXCLUDE USING gist (r WITH &&));"
	tests := []struct {
		name, desired string
		want          []string
	}{
		{
			name:    "the constraint leaves",
			desired: "CREATE TABLE t (id int PRIMARY KEY, r text, s int4range);",
			want: []string{
				`ALTER TABLE "t" DROP CONSTRAINT "t_ex"`,
				`ALTER TABLE "t" ALTER COLUMN "r" TYPE text`,
			},
		},
		{
			name:    "the constraint moves under its name to a column the plan adds",
			desired: "CREATE TABLE t (id int PRIMARY KEY, r text, s int4range, u int4range, CONSTRAINT t_ex EXCLUDE USING gist (u WITH &&));",
			want: []string{
				`ALTER TABLE "t" DROP CONSTRAINT "t_ex"`,
				`ALTER TABLE "t" ADD COLUMN "u" int4range`,
				`ALTER TABLE "t" ALTER COLUMN "r" TYPE text`,
				`ALTER TABLE "t" ADD CONSTRAINT "t_ex" EXCLUDE USING gist (u WITH &&)`,
			},
		},
		{
			name:    "the constraint moves under another name",
			desired: "CREATE TABLE t (id int PRIMARY KEY, r text, s int4range, CONSTRAINT t_new EXCLUDE USING gist (s WITH &&));",
			want: []string{
				`ALTER TABLE "t" DROP CONSTRAINT "t_ex"`,
				`ALTER TABLE "t" ALTER COLUMN "r" TYPE text`,
				`ALTER TABLE "t" ADD CONSTRAINT "t_new" EXCLUDE USING gist (s WITH &&)`,
			},
		},
		{
			name:    "the column it covers leaves with it",
			desired: "CREATE TABLE t (id int PRIMARY KEY, s int4range);",
			want: []string{
				`ALTER TABLE "t" DROP CONSTRAINT "t_ex"`,
				`ALTER TABLE "t" DROP COLUMN "r"`,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(exclusionPlanOf(c, held, test.desired), qt.DeepEquals, test.want)
		})
	}
}
