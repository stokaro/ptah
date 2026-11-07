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
	"ptah.run/migration/generator"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// rollbackOfPlan plans held to desired on dialect and answers the statements that roll
// the plan back.
func rollbackOfPlan(c *qt.C, dialect, held, desired string) []string {
	c.Helper()
	runtime := builtintest.Runtime()
	read := func(sql string) schemamodel.Database {
		schema, _, err := sqlschema.ReadWith(c.Context(), []byte(sql), platform.Postgres, nil, sqlschema.Options{Owners: runtime.SQL()})
		c.Assert(err, qt.IsNil, qt.Commentf("%s", sql))
		return schema
	}
	heldSchema, desiredSchema := read(held), read(desired)
	current, err := goschematodb.ToDBSchema(c.Context(), &heldSchema, dialect, runtime)
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDialect(c.Context(), &desiredSchema, current, dialect, runtime)
	c.Assert(err, qt.IsNil)
	plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: must.Must(builtin.New()), Diff: diff, DesiredSchema: &desiredSchema, CurrentSchema: current,
		Dialect: dialect, Capabilities: capability.ForDialect(dialect),
		Policy: generator.BidirectionalPlanPolicy{Create: generator.ConcurrentIndexDisabled, Drop: generator.ConcurrentIndexDisabled},
	})
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), must.Must(builtin.New()), plan.Reverse.Diff, dialect,
		planext.Options{Capabilities: capability.ForDialect(dialect), Statements: planext.StatementStyleExact})
	c.Assert(err, qt.IsNil)
	return statements
}

// TestUniqueNameAnAddedIndexTakes_HappyPath pins the rollback of an index turned into a
// UNIQUE under one name: it adds the index and removes the UNIQUE. A UNIQUE is an index
// of its name, so the addition stops at the server while the UNIQUE stands. On MySQL and
// MariaDB one statement does both, as for an index rebuilt under its own name, since a
// foreign key may stand on the UNIQUE and on nothing else.
func TestUniqueNameAnAddedIndexTakes_HappyPath(t *testing.T) {
	const (
		held    = `CREATE TABLE t (id int PRIMARY KEY, a int); CREATE INDEX u ON t (a);`
		desired = `CREATE TABLE t (id int PRIMARY KEY, a int, CONSTRAINT u UNIQUE (a));`
	)
	tests := []struct {
		dialect string
		want    []string
	}{
		{dialect: "mysql", want: []string{"ALTER TABLE `t` DROP INDEX `u`, ADD INDEX `u` (`a`)"}},
		{dialect: "mariadb", want: []string{"ALTER TABLE `t` DROP INDEX `u`, ADD INDEX `u` (`a`)"}},
		{dialect: "postgres", want: []string{`ALTER TABLE "t" DROP CONSTRAINT "u"`, `CREATE INDEX "u" ON "t" ("a")`}},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			got := rollbackOfPlan(c, test.dialect, held, desired)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestUniqueNameAnAddedIndexTakes_FailurePath pins the rollbacks that keep their order:
// the index has another name, the UNIQUE is on another table, or a CHECK holds the name,
// which is not an index.
func TestUniqueNameAnAddedIndexTakes_FailurePath(t *testing.T) {
	tests := []struct {
		name, held, desired string
		want                []string
	}{
		{
			name:    "another name",
			held:    `CREATE TABLE t (id int PRIMARY KEY, a int); CREATE INDEX v ON t (a);`,
			desired: `CREATE TABLE t (id int PRIMARY KEY, a int, CONSTRAINT u UNIQUE (a));`,
			want:    []string{"CREATE INDEX `v` ON `t` (`a`)", "ALTER TABLE `t` DROP INDEX `u`"},
		},
		{
			name: "another table",
			held: `CREATE TABLE t (id int PRIMARY KEY, a int);
				CREATE TABLE z (id int PRIMARY KEY, b int); CREATE INDEX u ON z (b);`,
			desired: `CREATE TABLE t (id int PRIMARY KEY, a int, CONSTRAINT u UNIQUE (a));
				CREATE TABLE z (id int PRIMARY KEY, b int);`,
			want: []string{"CREATE INDEX `u` ON `z` (`b`)", "ALTER TABLE `t` DROP INDEX `u`"},
		},
		{
			name:    "a CHECK of that name",
			held:    `CREATE TABLE t (id int PRIMARY KEY, a int); CREATE INDEX u ON t (a);`,
			desired: `CREATE TABLE t (id int PRIMARY KEY, a int, CONSTRAINT u CHECK (a > 0));`,
			want:    []string{"CREATE INDEX `u` ON `t` (`a`)", "ALTER TABLE `t` DROP CONSTRAINT `u`"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := rollbackOfPlan(c, "mysql", test.held, test.desired)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
