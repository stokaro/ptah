package postgres_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/postgres"
)

// notNullNameDiff is one table whose column's NOT NULL constraint name moved
// from current to desired.
func notNullNameDiff(current, desired string) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName: "widget",
			ColumnsModified: []difftypes.ColumnDiff{{
				ColumnName: "a",
				Changes:    make(map[string]string),
				NotNullConstraintNameChange: &difftypes.NotNullConstraintNameChange{
					Current: current,
					Desired: desired,
				},
			}},
		}},
	}
}

// planNotNullName renders the plan for one name transition against a target
// whose named-NOT-NULL answer is the caller's.
func planNotNullName(c *qt.C, current, desired string, named bool) string {
	c.Helper()

	caps := capability.Postgres17().With(capability.NamedNotNullConstraints, named)
	nodes, err := postgres.NewForDialect(platform.Postgres, caps).
		GenerateMigrationAST(
			context.Background(), must.Must(builtin.New()),
			notNullNameDiff(current, desired),
		)
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQLWithCapabilities(platform.Postgres, caps, nodes...)
	c.Assert(err, qt.IsNil)
	return sql
}

// TestPlanner_ANameOnlyDriftIsRenamed pins the statement, and the reason it is
// a rename rather than a drop and an add.
//
// PostgreSQL generates a name for an unnamed NOT NULL, so re-establishing the
// constraint through `ALTER COLUMN ... SET NOT NULL` would land on the
// generated name rather than the declared one, and the column is momentarily
// nullable in between (stokaro/ptah#2161).
func TestPlanner_ANameOnlyDriftIsRenamed(t *testing.T) {
	c := qt.New(t)

	sql := planNotNullName(c, "c_old", "c_new", true)

	c.Assert(sql, qt.Contains, `RENAME CONSTRAINT "c_old" TO "c_new"`)
	c.Assert(sql, qt.Not(qt.Contains), "SET NOT NULL")
	c.Assert(sql, qt.Not(qt.Contains), "DROP CONSTRAINT")
}

// TestPlanner_ATargetWithoutTheCapabilitySaysSo is the gap a live run found.
//
// The renderer refuses a named NOT NULL at CREATE TABLE, but a column that
// already exists never reaches that path -- so the declared name vanished from
// the plan with no diagnostic at all, which is the silent drop this whole path
// exists to prevent. The plan now states it, naming the key and the release
// that added it, following the precedent the generated-column gate set.
func TestPlanner_ATargetWithoutTheCapabilitySaysSo(t *testing.T) {
	c := qt.New(t)

	sql := planNotNullName(c, "c_old", "c_new", false)

	c.Assert(sql, qt.Contains, "named_not_null_constraints")
	c.Assert(sql, qt.Contains, "c_new")
	c.Assert(sql, qt.Not(qt.Contains), "RENAME CONSTRAINT")
}

// TestPlanner_NothingToRenameFromIsNotARename is the third answer.
//
// The catalog reported no name, so there is no constraint to rename; the
// declared one has to be established with the column's own NOT NULL. Emitting
// a rename here would name a constraint that does not exist.
func TestPlanner_NothingToRenameFromIsNotARename(t *testing.T) {
	c := qt.New(t)

	sql := planNotNullName(c, "", "c_new", true)

	c.Assert(sql, qt.Not(qt.Contains), "RENAME CONSTRAINT")
}

// madeNotNullDiff is one table whose column goes from nullable to NOT NULL under a name
// the comparison asks for.
func madeNotNullDiff(current, desired string) *difftypes.SchemaDiff {
	diff := notNullNameDiff(current, desired)
	column := &diff.TablesModified[0].ColumnsModified[0]
	column.Changes["nullable"] = "true -> false"
	column.Desired = schemamodel.Field{Name: "a", Type: "TEXT", NotNullConstraintName: desired}
	return diff
}

// TestPlanner_AColumnMadeNotNullTakesTheDeclaredName_HappyPath states the name on the
// statement that adds the constraint. The target that keeps no name keeps its comment
// and its SET NOT NULL, and a column that holds a name already has one to rename.
func TestPlanner_AColumnMadeNotNullTakesTheDeclaredName_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		current string
		named   bool
		want    []string
		absent  []string
	}{
		{
			name:   "a target that keeps the name",
			named:  true,
			want:   []string{`ALTER TABLE "widget" ADD CONSTRAINT "c_new" NOT NULL "a";`},
			absent: []string{"SET NOT NULL;", "RENAME CONSTRAINT", "WARNING"},
		},
		{
			name:   "a target that keeps none",
			want:   []string{`ALTER TABLE "widget" ALTER COLUMN "a" SET NOT NULL;`, "WARNING: Column widget.a declares NOT NULL constraint name"},
			absent: []string{"ADD CONSTRAINT", "RENAME CONSTRAINT"},
		},
		{
			name:    "a column that holds a name",
			current: "c_old",
			named:   true,
			want:    []string{`RENAME CONSTRAINT "c_old" TO "c_new"`, `ALTER TABLE "widget" ALTER COLUMN "a" SET NOT NULL;`},
			absent:  []string{"ADD CONSTRAINT"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			caps := capability.Postgres17().With(capability.NamedNotNullConstraints, test.named)

			nodes, err := postgres.NewForDialect(platform.Postgres, caps).GenerateMigrationAST(
				context.Background(), must.Must(builtin.New()), madeNotNullDiff(test.current, "c_new"))
			c.Assert(err, qt.IsNil)
			sql, err := builtin.RenderSQLWithCapabilities(platform.Postgres, caps, nodes...)

			c.Assert(err, qt.IsNil)
			for _, want := range test.want {
				c.Assert(sql, qt.Contains, want)
			}
			for _, absent := range test.absent {
				c.Assert(sql, qt.Not(qt.Contains), absent)
			}
		})
	}
}

// TestPlanner_AnOrdinarySetNotNullStaysAsItIs is the control: with no name to state, the
// comparison reports no change and the statement is SET NOT NULL.
func TestPlanner_AnOrdinarySetNotNullStaysAsItIs(t *testing.T) {
	c := qt.New(t)
	diff := madeNotNullDiff("", "c_new")
	diff.TablesModified[0].ColumnsModified[0].NotNullConstraintNameChange = nil
	caps := capability.Postgres17().With(capability.NamedNotNullConstraints, true)

	nodes, err := postgres.NewForDialect(platform.Postgres, caps).GenerateMigrationAST(
		context.Background(), must.Must(builtin.New()), diff)
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQLWithCapabilities(platform.Postgres, caps, nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `ALTER TABLE "widget" ALTER COLUMN "a" SET NOT NULL;`)
	c.Assert(sql, qt.Not(qt.Contains), "ADD CONSTRAINT")
}

// TestPlanner_AColumnThatStaysNotNullStatesNoName is a document that names a NOT NULL
// beside a column with no name to report, and changes its type. The modification adds no
// constraint, so it states no name, which the pass that frees a name would read as one
// the statement takes.
func TestPlanner_AColumnThatStaysNotNullStatesNoName(t *testing.T) {
	c := qt.New(t)
	diff := madeNotNullDiff("", "c_new")
	column := &diff.TablesModified[0].ColumnsModified[0]
	delete(column.Changes, "nullable")
	column.Changes["type"] = "TEXT -> VARCHAR"
	caps := capability.Postgres17().With(capability.NamedNotNullConstraints, true)

	nodes, err := postgres.NewForDialect(platform.Postgres, caps).GenerateMigrationAST(
		context.Background(), must.Must(builtin.New()), diff)

	c.Assert(err, qt.IsNil)
	c.Assert(modifiedNotNullNames(nodes), qt.DeepEquals, []string{""})
}

// modifiedNotNullNames lists the name each column modification of nodes states.
func modifiedNotNullNames(nodes []ast.Node) []string {
	var names []string
	for _, node := range nodes {
		alter, ok := node.(*ast.AlterTableNode)
		if !ok {
			continue
		}
		for _, operation := range alter.Operations {
			modify, ok := operation.(*ast.ModifyColumnOperation)
			if ok {
				names = append(names, modify.NotNullConstraintName)
			}
		}
	}
	return names
}

// addedKeyColumnDiff adds a primary-key column whose NOT NULL holds name to the declared
// table widget.
func addedKeyColumnDiff(name string) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		DeclaredTables: []schemamodel.Table{{StructName: "Widget", Name: "widget"}},
		TablesModified: []difftypes.TableDiff{{
			TableName: "widget",
			ColumnsAdded: []schemamodel.Field{{
				StructName: "Widget", Name: "a", Type: "INTEGER", Primary: true, NotNullConstraintName: name,
			}},
		}},
	}
}

// TestPlanner_AnAddedKeyColumnKeepsAWrittenName carries a name the server would not give
// the column into ADD COLUMN, where a plan without it lands on the name the server gives
// and takes a second plan to move it. The name the server gives stays out of the statement.
func TestPlanner_AnAddedKeyColumnKeepsAWrittenName(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{name: "c_old", want: `ALTER TABLE "widget" ADD COLUMN "a" INTEGER CONSTRAINT "c_old" NOT NULL, ADD PRIMARY KEY ("a");`},
		{name: "widget_a_not_null", want: `ALTER TABLE "widget" ADD COLUMN "a" INTEGER NOT NULL, ADD PRIMARY KEY ("a");`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			caps := capability.Postgres18()

			nodes, err := postgres.NewForDialect(platform.Postgres, caps).GenerateMigrationAST(
				context.Background(), must.Must(builtin.New()), addedKeyColumnDiff(test.name))
			c.Assert(err, qt.IsNil)
			sql, err := builtin.RenderSQLWithCapabilities(platform.Postgres, caps, nodes...)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}
