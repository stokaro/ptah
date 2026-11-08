package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// addedColumnOptions plans adding the fields to items, a table that holds id
// alone, beside owners, on PostgreSQL. constraints are the table-level
// constraints the desired state declares on items.
func addedColumnOptions(c *qt.C, fields []schemamodel.Field, constraints []schemamodel.Constraint) generator.BidirectionalSchemaPlanOptions {
	c.Helper()
	runtime := must.Must(builtin.New())
	tables := []schemamodel.Table{{Name: "items", Schema: "public", StructName: "Item"}, {Name: "owners", Schema: "public", StructName: "Owner"}}
	held := []schemamodel.Field{{Name: "id", StructName: "Item", Type: "INTEGER", Primary: true}, {Name: "id", StructName: "Owner", Type: "INTEGER", Primary: true}}
	current, err := goschematodb.ToDBSchema(c.Context(), &schemamodel.Database{Tables: tables, Fields: held}, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	desired := &schemamodel.Database{Tables: tables, Fields: append(held, fields...), Constraints: constraints}
	caps := capability.Postgres18()
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current, catalog.ServerInfo{Dialect: "postgres", Capabilities: caps}, nil, runtime)
	c.Assert(err, qt.IsNil)
	return generator.BidirectionalSchemaPlanOptions{Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: "postgres", Capabilities: caps}
}

// The table a reverse plan starts from holds the CHECK and the foreign key
// that the columns the forward plan adds state on themselves. The comparison
// lists neither among its constraint additions, since adding the column adds
// them, so a capture built from the diff's constraint changes alone shows the
// column without them, and a provider that reads it drops the column without
// first dropping a constraint that names it. The foreign key reaches the
// target's projector, which states its rules and its referenced schema.
func TestReverseCaptureOfAddedColumnConstraints_HappyPath(t *testing.T) {
	c := qt.New(t)
	opts := addedColumnOptions(c, []schemamodel.Field{
		{Name: "score", StructName: "Item", Type: "INTEGER", Nullable: true, Check: "score >= 0"},
		{Name: "owner_id", StructName: "Item", Type: "INTEGER", Nullable: true, Foreign: "owners(id)"},
	}, nil)
	c.Assert(opts.Diff.ConstraintsAdded, qt.HasLen, 0)

	plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), opts)

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Diff.TablesModified, qt.HasLen, 1)
	captured := plan.Reverse.Diff.TablesModified[0].Current
	c.Assert(captured.Table.Columns, qt.HasLen, 3)
	c.Assert(captured.Constraints, qt.HasLen, 2)
	check, key := captured.Constraints[0], captured.Constraints[1]
	c.Assert(check.Name, qt.Equals, "items_score_check")
	c.Assert(check.Type, qt.Equals, "CHECK")
	c.Assert(check.QualifiedTableName(), qt.Equals, "public.items")
	c.Assert(*check.CheckClause, qt.Equals, "score >= 0")
	c.Assert(check.ColumnNames, qt.DeepEquals, []string{"score"})
	c.Assert(key.Name, qt.Equals, "fk_items_owner_id")
	c.Assert(key.Type, qt.Equals, "FOREIGN KEY")
	c.Assert(key.QualifiedTableName(), qt.Equals, "public.items")
	c.Assert(key.ColumnNames, qt.DeepEquals, []string{"owner_id"})
	c.Assert(*key.ForeignTable, qt.Equals, "owners")
	c.Assert(key.ForeignSchema, qt.Equals, "public")
	c.Assert(key.ForeignColumns, qt.DeepEquals, []string{"id"})
	c.Assert(*key.DeleteRule, qt.Equals, "NO ACTION")
	c.Assert(*key.UpdateRule, qt.Equals, "NO ACTION")
	c.Assert(opts.CurrentSchema.Constraints, qt.HasLen, 0)
}

// A column's CHECK named like a constraint the table change adds is two
// constraints under one name, which no read reports, so the reverse plan has
// no table to start from and the plan is refused.
func TestReverseCaptureOfAddedColumnConstraints_FailurePath(t *testing.T) {
	c := qt.New(t)
	opts := addedColumnOptions(c, []schemamodel.Field{
		{Name: "score", StructName: "Item", Type: "INTEGER", Nullable: true, Check: "score >= 0", CheckName: "items_bounds"},
	}, []schemamodel.Constraint{{Name: "items_bounds", StructName: "Item", Type: "CHECK", CheckExpression: "id > 0"}})
	c.Assert(opts.Diff.ConstraintsAdded, qt.HasLen, 1)

	plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), opts)

	c.Assert(err, qt.ErrorMatches, `.*cannot project conflicting addition of constraint "items_bounds".*`)
	c.Assert(plan, qt.IsNil)
}
