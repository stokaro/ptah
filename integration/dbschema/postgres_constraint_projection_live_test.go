//go:build integration

package dbschema_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// The catalog, projected reverse input, and executed rollback must agree when
// a table changes its columns, keys, CHECK definitions, validation, and comments.
func TestPostgresLiveConstraintProjectionAndRollback(t *testing.T) {
	c := qt.New(t)
	conn := returnTypeConnection(c, dbtarget.URL(t, dbtarget.PostgreSQL))
	schema := returnTypeSchema(c, conn, "ptah_constraint_projection")
	before := &schemamodel.Database{
		Tables:  []schemamodel.Table{{Name: "items", Schema: schema, StructName: "Item"}},
		Fields:  []schemamodel.Field{{Name: "id", StructName: "Item", Type: "INTEGER"}},
		Indexes: []schemamodel.Index{{Name: "retained", StructName: "Item", Fields: []string{"id"}, Comment: "independent index"}},
		Constraints: []schemamodel.Constraint{
			{Name: "gone", StructName: "Item", Type: "CHECK", CheckExpression: "id < 100", Comment: "removed comment"},
		},
	}
	applyDeclaration(c, conn, before)
	applyStatements(c, conn, []string{
		`INSERT INTO "` + schema + `".items (id) VALUES (1)`,
		`ALTER TABLE "` + schema + `".items ADD CONSTRAINT kept CHECK (id > 0) NOT VALID`,
		`COMMENT ON CONSTRAINT kept ON "` + schema + `".items IS 'old comment'`,
	})
	current, err := dbschema.ReadSchemaWithSchemasContext(t.Context(), conn, []string{schema})
	c.Assert(err, qt.IsNil)
	c.Assert(constraintProjectionAttributes(current.Constraints), qt.DeepEquals, map[string]constraintProjectionAttribute{
		"kept": {NotValid: true, Comment: "old comment"}, "gone": {Comment: "removed comment"},
	})
	after := &schemamodel.Database{
		Tables:  before.Tables,
		Indexes: before.Indexes,
		Fields:  append([]schemamodel.Field{before.Fields[0]}, schemamodel.Field{Name: "note", StructName: "Item", Type: "TEXT", Nullable: true}),
		Constraints: []schemamodel.Constraint{
			{Name: "kept", StructName: "Item", Type: "CHECK", CheckExpression: "id > 0", Comment: "new comment"},
			{Name: "fresh", StructName: "Item", Type: "CHECK", CheckExpression: "id < 200", NotValid: true, Comment: "new constraint"},
			{Name: "unique_id", StructName: "Item", Type: "UNIQUE", Columns: []string{"id"}, IncludeColumns: []string{"note"}, NullsDistinct: new(false)},
			{Name: "primary_id", StructName: "Item", Type: "PRIMARY KEY", Columns: []string{"id"}},
			{Name: "self_fk", StructName: "Item", Type: "FOREIGN KEY", Columns: []string{"id"}, ForeignTable: schema + ".items", ForeignColumns: []string{"id"}, Deferrable: true, Initially: "deferred", NotValid: true},
		},
	}
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), after, current, conn.Info(), nil, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 4)
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 1)
	c.Assert(diff.ConstraintsValidated, qt.HasLen, 1)
	c.Assert(diff.ConstraintCommentsChanged, qt.HasLen, 1)
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: after, CurrentSchema: current, Dialect: "postgres", Capabilities: conn.Info().Capabilities,
	})
	c.Assert(err, qt.IsNil)
	forward, err := builtin.RenderSQLWithCapabilities("postgres", conn.Info().Capabilities, plan.Forward.Nodes...)
	c.Assert(err, qt.IsNil)
	reverse, err := builtin.RenderSQLWithCapabilities("postgres", conn.Info().Capabilities, plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	applyStatements(c, conn, []string{forward})
	live, err := dbschema.ReadSchemaWithSchemasContext(t.Context(), conn, []string{schema})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Diff.ObservedConstraintHosts, qt.HasLen, 1)
	host := plan.Reverse.Diff.ObservedConstraintHosts[0]
	c.Assert(host.Table.Columns, qt.HasLen, 2)
	c.Assert(host.Table.Columns[0].IsPrimaryKey, qt.IsTrue)
	c.Assert(host.Table.Columns[0].IsUnique, qt.IsTrue)
	c.Assert(host.Table.Columns[0].NotNullConstraintName, qt.Equals, current.Tables[0].Columns[0].NotNullConstraintName)
	c.Assert(projectedIndexAttributes(host.Indexes), qt.DeepEquals, projectedIndexAttributes(live.Indexes))
	c.Assert(projectedKeyConstraints(host.Constraints), qt.DeepEquals, projectedKeyConstraints(live.Constraints))
	c.Assert(constraintProjectionAttributes(host.Constraints), qt.DeepEquals, constraintProjectionAttributes(live.Constraints))
	settled, err := schemadiff.CompareWithDatabaseInfo(t.Context(), after, live, conn.Info(), nil, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(settled.HasChanges(), qt.IsFalse)
	// Re-plan from actual key state to exercise removal effects independently
	// of the first plan's predicted capture.
	before.Constraints = append(before.Constraints, schemamodel.Constraint{Name: "kept", StructName: "Item", Type: "CHECK", CheckExpression: "id > 0", Comment: "old comment"})
	removal, err := schemadiff.CompareWithDatabaseInfo(t.Context(), before, live, conn.Info(), nil, runtime)
	c.Assert(err, qt.IsNil)
	removalPlan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: removal, DesiredSchema: before, CurrentSchema: live, Dialect: "postgres", Capabilities: conn.Info().Capabilities,
	})
	c.Assert(err, qt.IsNil)
	removedHost := removalPlan.Reverse.Diff.ObservedConstraintHosts[0]
	c.Assert(projectedIndexAttributes(removedHost.Indexes), qt.DeepEquals, projectedIndexAttributes(current.Indexes))
	c.Assert(removedHost.Table.Columns[0].IsPrimaryKey, qt.IsFalse)
	c.Assert(removedHost.Table.Columns[0].IsUnique, qt.IsFalse)
	c.Assert(removedHost.Table.Columns[0].IsNullable, qt.Equals, "NO")
	applyStatements(c, conn, []string{reverse})
	restored, err := dbschema.ReadSchemaWithSchemasContext(t.Context(), conn, []string{schema})
	c.Assert(err, qt.IsNil)
	// Validation remains true: PostgreSQL has no inverse VALIDATE operation.
	c.Assert(projectedIndexAttributes(restored.Indexes), qt.DeepEquals, projectedIndexAttributes(current.Indexes))
	settled, err = schemadiff.CompareWithDatabaseInfo(t.Context(), before, restored, conn.Info(), nil, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(settled.HasChanges(), qt.IsFalse)
	c.Assert(constraintProjectionAttributes(restored.Constraints), qt.DeepEquals, map[string]constraintProjectionAttribute{
		"kept": {Comment: "old comment"}, "gone": {Comment: "removed comment"},
	})
}

// CHECK expression text is rewritten by the server; key constraints have no
// expression text and their entire captured definition must match.
func projectedKeyConstraints(constraints []catalog.Constraint) []catalog.Constraint {
	result := slices.DeleteFunc(slices.Clone(constraints), func(constraint catalog.Constraint) bool {
		return constraint.Type == "CHECK"
	})
	slices.SortFunc(result, func(left, right catalog.Constraint) int { return strings.Compare(left.Name, right.Name) })
	return result
}

// Raw SQL is a reader's presentation; projections retain structural index data.
func projectedIndexAttributes(indexes []catalog.Index) []catalog.Index {
	result := slices.Clone(indexes)
	for i := range result {
		result[i].Definition = ""
	}
	slices.SortFunc(result, func(left, right catalog.Index) int { return strings.Compare(left.Name, right.Name) })
	return result
}

type constraintProjectionAttribute struct {
	NotValid bool
	Comment  string
}

func constraintProjectionAttributes(constraints []catalog.Constraint) map[string]constraintProjectionAttribute {
	attributes := make(map[string]constraintProjectionAttribute, len(constraints))
	for _, constraint := range constraints {
		attributes[constraint.Name] = constraintProjectionAttribute{NotValid: constraint.NotValid, Comment: constraint.Comment}
	}
	return attributes
}
