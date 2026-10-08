package generator_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaprojection"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

func checkProjectionOptions(c *qt.C, addedFields []schemamodel.Field) generator.BidirectionalSchemaPlanOptions {
	c.Helper()
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	before := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "items", Schema: "public", StructName: "Item"}},
		Fields: []schemamodel.Field{{Name: "id", StructName: "Item", Type: "INTEGER"}},
		Constraints: []schemamodel.Constraint{
			{Name: "kept", StructName: "Item", Type: "CHECK", CheckExpression: "id > 0", NotValid: true, Comment: "old comment"},
			{Name: "gone", StructName: "Item", Type: "CHECK", CheckExpression: "id < 100", Comment: "removed comment"},
		},
	}
	current, err := goschematodb.ToDBSchema(c.Context(), before, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	after := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "items", StructName: "Item"}},
		Fields: append([]schemamodel.Field{{Name: "id", StructName: "Item", Type: "INTEGER"}}, addedFields...),
		Constraints: []schemamodel.Constraint{
			{Name: "kept", StructName: "Item", Type: "CHECK", CheckExpression: "id > 0", Comment: "new comment"},
			{Name: "fresh", StructName: "Item", Type: "CHECK", CheckExpression: "id < 200", NotValid: true, Comment: "new constraint"},
		},
	}
	caps := capability.Postgres18()
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), after, current, catalog.ServerInfo{Dialect: "postgres", Capabilities: caps}, nil, runtime)
	c.Assert(err, qt.IsNil)
	return generator.BidirectionalSchemaPlanOptions{Runtime: runtime, Diff: diff, DesiredSchema: after, CurrentSchema: current, Dialect: "postgres", Capabilities: caps}
}

func TestConstraintProjectionAppliesAcceptedChangesToCapturedHost(t *testing.T) {
	cases := []struct {
		name        string
		fields      []schemamodel.Field
		wantColumns int
	}{
		{name: "constraint-only host", wantColumns: 1},
		{name: "host with accepted column change", fields: []schemamodel.Field{{Name: "note", StructName: "Item", Type: "TEXT", Nullable: true}}, wantColumns: 2},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			opts := checkProjectionOptions(c, test.fields)
			c.Assert(opts.Diff.ConstraintsAdded, qt.HasLen, 1)
			c.Assert(opts.Diff.ConstraintsRemoved, qt.HasLen, 1)
			c.Assert(opts.Diff.ConstraintsValidated, qt.HasLen, 1)
			c.Assert(opts.Diff.ConstraintCommentsChanged, qt.HasLen, 1)
			plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), opts)
			c.Assert(err, qt.IsNil)
			c.Assert(plan.Reverse.Diff.ObservedConstraintHosts, qt.HasLen, 1)
			host := plan.Reverse.Diff.ObservedConstraintHosts[0]
			c.Assert(host.Table.Columns, qt.HasLen, test.wantColumns)
			c.Assert(host.Constraints, qt.HasLen, 2)
			c.Assert(host.Constraints[0].Name, qt.Equals, "kept")
			c.Assert(host.Constraints[0].NotValid, qt.IsFalse)
			c.Assert(host.Constraints[0].Comment, qt.Equals, "new comment")
			c.Assert(host.Constraints[1].Name, qt.Equals, "fresh")
			c.Assert(host.Constraints[1].Schema, qt.Equals, "public")
			c.Assert(host.Constraints[1].NotValid, qt.IsTrue)
			c.Assert(*host.Constraints[1].CheckClause, qt.Equals, "id < 200")
			c.Assert(host.Constraints[1].Comment, qt.Equals, "new constraint")
			*host.Constraints[0].CheckClause = "mutated"
			c.Assert(*opts.CurrentSchema.Constraints[0].CheckClause, qt.Equals, "id > 0")
			c.Assert(*opts.Diff.ObservedConstraintHosts[0].Constraints[0].CheckClause, qt.Equals, "id > 0")
			c.Assert(opts.Diff.ObservedConstraintHosts[0].Constraints[0].NotValid, qt.IsTrue)
			c.Assert(plan.Reverse.Diff.ConstraintsValidated, qt.HasLen, 0)
		})
	}
}

func TestConstraintAttributesAloneCaptureTheHost(t *testing.T) {
	c := qt.New(t)
	opts := checkProjectionOptions(c, nil)
	opts.DesiredSchema.Constraints = opts.DesiredSchema.Constraints[:1]
	opts.CurrentSchema.Constraints = opts.CurrentSchema.Constraints[:1]
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), opts.DesiredSchema, opts.CurrentSchema,
		catalog.ServerInfo{Dialect: "postgres", Capabilities: opts.Capabilities}, nil, opts.Runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
	c.Assert(diff.ObservedConstraintHosts, qt.HasLen, 1)
	opts.Diff = diff
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), opts)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Diff.ObservedConstraintHosts[0].Constraints[0].Comment, qt.Equals, "new comment")
	c.Assert(plan.Reverse.Diff.ObservedConstraintHosts[0].Constraints[0].NotValid, qt.IsFalse)
}

func TestConstraintProjectionRefusesUnresolvableOperands(t *testing.T) {
	cases := []struct {
		name      string
		additions difftypes.ConstraintAdditions
		removals  difftypes.ConstraintRemovals
		comments  []difftypes.ConstraintCommentChange
		validated []difftypes.ConstraintValidation
		captured  []catalog.Constraint
		want      string
	}{
		{name: "missing removal", removals: difftypes.ConstraintRemovals{{Name: "missing", TableName: "items", Type: "CHECK"}}, want: "removal of missing or conflicting constraint"},
		{name: "duplicate removal", removals: difftypes.ConstraintRemovals{{Name: "gone", TableName: "items", Type: "CHECK"}, {Name: "gone", TableName: "items", Type: "CHECK"}}, want: "removal of missing or conflicting constraint"},
		{name: "missing predicate", additions: difftypes.ConstraintAdditions{{Name: "fresh", TableName: "items", Type: "CHECK"}}, want: "incomplete constraint"},
		{name: "duplicate addition", additions: difftypes.ConstraintAdditions{{Name: "kept", TableName: "items", Type: "CHECK", CheckExpression: "id > 1"}}, want: "conflicting addition of constraint"},
		{name: "conflicting identity", removals: difftypes.ConstraintRemovals{{Name: "gone", TableName: "items", Type: "CHECK", Identity: difftypes.ConstraintIdentity{Name: "other"}}}, want: "conflicting identity of constraint"},
		{name: "missing validation", validated: []difftypes.ConstraintValidation{{TableName: "items", Name: "missing"}}, want: "validation of missing constraint"},
		{name: "missing comment", comments: []difftypes.ConstraintCommentChange{{TableName: "items", Name: "missing", Desired: "x"}}, want: "comment of missing constraint"},
		{name: "wrong parent", captured: []catalog.Constraint{{Name: "kept", TableName: "other", Type: "CHECK"}}, want: "constraint .* captured under another table"},
		{name: "duplicate capture", captured: []catalog.Constraint{{Name: "kept", TableName: "items", Type: "CHECK"}, {Name: "kept", TableName: "items", Type: "CHECK"}}, want: "duplicate captured constraint"},
		{name: "unenforced validation", captured: []catalog.Constraint{{Name: "kept", TableName: "items", Type: "CHECK", NotEnforced: true}}, validated: []difftypes.ConstraintValidation{{TableName: "items", Name: "kept"}}, want: "validation of ineligible constraint"},
		{name: "unique validation", captured: []catalog.Constraint{{Name: "kept", TableName: "items", Type: "UNIQUE"}}, validated: []difftypes.ConstraintValidation{{TableName: "items", Name: "kept"}}, want: "validation of ineligible constraint"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			opts := checkProjectionOptions(c, nil)
			opts.Diff.ConstraintsAdded, opts.Diff.ConstraintsRemoved = test.additions, test.removals
			opts.Diff.ConstraintCommentsChanged, opts.Diff.ConstraintsValidated = test.comments, test.validated
			opts.Diff.ObservedConstraintHosts[0].Constraints = projectionTestCapture(test.captured, opts.Diff.ObservedConstraintHosts[0].Constraints)
			plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), opts)
			c.Assert(err, qt.ErrorMatches, ".*cannot project "+test.want+".*")
			c.Assert(plan, qt.IsNil)
		})
	}
}

func projectionTestCapture(override, original []catalog.Constraint) []catalog.Constraint {
	if override != nil {
		return override
	}
	return original
}

func TestConstraintReplacementProjectsNewDefinitionAndRestoresOldOne(t *testing.T) {
	c := qt.New(t)
	opts := checkProjectionOptions(c, nil)
	opts.Diff.ConstraintsAdded[0].Name = "gone"
	opts.Diff.ConstraintsAdded[0].Identity = opts.Diff.ConstraintsRemoved[0].Identity
	opts.Diff.ConstraintsAdded[0].Columns = []string{"id"}
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), opts)
	c.Assert(err, qt.IsNil)
	projected := plan.Reverse.Diff.ObservedConstraintHosts[0].Constraints[1]
	c.Assert(projected.Name, qt.Equals, "gone")
	c.Assert(projected.ColumnName, qt.Equals, "id")
	c.Assert(projected.ColumnNames, qt.DeepEquals, []string{"id"})
	c.Assert(*projected.CheckClause, qt.Equals, "id < 200")
	c.Assert(projected.Comment, qt.Equals, "new constraint")
	c.Assert(plan.Reverse.Diff.ConstraintsAdded, qt.HasLen, 1)
	c.Assert(plan.Reverse.Diff.ConstraintsAdded[0].CheckExpression, qt.Equals, "id < 100")
	c.Assert(plan.Reverse.Diff.ConstraintsAdded[0].Comment, qt.Equals, "removed comment")
}

func TestColumnCapturesOwnSourceAndReverseOperands(t *testing.T) {
	c := qt.New(t)
	opts := checkProjectionOptions(c, []schemamodel.Field{{
		Name: "note", StructName: "Item", Type: "TEXT", Nullable: true,
		Overrides: map[string]map[string]string{"sqlite": {"type": "TEXT"}},
	}})
	opts.DesiredSchema.Fields[1].Overrides["sqlite"]["type"] = "source mutation"
	c.Assert(opts.Diff.TablesModified[0].ColumnsAdded[0].Overrides["sqlite"]["type"], qt.Equals, "TEXT")
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), opts)
	c.Assert(err, qt.IsNil)
	plan.Reverse.Diff.TablesModified[0].ColumnsRemoved[0].Overrides["sqlite"]["type"] = "reverse mutation"
	c.Assert(opts.Diff.TablesModified[0].ColumnsAdded[0].Overrides["sqlite"]["type"], qt.Equals, "TEXT")
}

func TestGeneratorUsesPostgresConstraintEffectsInCombinedCapture(t *testing.T) {
	c := qt.New(t)
	opts := keyProjectionOptions(c)
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), opts)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Diff.ObservedConstraintHosts, qt.HasLen, 1)
	host := plan.Reverse.Diff.ObservedConstraintHosts[0]
	c.Assert(host.Table.Columns, qt.HasLen, 2)
	c.Assert(host.Table.Columns[0].IsUnique, qt.IsTrue)
	c.Assert(host.Indexes, qt.HasLen, 1)
	c.Assert(host.Indexes[0].Name, qt.Equals, "unique_id")
	c.Assert(host.Indexes[0].IncludeColumns, qt.DeepEquals, []string{"note"})
	c.Assert(*host.Indexes[0].NullsDistinct, qt.IsFalse)
	c.Assert(opts.CurrentSchema.Indexes, qt.HasLen, 0)
	c.Assert(opts.CurrentSchema.Tables[0].Columns[0].IsUnique, qt.IsFalse)
}

func keyProjectionOptions(c *qt.C) generator.BidirectionalSchemaPlanOptions {
	c.Helper()
	opts := checkProjectionOptions(c, []schemamodel.Field{{Name: "note", StructName: "Item", Type: "TEXT", Nullable: true}})
	opts.DesiredSchema.Constraints = append(opts.DesiredSchema.Constraints, schemamodel.Constraint{
		Name: "unique_id", StructName: "Item", Type: "UNIQUE", Columns: []string{"id"}, IncludeColumns: []string{"note"}, NullsDistinct: new(false),
	})
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), opts.DesiredSchema, opts.CurrentSchema,
		catalog.ServerInfo{Dialect: "postgres", Capabilities: opts.Capabilities}, nil, opts.Runtime)
	c.Assert(err, qt.IsNil)
	opts.Diff = diff
	return opts
}

type invalidConstraintProjection struct {
	generator.Runtime
	result schemaprojection.ConstraintResult
}

func (r invalidConstraintProjection) ProjectConstraints(context.Context, schemaprojection.ConstraintRequest) (schemaprojection.ConstraintResult, error) {
	return r.result, nil
}

func TestGeneratorRefusesMalformedConstraintProjection(t *testing.T) {
	cases := []struct {
		name   string
		result schemaprojection.ConstraintResult
	}{
		{name: "missing outcome"},
		{name: "whitespace reason", result: schemaprojection.ConstraintResult{Unavailable: " \t"}},
		{name: "state and unavailable", result: schemaprojection.ConstraintResult{State: &schemaprojection.TableState{}, Unavailable: "missing evidence"}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			opts := keyProjectionOptions(c)
			opts.Runtime = invalidConstraintProjection{Runtime: opts.Runtime, result: test.result}
			_, err := generator.PlanBidirectionalSchemaDiff(t.Context(), opts)
			c.Assert(err, qt.ErrorIs, schemaprojection.ErrInvalid)
		})
	}
}
