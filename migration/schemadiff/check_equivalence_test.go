package schemadiff_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine/builtin"
	"ptah.run/internal/schemaprep"
	"ptah.run/migration/schemadiff"
)

// checkRuntime is a target whose comparison rules decide CHECK equivalence
// with checks, and whose lowering builds an enum column as a TEXT column under
// the CHECK `<column> IN (...)`, as a provider engine that states that form
// (schema.enum-columns) has the host build it.
type checkRuntime struct {
	*selectedProbeRuntime
	checks schemacomparison.Checks
}

func (r checkRuntime) ComparisonRules(string) (schemacomparison.Rules, error) {
	return schemacomparison.Rules{Checks: r.checks}, nil
}

// ValidateSchema accepts every declaration, so the comparison runs to its end.
func (checkRuntime) ValidateSchema(context.Context, schemavalidation.Request) (schemavalidation.Result, error) {
	return schemavalidation.Result{Complete: true}, nil
}

func (r checkRuntime) LowerDesired(ctx context.Context, request schemapreparation.LoweringRequest) (*schemamodel.Database, error) {
	desired, err := r.selectedProbeRuntime.LowerDesired(ctx, request)
	if err != nil {
		return nil, err
	}
	lowered := *desired
	lowered.Fields = schemaprep.LowerEnumColumns(desired.Fields, desired.Enums, schemaprep.EnumForm{Style: descriptor.EnumCheck, CheckType: "TEXT"})
	lowered.Enums = nil
	return &lowered, nil
}

// recordedChecks records each request and answers every pair with
// equivalent, or fails with err.
type recordedChecks struct {
	requests   []schemacomparison.ChecksRequest
	equivalent bool
	err        error
}

func (r *recordedChecks) EquivalentChecks(_ context.Context, request schemacomparison.ChecksRequest) ([]bool, error) {
	r.requests = append(r.requests, request)
	if r.err != nil {
		return nil, r.err
	}
	answers := make([]bool, len(request.Pairs))
	for i := range answers {
		answers[i] = r.equivalent
	}
	return answers, nil
}

// checkSchemas declare items.score with the CHECK items_score_check, which
// the database holds as written by its server, beside a held CHECK nothing
// declares.
func checkSchemas() (*schemamodel.Database, *catalog.Database) {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "items", StructName: "Item"}},
		Fields: []schemamodel.Field{{StructName: "Item", Name: "id", Type: "INTEGER"},
			{StructName: "Item", Name: "score", Type: "INTEGER", Check: "score >= 0", CheckName: "items_score_check"}},
	}, &catalog.Database{
		Tables: []catalog.Table{{Name: "items", Columns: []catalog.Column{{Name: "id", DataType: "INTEGER"}, {Name: "score", DataType: "INTEGER"}}}},
		Constraints: []catalog.Constraint{
			{Name: "items_score_check", TableName: "items", Type: "CHECK", CheckClause: new("(score >= '0')")},
			{Name: "items_id_check", TableName: "items", Type: "CHECK", CheckClause: new("(id > 0)")},
		},
	}
}

// enumSchemas declare items.kind as an enum column of the values a, b and c,
// and the database holds the column as TEXT under the CHECK items_kind_check
// of the values a and b, in the spelling Spanner keeps.
func enumSchemas() (*schemamodel.Database, *catalog.Database) {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "items", StructName: "Item"}},
		Fields: []schemamodel.Field{{StructName: "Item", Name: "id", Type: "INTEGER"},
			{StructName: "Item", Name: "kind", Type: "ENUM", Enum: []string{"a", "b", "c"}}},
	}, &catalog.Database{
		Tables: []catalog.Table{{Name: "items", Columns: []catalog.Column{{Name: "id", DataType: "INTEGER"}, {Name: "kind", DataType: "TEXT"}}}},
		Constraints: []catalog.Constraint{
			{Name: "items_kind_check", TableName: "items", Type: "CHECK", CheckClause: new("(kind = ANY (ARRAY['a'::text, 'b'::text]))")},
		},
	}
}

// checkComparison compares desired with current for a target that decides
// CHECK equivalence with checks.
func checkComparison(c *qt.C, checks schemacomparison.Checks, desired *schemamodel.Database, current *catalog.Database) (*difftypes.SchemaDiff, error) {
	c.Helper()
	embedded := must.Must(builtin.New())
	runtime := checkRuntime{selectedProbeRuntime: &selectedProbeRuntime{TargetRuntime: embedded, NormalizationService: embedded,
		FacetNormalizationService: embedded}, checks: checks}
	return schemadiff.CompareWithDatabase(c.Context(), probeComparisonSession{dialect: "sqlite"}, desired, current, nil, runtime)
}

// addedChecks are the CHECKs diff adds, each as its name and expression.
func addedChecks(diff *difftypes.SchemaDiff) []string {
	var added []string
	for _, constraint := range diff.ConstraintsAdded {
		added = append(added, constraint.Name+": "+constraint.CheckExpression)
	}
	return added
}

// A target that decides CHECK equivalence is asked about each declared CHECK
// the database holds, with the held CHECK and the declared expression, and
// about no CHECK only one side has. Its answer decides: a CHECK it calls
// another constraint is dropped and added, one it calls the same is kept.
func TestCompareWithDatabaseAsksTheTargetAboutChecks_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		equivalent  bool
		wantRemoved []string
		wantAdded   []string
	}{
		{name: "the target calls them one constraint", equivalent: true, wantRemoved: []string{"items_id_check"}},
		{name: "the target calls them two constraints", equivalent: false, wantRemoved: []string{"items_id_check", "items_score_check"},
			wantAdded: []string{"items_score_check: score >= 0"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			checks := &recordedChecks{equivalent: test.equivalent}
			desired, current := checkSchemas()

			diff, err := checkComparison(c, checks, desired, current)

			c.Assert(err, qt.IsNil)
			c.Assert(checks.requests, qt.HasLen, 1)
			request := checks.requests[0]
			c.Assert(request.Target, qt.Equals, "sqlite")
			c.Assert(request.Pairs, qt.HasLen, 1)
			c.Assert(request.Pairs[0].Held.Name, qt.Equals, "items_score_check")
			c.Assert(*request.Pairs[0].Held.CheckClause, qt.Equals, "(score >= '0')")
			c.Assert(request.Pairs[0].Declared, qt.Equals, "score >= 0")
			c.Assert(request.Current.Constraints, qt.HasLen, 2)
			c.Assert(diff.ConstraintsRemoved.Names(), qt.DeepEquals, test.wantRemoved)
			c.Assert(addedChecks(diff), qt.DeepEquals, test.wantAdded)
		})
	}
}

// The CHECK a target's lowering builds for an enum column is one the target
// is asked about, so a changed list of values replaces the CHECK and an
// unchanged one keeps it. Asked about the declaration before the lowering,
// the target never saw the CHECK, and the comparison read the held
// `= ANY (ARRAY[...])` against the declared IN list as a rewrite it cannot
// judge and planned nothing for the new value.
func TestCompareWithDatabaseAsksTheTargetAboutLoweredEnumChecks_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		equivalent  bool
		wantRemoved []string
		wantAdded   []string
	}{
		{name: "the values are the held ones", equivalent: true},
		{name: "the values changed", equivalent: false, wantRemoved: []string{"items_kind_check"},
			wantAdded: []string{"items_kind_check: kind IN ('a', 'b', 'c')"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			checks := &recordedChecks{equivalent: test.equivalent}
			desired, current := enumSchemas()

			diff, err := checkComparison(c, checks, desired, current)

			c.Assert(err, qt.IsNil)
			c.Assert(checks.requests, qt.HasLen, 1)
			c.Assert(checks.requests[0].Pairs, qt.HasLen, 1)
			c.Assert(checks.requests[0].Pairs[0].Held.Name, qt.Equals, "items_kind_check")
			c.Assert(checks.requests[0].Pairs[0].Declared, qt.Equals, "kind IN ('a', 'b', 'c')")
			c.Assert(diff.ConstraintsRemoved.Names(), qt.DeepEquals, test.wantRemoved)
			c.Assert(addedChecks(diff), qt.DeepEquals, test.wantAdded)
		})
	}
}

// A target that cannot answer fails the comparison with its error.
func TestCompareWithDatabaseAsksTheTargetAboutChecks_FailurePath(t *testing.T) {
	c := qt.New(t)
	refused := errors.New("the target cannot read this CHECK")
	desired, current := checkSchemas()

	diff, err := checkComparison(c, &recordedChecks{err: refused}, desired, current)

	c.Assert(err, qt.ErrorIs, refused)
	c.Assert(err, qt.ErrorMatches, `compare schemas: the target cannot read this CHECK`)
	c.Assert(diff, qt.IsNil)
}
