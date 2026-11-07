package schemadiff_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// checkRuntime is a target whose comparison rules decide CHECK equivalence
// with checks. Its validation stops the comparison after the probes.
type checkRuntime struct {
	*selectedProbeRuntime
	checks schemacomparison.Checks
}

func (r checkRuntime) ComparisonRules(string) (schemacomparison.Rules, error) {
	return schemacomparison.Rules{Checks: r.checks}, nil
}

// recordedChecks records each request and answers every pair as equivalent,
// or fails with err.
type recordedChecks struct {
	requests []schemacomparison.ChecksRequest
	err      error
}

func (r *recordedChecks) EquivalentChecks(_ context.Context, request schemacomparison.ChecksRequest) ([]bool, error) {
	r.requests = append(r.requests, request)
	if r.err != nil {
		return nil, r.err
	}
	return make([]bool, len(request.Pairs)), nil
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

// errCheckValidation stops a comparison at its validation, after the probes.
var errCheckValidation = errors.New("validation reached")

// checkComparison runs a comparison of checkSchemas whose target decides CHECK
// equivalence with checks, up to its validation, and returns its error.
func checkComparison(c *qt.C, checks schemacomparison.Checks) error {
	c.Helper()
	desired, current := checkSchemas()
	embedded := must.Must(builtin.New())
	runtime := checkRuntime{selectedProbeRuntime: &selectedProbeRuntime{TargetRuntime: embedded, NormalizationService: embedded,
		FacetNormalizationService: embedded, stop: errCheckValidation}, checks: checks}
	diff, err := schemadiff.CompareWithDatabase(c.Context(), probeComparisonSession{dialect: "sqlite"}, desired, current, nil, runtime)
	c.Assert(diff, qt.IsNil)
	return err
}

// A target that decides CHECK equivalence is asked about each declared CHECK
// the database holds, with the held CHECK and the declared expression, and
// about no CHECK only one side has.
func TestCompareWithDatabaseAsksTheTargetAboutChecks_HappyPath(t *testing.T) {
	c := qt.New(t)
	checks := &recordedChecks{}

	err := checkComparison(c, checks)

	c.Assert(err, qt.ErrorIs, errCheckValidation)
	c.Assert(checks.requests, qt.HasLen, 1)
	request := checks.requests[0]
	c.Assert(request.Target, qt.Equals, "sqlite")
	c.Assert(request.Pairs, qt.HasLen, 1)
	c.Assert(request.Pairs[0].Held.Name, qt.Equals, "items_score_check")
	c.Assert(*request.Pairs[0].Held.CheckClause, qt.Equals, "(score >= '0')")
	c.Assert(request.Pairs[0].Declared, qt.Equals, "score >= 0")
	c.Assert(request.Current.Constraints, qt.HasLen, 2)
}

// A target that cannot answer fails the comparison with its error.
func TestCompareWithDatabaseAsksTheTargetAboutChecks_FailurePath(t *testing.T) {
	c := qt.New(t)
	refused := errors.New("the target cannot read this CHECK")

	err := checkComparison(c, &recordedChecks{err: refused})

	c.Assert(err, qt.ErrorIs, refused)
	c.Assert(err, qt.ErrorMatches, `compare schemas: the target cannot read this CHECK`)
}
