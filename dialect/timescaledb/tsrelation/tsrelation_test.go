package tsrelation_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsrelation"
	"ptah.run/dialect/timescaledb/tsschema"
)

// TestDescribeRelations_NamesTheHypertableAnAggregateReads pins the edge a
// planner orders by: an observed aggregate depends on the hypertable the
// catalog names, and says that list is not exhaustive. A declared aggregate's
// body is not parsed, so it names nothing and says so; a hypertable facet
// depends only on its own table.
func TestDescribeRelations_NamesTheHypertableAnAggregateReads(t *testing.T) {
	builder := objectidentity.NewBuilder(identifier.ForDialect("postgres"))
	aggregate := schemaext.RelationSubject{Kind: tsschema.ContinuousAggregateKind, Placement: schemaext.ObjectPlacement, Subject: tsschema.ContinuousAggregateRef("", "hourly")}
	facet := schemaext.RelationSubject{Kind: tsschema.HypertableKind, Placement: schemaext.FacetPlacement, Subject: builder.TableParts("", "readings")}
	tests := []struct {
		name         string
		value        schemaext.RelationValue
		wantComplete bool
		wantDeps     []objectidentity.ID
	}{
		{name: "an observed aggregate", wantDeps: []objectidentity.ID{builder.TableParts("metrics", "readings")},
			value: schemaext.RelationValue{Subject: aggregate, Value: &tsschema.ObservedContinuousAggregate{Definition: "SELECT 1", HypertableSchema: "metrics", HypertableName: "readings"}}},
		{name: "a declared aggregate",
			value: schemaext.RelationValue{Subject: aggregate, Value: &tsschema.DesiredContinuousAggregate{Body: "SELECT 1"}}},
		{name: "a hypertable", wantComplete: true,
			value: schemaext.RelationValue{Subject: facet, Value: &tsschema.ObservedHypertable{Column: "time", Dimensions: 1}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := tsrelation.Service{}.DescribeRelations(t.Context(), schemaext.RelationRequest{
				Target: "postgres", Identifiers: identifier.ForDialect("postgres"), Values: []schemaext.RelationValue{test.value},
			})

			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Values, qt.HasLen, 1)
			c.Assert(result.Values[0].Complete, qt.Equals, test.wantComplete)
			c.Assert(result.Values[0].Dependencies, qt.DeepEquals, test.wantDeps)
		})
	}
}

// TestDescribeRelations_FailurePath pins the refusals: another target family
// and a value of another owner.
func TestDescribeRelations_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		request schemaext.RelationRequest
		want    error
	}{
		{name: "another target", request: schemaext.RelationRequest{Target: "clickhouse"}, want: ptaherr.ErrUnsupportedDialect},
		{name: "a value of another owner", request: schemaext.RelationRequest{Target: "postgres", Values: []schemaext.RelationValue{{Value: nil}}}, want: schemaext.ErrInvalidValue},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := tsrelation.Service{}.DescribeRelations(t.Context(), test.request)

			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result.Values, qt.IsNil)
		})
	}
}
