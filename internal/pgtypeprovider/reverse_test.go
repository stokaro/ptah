package pgtypeprovider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
	"ptah.run/feature/pgtype/typereport"
	"ptah.run/feature/pgtype/typereverse"
)

func reversing(changes ...schemaext.ChangeRecord) schemaext.ReversalRequest {
	return schemaext.ReversalRequest{Target: "postgres", Identifiers: identifier.ForDialect("postgres"), Capabilities: capability.Postgres17(),
		Changes: changes}
}

// TestReverseTypes pins each inverse: a creation drops what it created, a drop
// creates the type again as it was, and a surviving type goes back to its
// observation, an attribute delta simulated again over the old list.
func TestReverseTypes(t *testing.T) {
	text := &pgtype.ObservedDomain{BaseType: "text", Default: "'x'::text", Comment: "mail"}
	integer := &pgtype.DesiredDomain{BaseType: "integer", NotNull: true}
	checked := &pgtype.ObservedDomain{BaseType: "text", Check: "(VALUE <> ''::text)",
		Checks: []pgtype.DomainCheck{{Name: "t_check", Expression: "(VALUE <> ''::text)"}}}
	otherCheck := &pgtype.DesiredDomain{BaseType: "text", Check: "VALUE <> 'a'"}
	a := pgtype.Attribute{Name: "a", Type: "int"}
	b := pgtype.Attribute{Name: "b", Type: "text"}
	cc := pgtype.Attribute{Name: "c", Type: "text"}
	abc := &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{a, b, cc}}
	ac := &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{a, cc}}
	ab := &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{a, b}}
	abcDeclared := &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{a, b, cc}}
	numeric := &pgtype.ObservedRange{Subtype: "numeric", SubtypeOpClass: "numeric_ops"}
	bigints := &pgtype.DesiredRange{Subtype: "int8"}
	tests := []struct {
		name        string
		change      schemaext.ChangeRecord
		want        schemaext.ChangeValue
		forward     schemaext.Value
		strategy    string
		limitations []string
	}{
		{name: "a domain created", change: typeChange(pgtype.DomainKind, "t", &pgtype.DomainChange{After: integer}),
			want: &pgtype.DomainChange{Before: integer.Observed()}, forward: integer.Observed(), strategy: "drop the domain the forward plan created"},
		{name: "a domain dropped", change: typeChange(pgtype.DomainKind, "t", &pgtype.DomainChange{Before: text}),
			want: &pgtype.DomainChange{After: text.Desired()}, strategy: "create the domain again as it was"},
		{name: "a domain rebuilt", change: typeChange(pgtype.DomainKind, "t", &pgtype.DomainChange{Before: text, After: integer,
			Differs: []string{pgtype.DiffBaseType, pgtype.DiffComment, pgtype.DiffNotNull}}),
			want: &pgtype.DomainChange{Before: integer.Observed(), After: text.Desired(), Differs: []string{pgtype.DiffBaseType, pgtype.DiffComment,
				pgtype.DiffNotNull}}, forward: integer.Observed(), strategy: "change the domain back to what it was"},
		{name: "a domain's CHECK", change: typeChange(pgtype.DomainKind, "t", &pgtype.DomainChange{Before: checked, After: otherCheck,
			Differs: []string{pgtype.DiffCheck}}),
			want:    &pgtype.DomainChange{Before: otherCheck.Observed(), After: checked.Desired(), Differs: []string{pgtype.DiffCheck}},
			forward: otherCheck.Observed(), strategy: "change the domain back to what it was",
			limitations: []string{"the rollback adds the CHECK domain t had, but cannot drop the one the migration added: " +
				"the server named it, and the name is not recorded"}},
		{name: "a composite type's middle attribute dropped", change: typeChange(pgtype.CompositeKind, "t", &pgtype.CompositeChange{
			Before: abc, After: ac, Differs: []string{pgtype.DiffAttributes}, Removed: []string{"b"}}),
			want:    &pgtype.CompositeChange{Before: ac.Observed(), After: abc.Desired(), Differs: []string{pgtype.DiffAttributes}},
			forward: ac.Observed(), strategy: "change the composite type back to what it was"},
		{name: "a composite type's last attribute added", change: typeChange(pgtype.CompositeKind, "t", &pgtype.CompositeChange{
			Before: ab, After: abcDeclared, Differs: []string{pgtype.DiffAttributes}, Added: []pgtype.Attribute{cc}}),
			want: &pgtype.CompositeChange{Before: abcDeclared.Observed(), After: ab.Desired(), Differs: []string{pgtype.DiffAttributes},
				Removed: []string{"c"}}, forward: abcDeclared.Observed(), strategy: "change the composite type back to what it was"},
		{name: "a range type created", change: typeChange(pgtype.RangeKind, "t", &pgtype.RangeChange{After: bigints}),
			want: &pgtype.RangeChange{Before: bigints.Observed()}, forward: bigints.Observed(), strategy: "drop the range type the forward plan created"},
		{name: "a range type rebuilt", change: typeChange(pgtype.RangeKind, "t", &pgtype.RangeChange{Before: numeric, After: bigints,
			Differs: []string{pgtype.DiffSubtype}}),
			want:    &pgtype.RangeChange{Before: bigints.Observed(), After: numeric.Desired(), Differs: []string{pgtype.DiffSubtype}},
			forward: bigints.Observed(), strategy: "change the range type back to what it was"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reversed, err := newRuntime(c).ReverseChanges(t.Context(), reversing(test.change))

			c.Assert(err, qt.IsNil)
			c.Assert(reversed, qt.HasLen, 1)
			c.Assert(reversed[0].Change.Subject, qt.DeepEquals, test.change.Subject)
			c.Assert(reversed[0].Change.Value, qt.DeepEquals, test.want)
			c.Assert(reversed[0].ForwardState, qt.DeepEquals, []schemaext.ProjectedValue{{Placement: schemaext.ObjectPlacement,
				Kind: schemaext.Kind(test.change.Subject.Kind), Value: test.forward}})
			c.Assert(reversed[0].Strategy, qt.Equals, test.strategy)
			c.Assert(reversed[0].Limitations, qt.DeepEquals, test.limitations)
		})
	}
}

// TestReverseTypes_FailurePath refuses what no reversal of user types can
// answer, asked of the service directly.
func TestReverseTypes_FailurePath(t *testing.T) {
	created := typeChange(pgtype.DomainKind, "t", &pgtype.DomainChange{After: &pgtype.DesiredDomain{BaseType: "text"}})
	outside := reversing(created)
	outside.Target = "mysql"
	tests := []struct {
		name    string
		request schemaext.ReversalRequest
		is      error
	}{
		{name: "a target outside the family", request: outside, is: ptaherr.ErrUnsupportedDialect},
		{name: "a change of another owner", request: reversing(schemaext.ChangeRecord{Subject: created.Subject}), is: schemaext.ErrInvalidValue},
		{name: "an invalid change", request: reversing(typeChange(pgtype.RangeKind, "t", &pgtype.RangeChange{})), is: schemaext.ErrInvalidValue},
		{name: "a change of another model than its subject", is: schemaext.ErrInvalidValue,
			request: reversing(typeChange(pgtype.RangeKind, "t", created.Value))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reversed, err := typereverse.Service{}.ReverseChanges(t.Context(), test.request)

			c.Assert(err, qt.ErrorIs, test.is)
			c.Assert(reversed, qt.IsNil)
		})
	}
}

// TestReportTypes counts each type once under its model's metric, the name
// the common schema statistics report it under, in either representation.
func TestReportTypes(t *testing.T) {
	tests := []struct {
		name           string
		representation schemaext.Representation
		values         []schemaext.Value
		want           []schemaext.ValueReport
	}{
		{name: "declarations", representation: schemaext.Desired, values: []schemaext.Value{
			&pgtype.DesiredDomain{BaseType: "text"}, &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "n", Type: "int"}}},
			&pgtype.DesiredRange{Subtype: "int4"},
		}, want: []schemaext.ValueReport{
			{Kind: pgtype.DomainKind, Counts: []schemaext.MetricCount{{Name: "domains", Value: 1}}},
			{Kind: pgtype.CompositeKind, Counts: []schemaext.MetricCount{{Name: "composite_types", Value: 1}}},
			{Kind: pgtype.RangeKind, Counts: []schemaext.MetricCount{{Name: "range_types", Value: 1}}},
		}},
		{name: "observations", representation: schemaext.Observed, values: []schemaext.Value{
			&pgtype.ObservedDomain{BaseType: "text"}, &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "n", Type: "int"}}},
			&pgtype.ObservedRange{Subtype: "int4"},
		}, want: []schemaext.ValueReport{
			{Kind: pgtype.DomainKind, Counts: []schemaext.MetricCount{{Name: "domains", Value: 1}}},
			{Kind: pgtype.CompositeKind, Counts: []schemaext.MetricCount{{Name: "composite_types", Value: 1}}},
			{Kind: pgtype.RangeKind, Counts: []schemaext.MetricCount{{Name: "range_types", Value: 1}}},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			report, err := newRuntime(c).ReportFeatures(t.Context(), schemaext.ReportingRequest{Target: "postgres",
				Representation: test.representation, Values: test.values})

			c.Assert(err, qt.IsNil)
			c.Assert(report.Values, qt.DeepEquals, test.want)
		})
	}
}

// TestReportTypes_FailurePath refuses a value of the other representation
// and an invalid one.
func TestReportTypes_FailurePath(t *testing.T) {
	tests := []struct {
		name           string
		representation schemaext.Representation
		value          schemaext.Value
	}{
		{name: "an observation among declarations", representation: schemaext.Desired, value: &pgtype.ObservedDomain{BaseType: "text"}},
		{name: "a composite type observation among declarations", representation: schemaext.Desired,
			value: &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "n", Type: "int"}}}},
		{name: "a range type observation among declarations", representation: schemaext.Desired, value: &pgtype.ObservedRange{Subtype: "int4"}},
		{name: "a domain declaration among observations", representation: schemaext.Observed, value: &pgtype.DesiredDomain{BaseType: "text"}},
		{name: "a composite type declaration among observations", representation: schemaext.Observed,
			value: &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "n", Type: "int"}}}},
		{name: "a declaration among observations", representation: schemaext.Observed, value: &pgtype.DesiredRange{Subtype: "int4"}},
		{name: "an invalid declaration", representation: schemaext.Desired, value: &pgtype.DesiredComposite{}},
		{name: "an invalid observation", representation: schemaext.Observed, value: &pgtype.ObservedRange{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reports, err := typereport.Service{}.ReportValues(t.Context(), schemaext.ReportingRequest{Target: "postgres",
				Representation: test.representation, Values: []schemaext.Value{test.value}})

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(reports, qt.IsNil)
		})
	}
}
