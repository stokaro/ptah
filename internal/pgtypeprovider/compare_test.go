package pgtypeprovider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/coverage"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
	"ptah.run/feature/pgtype/typecompare"
)

// values is a short spelling of a list of operands.
func values(operands ...schemaext.Value) []schemaext.Value { return operands }

// TestCompareTypes_LeavesWhatAgrees pins what is no difference: another
// spelling of a type, what only a server could spell, what the declaration
// leaves to the catalog, and whitespace around a comment. Each declaration
// names no schema and each observation is in public, so every row also pins
// that the two pair.
func TestCompareTypes_LeavesWhatAgrees(t *testing.T) {
	check := "(VALUE <> ''::text)"
	tests := []struct {
		name        string
		declaration schemaext.Value
		observation schemaext.Value
	}{
		{name: "an alias of a domain's base type", declaration: &pgtype.DesiredDomain{BaseType: "VARCHAR(20)"},
			observation: &pgtype.ObservedDomain{BaseType: "character varying(20)"}},
		{name: "a CHECK and a default no server spelled", declaration: &pgtype.DesiredDomain{BaseType: "text", Check: "VALUE <> ''", Default: "x"},
			observation: &pgtype.ObservedDomain{BaseType: "text", Check: "(VALUE <> 'y'::text)", Default: "'y'::text"}},
		{name: "a CHECK in the server's spelling", declaration: &pgtype.DesiredDomain{BaseType: "text", Check: "VALUE <> ''",
			Normalized: &pgtype.NormalizedDomain{Check: check}},
			observation: &pgtype.ObservedDomain{BaseType: "text", Check: check, Checks: []pgtype.DomainCheck{{Name: "t_check", Expression: check}}}},
		{name: "a CHECK in the server's spelling, the constraints not listed", declaration: &pgtype.DesiredDomain{BaseType: "text",
			Check: "VALUE <> ''", Normalized: &pgtype.NormalizedDomain{Check: check}},
			observation: &pgtype.ObservedDomain{BaseType: "text", Check: check}},
		{name: "a default in the server's spelling", declaration: &pgtype.DesiredDomain{BaseType: "text", Default: "x",
			Normalized: &pgtype.NormalizedDomain{Default: "'x'::text"}},
			observation: &pgtype.ObservedDomain{BaseType: "text", Default: "'x'::text"}},
		{name: "a CHECK and a default the declaration leaves out", declaration: &pgtype.DesiredDomain{BaseType: "text",
			Normalized: &pgtype.NormalizedDomain{}},
			observation: &pgtype.ObservedDomain{BaseType: "text", Check: check, Default: "'x'::text"}},
		{name: "a domain comment in other whitespace", declaration: &pgtype.DesiredDomain{BaseType: "text", Comment: " mail "},
			observation: &pgtype.ObservedDomain{BaseType: "text", Comment: "mail"}},
		{name: "attribute names in another case and type aliases",
			declaration: &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "Street", Type: "varchar(40)"}, {Name: "n", Type: "int"}}},
			observation: &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "street", Type: "character varying(40)"},
				{Name: "n", Type: "integer"}}}},
		{name: "a composite type comment in other whitespace",
			declaration: &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "n", Type: "int"}}, Comment: "point\n"},
			observation: &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "n", Type: "integer"}}, Comment: "point"}},
		{name: "range attributes the declaration leaves out", declaration: &pgtype.DesiredRange{Subtype: "int4"},
			observation: &pgtype.ObservedRange{Subtype: "integer", SubtypeOpClass: "int4_ops", Canonical: "span_canonical", SubtypeDiff: "span_diff"}},
		{name: "a range attribute in another case", declaration: &pgtype.DesiredRange{Subtype: "numeric", SubtypeDiff: " Span_Diff "},
			observation: &pgtype.ObservedRange{Subtype: "numeric", SubtypeDiff: "span_diff"}},
		{name: "a range attribute cleared and absent", declaration: &pgtype.DesiredRange{Subtype: "numeric", Cleared: []string{pgtype.RangeCanonical}},
			observation: &pgtype.ObservedRange{Subtype: "numeric"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := newRuntime(c).CompareObjects(t.Context(), comparisonOf(values(test.declaration), values(test.observation)))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

// TestCompareTypes_FindsChanges pins each difference, the attribute delta
// ALTER TYPE can reach and the lists it cannot, a creation and a drop.
func TestCompareTypes_FindsChanges(t *testing.T) {
	check := "(VALUE <> ''::text)"
	text := &pgtype.ObservedDomain{BaseType: "text"}
	checked := &pgtype.ObservedDomain{BaseType: "text", Check: check, Checks: []pgtype.DomainCheck{{Name: "t_check", Expression: check}}}
	twoChecks := &pgtype.ObservedDomain{BaseType: "text", Check: "(" + check + ") AND (VALUE <> 'x'::text)",
		Checks: []pgtype.DomainCheck{{Name: "t_check", Expression: check}, {Name: "t_check1", Expression: "(VALUE <> 'x'::text)"}}}
	defaulted := &pgtype.ObservedDomain{BaseType: "text", Default: "'x'::text"}
	integer := &pgtype.DesiredDomain{BaseType: "integer"}
	notNull := &pgtype.DesiredDomain{BaseType: "text", NotNull: true}
	otherCheck := &pgtype.DesiredDomain{BaseType: "text", Check: "VALUE <> 'a'", Normalized: &pgtype.NormalizedDomain{Check: "(VALUE <> 'a'::text)"}}
	sameCheck := &pgtype.DesiredDomain{BaseType: "text", Check: "VALUE <> ''", Normalized: &pgtype.NormalizedDomain{Check: check}}
	joined := &pgtype.DesiredDomain{BaseType: "text", Check: "VALUE <> '' AND VALUE <> 'x'",
		Normalized: &pgtype.NormalizedDomain{Check: twoChecks.Check}}
	otherDefault := &pgtype.DesiredDomain{BaseType: "text", Default: "y", Normalized: &pgtype.NormalizedDomain{Default: "'y'::text"}}
	commented := &pgtype.DesiredDomain{BaseType: "text", Comment: "mail"}
	everything := &pgtype.DesiredDomain{BaseType: "integer", NotNull: true, Comment: "mail"}

	a := pgtype.Attribute{Name: "a", Type: "integer"}
	b := pgtype.Attribute{Name: "b", Type: "text"}
	cc := pgtype.Attribute{Name: "c", Type: "text"}
	ab := &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{a, b}}
	appended := &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{a, b, cc}}
	removed := &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{a}}
	replaced := &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{a, cc}}
	moved := &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{b, a}}
	inserted := &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{a, cc, b}}
	retyped := &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "a", Type: "bigint"}, b}}
	insertedRetyped := &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "a", Type: "bigint"}, b, cc}}
	commentedComposite := &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{a, b}, Comment: "pair"}

	numeric := &pgtype.ObservedRange{Subtype: "numeric", Collation: "POSIX", Canonical: "span_canonical"}
	otherSubtype := &pgtype.DesiredRange{Subtype: "int8"}
	otherCollation := &pgtype.DesiredRange{Subtype: "numeric", Collation: "C"}
	cleared := &pgtype.DesiredRange{Subtype: "numeric", Cleared: []string{pgtype.RangeCanonical}}
	commentedRange := &pgtype.DesiredRange{Subtype: "numeric", Comment: "span"}
	tests := []struct {
		name         string
		declarations []schemaext.Value
		observations []schemaext.Value
		want         schemaext.ChangeValue
	}{
		{name: "a domain's base type", declarations: values(integer), observations: values(text),
			want: &pgtype.DomainChange{Before: text, After: integer, Differs: []string{pgtype.DiffBaseType}}},
		{name: "a domain's NOT NULL", declarations: values(notNull), observations: values(text),
			want: &pgtype.DomainChange{Before: text, After: notNull, Differs: []string{pgtype.DiffNotNull}}},
		{name: "a domain's CHECK in the server's spelling", declarations: values(otherCheck), observations: values(checked),
			want: &pgtype.DomainChange{Before: checked, After: otherCheck, Differs: []string{pgtype.DiffCheck}}},
		{name: "a domain CHECK against two constraints", declarations: values(sameCheck), observations: values(twoChecks),
			want: &pgtype.DomainChange{Before: twoChecks, After: sameCheck, Differs: []string{pgtype.DiffCheck}}},
		{name: "a CHECK spelled as two constraints joined", declarations: values(joined), observations: values(twoChecks),
			want: &pgtype.DomainChange{Before: twoChecks, After: joined, Differs: []string{pgtype.DiffCheck}}},
		{name: "a domain CHECK the domain lacks", declarations: values(sameCheck), observations: values(text),
			want: &pgtype.DomainChange{Before: text, After: sameCheck, Differs: []string{pgtype.DiffCheck}}},
		{name: "a domain's default in the server's spelling", declarations: values(otherDefault), observations: values(defaulted),
			want: &pgtype.DomainChange{Before: defaulted, After: otherDefault, Differs: []string{pgtype.DiffDefault}}},
		{name: "a domain's comment", declarations: values(commented), observations: values(text),
			want: &pgtype.DomainChange{Before: text, After: commented, Differs: []string{pgtype.DiffComment}}},
		{name: "a domain's differences, sorted", declarations: values(everything), observations: values(text),
			want: &pgtype.DomainChange{Before: text, After: everything, Differs: []string{pgtype.DiffBaseType, pgtype.DiffComment, pgtype.DiffNotNull}}},
		{name: "an attribute appended", declarations: values(appended), observations: values(ab),
			want: &pgtype.CompositeChange{Before: ab, After: appended, Differs: []string{pgtype.DiffAttributes}, Added: []pgtype.Attribute{cc}}},
		{name: "an attribute removed", declarations: values(removed), observations: values(ab),
			want: &pgtype.CompositeChange{Before: ab, After: removed, Differs: []string{pgtype.DiffAttributes}, Removed: []string{"b"}}},
		{name: "an attribute replaced", declarations: values(replaced), observations: values(ab),
			want: &pgtype.CompositeChange{Before: ab, After: replaced, Differs: []string{pgtype.DiffAttributes}, Added: []pgtype.Attribute{cc},
				Removed: []string{"b"}}},
		{name: "attributes in another order", declarations: values(moved), observations: values(ab),
			want: &pgtype.CompositeChange{Before: ab, After: moved, Differs: []string{pgtype.DiffAttributes}}},
		{name: "an attribute inserted before the last", declarations: values(inserted), observations: values(ab),
			want: &pgtype.CompositeChange{Before: ab, After: inserted, Differs: []string{pgtype.DiffAttributes}}},
		{name: "an attribute of another type", declarations: values(retyped), observations: values(ab),
			want: &pgtype.CompositeChange{Before: ab, After: retyped, Differs: []string{pgtype.DiffAttributes}}},
		{name: "an attribute of another type beside one appended", declarations: values(insertedRetyped), observations: values(ab),
			want: &pgtype.CompositeChange{Before: ab, After: insertedRetyped, Differs: []string{pgtype.DiffAttributes}}},
		{name: "a composite type's comment", declarations: values(commentedComposite), observations: values(ab),
			want: &pgtype.CompositeChange{Before: ab, After: commentedComposite, Differs: []string{pgtype.DiffComment}}},
		{name: "a range type's subtype", declarations: values(otherSubtype), observations: values(numeric),
			want: &pgtype.RangeChange{Before: numeric, After: otherSubtype, Differs: []string{pgtype.DiffSubtype}}},
		{name: "a range attribute the declaration states", declarations: values(otherCollation), observations: values(numeric),
			want: &pgtype.RangeChange{Before: numeric, After: otherCollation, Differs: []string{pgtype.RangeCollation}}},
		{name: "a range attribute the declaration clears", declarations: values(cleared), observations: values(numeric),
			want: &pgtype.RangeChange{Before: numeric, After: cleared, Differs: []string{pgtype.RangeCanonical}}},
		{name: "a range type's comment", declarations: values(commentedRange), observations: values(numeric),
			want: &pgtype.RangeChange{Before: numeric, After: commentedRange, Differs: []string{pgtype.DiffComment}}},
		{name: "a domain the server lacks", declarations: values(notNull), want: &pgtype.DomainChange{After: notNull}},
		{name: "a composite type nothing declares", observations: values(ab), want: &pgtype.CompositeChange{Before: ab}},
		{name: "a range type the server lacks", declarations: values(cleared), want: &pgtype.RangeChange{After: cleared}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := comparisonOf(test.declarations, test.observations)

			result, err := newRuntime(c).CompareObjects(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Undecided, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, 1)
			c.Assert(result.Changes[0].Value, qt.DeepEquals, test.want)
		})
	}
}

// TestCompareTypes_LeavesCommentsATargetDoesNotStore pins that a comment is
// compared only where the target stores the kind's comment: a reader reports
// none elsewhere, so a declared one would differ on every run.
func TestCompareTypes_LeavesCommentsATargetDoesNotStore(t *testing.T) {
	tests := []struct {
		name        string
		declaration schemaext.Value
		observation schemaext.Value
		without     capability.Capability
	}{
		{name: "a domain", declaration: &pgtype.DesiredDomain{BaseType: "text", Comment: "mail"},
			observation: &pgtype.ObservedDomain{BaseType: "text"}, without: capability.DomainComments},
		{name: "a composite type", declaration: &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "n", Type: "int"}}, Comment: "n"},
			observation: &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "n", Type: "int"}}}, without: capability.TypeComments},
		{name: "a range type", declaration: &pgtype.DesiredRange{Subtype: "int4", Comment: "span"},
			observation: &pgtype.ObservedRange{Subtype: "int4"}, without: capability.TypeComments},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := comparisonOf(values(test.declaration), values(test.observation))
			request.Capabilities = capability.Postgres17().With(test.without, false)

			result, err := newRuntime(c).CompareObjects(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
		})
	}
}

// TestCompareTypes_WaitsForARead pins that a declaration is not created where
// the read could not say whether the type is there: CREATE DOMAIN and CREATE
// TYPE have no conditional form, so a type that exists fails the migration.
func TestCompareTypes_WaitsForARead(t *testing.T) {
	c := qt.New(t)
	request := comparisonOf(values(&pgtype.DesiredDomain{BaseType: "text"}), nil)
	request.Current.Coverage = must.Must(pgtype.Coverage(pgtype.DomainKind, schemaext.Observed,
		schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "this read lists no domains"}, nil))

	result, err := newRuntime(c).CompareObjects(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.DeepEquals, []schemaext.UndecidedChange{{Kind: pgtype.DomainKind,
		Subject: pgtype.Ref(pgtype.DomainKind, "", "t"), Reason: "whether the domain exists was not established: this read lists no domains"}})
}

// TestCompareTypes_KeepsWhatNobodyDescribed pins that a type the desired
// source did not describe, or asked to default, is not dropped.
func TestCompareTypes_KeepsWhatNobodyDescribed(t *testing.T) {
	subject := pgtype.Ref(pgtype.CompositeKind, "public", "t")
	tests := []struct {
		name      string
		knowledge schemaext.Knowledge
		subjects  []schemaext.SubjectCoverage
	}{
		{name: "an uninspected kind", knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the document leaves composite types out"}},
		{name: "an unrepresentable kind", knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the source cannot say"}},
		{name: "a defaulted type", knowledge: schemaext.Knowledge{State: schemaext.Complete}, subjects: []schemaext.SubjectCoverage{{
			Kind: pgtype.CompositeKind, Subject: subject, Knowledge: schemaext.Knowledge{State: schemaext.Defaulted}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := comparisonOf(nil, values(&pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "n", Type: "int"}}}))
			request.Desired.Coverage = must.Must(pgtype.Coverage(pgtype.CompositeKind, schemaext.Desired, test.knowledge, test.subjects))

			result, err := newRuntime(c).CompareObjects(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

// TestProvider_MapsTheTypeCoverageKinds pins that a description declining a
// kind declines the owner's types of it: a document whose header says it does
// not describe domains plans no drop of the domains the database holds.
func TestProvider_MapsTheTypeCoverageKinds(t *testing.T) {
	domain := &pgtype.ObservedDomain{BaseType: "text"}
	composite := &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "n", Type: "int"}}}
	span := &pgtype.ObservedRange{Subtype: "int4"}
	tests := []struct {
		name    string
		held    schemaext.Value
		limited []coverage.Object
		changes int
	}{
		{name: "a record naming domains", held: domain, limited: []coverage.Object{coverage.Refused(coverage.Domain)}},
		{name: "a record naming the domain qualified", held: domain, limited: []coverage.Object{{Kind: coverage.Domain, Name: "public.t"}}},
		{name: "a record naming the domain bare", held: domain, limited: []coverage.Object{{Kind: coverage.Domain, Name: "t"}}},
		{name: "a record naming composite types", held: composite, limited: []coverage.Object{coverage.Refused(coverage.Composite)}},
		{name: "a record naming range types", held: span, limited: []coverage.Object{coverage.Refused(coverage.Range)}},
		{name: "control: a record naming another domain", held: domain, limited: []coverage.Object{{Kind: coverage.Domain, Name: "u"}}, changes: 1},
		{name: "control: a record naming another kind", held: span, limited: []coverage.Object{coverage.Refused(coverage.Domain)}, changes: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			objects := comparisonOf(nil, values(test.held))
			request := schemaext.ComparisonRequest{Target: "postgres", Identifiers: objects.Identifiers, Capabilities: objects.Capabilities,
				Desired:       schemaext.FeatureState{Objects: objects.Desired.Objects, Coverage: objects.Desired.Coverage},
				Current:       schemaext.FeatureState{Objects: objects.Current.Objects, Coverage: objects.Current.Coverage},
				DesiredLimits: coverage.Set{}.With(test.limited...)}

			result, err := newRuntime(c).CompareFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, test.changes)
		})
	}
}

// TestCompareTypes_FailurePath refuses what no comparison of user types can
// answer, before deciding anything. The runtime sends the owner nothing it was
// not registered for, so each is asked of the service directly.
func TestCompareTypes_FailurePath(t *testing.T) {
	domain := &pgtype.DesiredDomain{BaseType: "text"}
	outside := direct(nil, nil)
	outside.Target = "mysql"
	otherKind := direct(nil, nil)
	otherKind.Kinds = []schemaext.Kind{pgtype.DomainKind, "example.org/other"}
	noKind := direct(nil, nil)
	noKind.Kinds = nil
	requested := direct(nil, nil)
	requested.Requests = []schemaext.ChangeRequest{{Subject: pgtype.Ref(pgtype.DomainKind, "", "t")}}
	observationDeclared := direct(nil, nil)
	observationDeclared.Desired.Objects = objectsOf(held(&pgtype.ObservedDomain{BaseType: "text"}))
	declarationHeld := direct(nil, nil)
	declarationHeld.Current.Objects = objectsOf(declared(domain))
	invalid := direct(nil, nil)
	invalid.Desired.Objects = objectsOf(schemaext.Object{Ref: ref(domain, ""), Value: &pgtype.DesiredDomain{}})
	tests := []struct {
		name    string
		request schemaext.ObjectComparisonRequest
		is      error
		want    string
	}{
		{name: "a target outside the family", request: outside, is: ptaherr.ErrUnsupportedDialect, want: `.*PostgreSQL user type comparison on "mysql"`},
		{name: "another kind", request: otherKind, is: schemaext.ErrInvalidValue, want: `.*unsupported user type comparison kinds`},
		{name: "no kind", request: noKind, is: schemaext.ErrInvalidValue, want: `.*unsupported user type comparison kinds`},
		{name: "a change request", request: requested, is: schemaext.ErrInvalidValue, want: `.*user type comparison accepts no change requests`},
		{name: "an observation among the declarations", request: observationDeclared, is: schemaext.ErrInvalidValue,
			want: `.*a domain observation among the declarations: .*`},
		{name: "a declaration among the observations", request: declarationHeld, is: schemaext.ErrInvalidValue,
			want: `.*a domain declaration among the observations: .*`},
		{name: "an invalid declaration", request: invalid, is: schemaext.ErrInvalidValue, want: `.*a domain requires a base type`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := typecompare.Service{}.CompareObjects(t.Context(), test.request)

			c.Assert(err, qt.ErrorIs, test.is)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(result.Complete, qt.IsFalse)
		})
	}
}
