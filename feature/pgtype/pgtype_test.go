package pgtype_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
)

// TestRef_HappyPath pins the identity of a type: its schema and name under
// PostgreSQL's rules, a schema left to the default recorded as defaulted, and
// the spelling a statement writes, which is the schema the source wrote.
func TestRef_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		kind      schemaext.Kind
		schema    string
		typeName  string
		qualified string
		label     string
	}{
		{name: "a domain in a schema", kind: pgtype.DomainKind, schema: "app", typeName: "qty", qualified: "app.qty", label: "domain"},
		{name: "a composite type in the default schema", kind: pgtype.CompositeKind, typeName: "addr", qualified: "addr", label: "composite type"},
		{name: "a range type with a dot in its name", kind: pgtype.RangeKind, schema: "app", typeName: "a.b", qualified: `app."a.b"`,
			label: "range type"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			ref := pgtype.Ref(test.kind, test.schema, test.typeName)

			c.Assert(pgtype.ValidateRef(ref), qt.IsNil)
			c.Assert(pgtype.QualifiedName(ref), qt.Equals, test.qualified)
			c.Assert(pgtype.Label(ref), qt.Equals, test.label)
			c.Assert(pgtype.TargetOf(ref).Subject(), qt.DeepEquals, ref)
		})
	}
}

// TestRef_DefaultedSchema pins that an identity built without a schema
// names the connection's default, so it pairs with one that spells it, and
// still renders without it.
func TestRef_DefaultedSchema(t *testing.T) {
	c := qt.New(t)

	defaulted, spelled := pgtype.Ref(pgtype.DomainKind, "", "qty"), pgtype.Ref(pgtype.DomainKind, "public", "qty")

	c.Assert(defaulted.Key(), qt.Equals, spelled.Key())
	c.Assert(pgtype.AuthoredSchema(defaulted), qt.Equals, "")
	c.Assert(pgtype.AuthoredSchema(spelled), qt.Equals, "public")
}

func TestValidateRef_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		ref  objectidentity.ID
		want string
	}{
		{name: "a kind of another owner", ref: objectidentity.ID{Kind: "ptah.run/pgextension/extension", Name: objectidentity.Part{Source: "x", Normalized: "x"}},
			want: `.*"ptah.run/pgextension/extension" is not a PostgreSQL user type kind`},
		{name: "a type on a table", ref: func() objectidentity.ID {
			ref := pgtype.Ref(pgtype.DomainKind, "app", "qty")
			ref.Parent = objectidentity.Part{Source: "t", Normalized: "t"}
			return ref
		}(), want: `.*a domain requires a schema-scoped identity with a name`},
		{name: "a type without a name", ref: pgtype.Ref(pgtype.RangeKind, "app", " "),
			want: `.*a range type requires a schema-scoped identity with a name`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := pgtype.ValidateRef(test.ref)

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// TestObjects_DefineTheirTypes pins what a host reads off the owner's
// objects: each type under its common kind, naming the types its definition
// names.
func TestObjects_DefineTheirTypes(t *testing.T) {
	c := qt.New(t)
	domain := must.Must(pgtype.DesiredObject(pgtype.Ref(pgtype.DomainKind, "app", "qty"), &pgtype.DesiredDomain{BaseType: "integer"}))
	composite := must.Must(pgtype.ObservedObject(pgtype.Ref(pgtype.CompositeKind, "app", "addr"),
		&pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "a", Type: "qty"}, {Name: "b", Type: "text"}}}))
	rangeType := must.Must(pgtype.DesiredObject(pgtype.Ref(pgtype.RangeKind, "", "fr"), &pgtype.DesiredRange{Subtype: "float8"}))

	defined := schemaext.DefinedTypes(must.Must(schemaext.NewObjects(domain, composite, rangeType)))

	subject := func(kind objectidentity.Kind, ref objectidentity.ID) objectidentity.ID {
		ref.Kind = kind
		return ref
	}
	c.Assert(defined, qt.DeepEquals, []schemaext.DefinedType{
		{ID: subject(objectidentity.KindComposite, composite.Ref), References: []string{"qty", "text"}},
		{ID: subject(objectidentity.KindDomain, domain.Ref), References: []string{"integer"}},
		{ID: subject(objectidentity.KindRange, rangeType.Ref), References: []string{"float8"}},
	})
}

func TestObject_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		build   func() (schemaext.Object, error)
		wantErr string
	}{
		{name: "an observation as a declaration", build: func() (schemaext.Object, error) {
			return pgtype.DesiredObject(pgtype.Ref(pgtype.DomainKind, "", "qty"), &pgtype.ObservedDomain{BaseType: "int"})
		}, wantErr: `.*expected the desired representation, got the observed one`},
		{name: "a declaration under another kind's identity", build: func() (schemaext.Object, error) {
			return pgtype.DesiredObject(pgtype.Ref(pgtype.RangeKind, "", "qty"), &pgtype.DesiredDomain{BaseType: "int"})
		}, wantErr: `.*a domain object is identified as a domain, not "ptah.run/pgtype/range"`},
		{name: "an invalid declaration", build: func() (schemaext.Object, error) {
			return pgtype.DesiredObject(pgtype.Ref(pgtype.CompositeKind, "", "addr"), &pgtype.DesiredComposite{})
		}, wantErr: `.*a composite type requires at least one attribute`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			object, err := test.build()

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(object, qt.DeepEquals, schemaext.Object{})
		})
	}
}

// TestRepresentations_RoundTrip pins the two conversions: an observation's
// declaration keeps it as it is, and a declaration predicts the observation
// of what it writes. A domain's stored default reads back as a literal where
// the server quoted it and as an expression otherwise.
func TestRepresentations_RoundTrip(t *testing.T) {
	c := qt.New(t)

	expression := (&pgtype.ObservedDomain{BaseType: "timestamptz", Default: "now()"}).Desired()
	literal := (&pgtype.ObservedDomain{BaseType: "text", Default: "'x'", Comment: "label"}).Desired()
	predicted := (&pgtype.DesiredDomain{BaseType: "text", Default: "x", DefaultExpr: "upper('x')", Check: "VALUE <> ''"}).Observed()
	rangeType := (&pgtype.ObservedRange{Subtype: "int4", Collation: "C"}).Desired().Observed()
	composite := (&pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "a", Type: "int"}}}).Desired().Observed()

	c.Assert(expression, qt.DeepEquals, &pgtype.DesiredDomain{BaseType: "timestamptz", DefaultExpr: "now()"})
	c.Assert(literal, qt.DeepEquals, &pgtype.DesiredDomain{BaseType: "text", Default: "'x'", Comment: "label"})
	c.Assert(predicted, qt.DeepEquals, &pgtype.ObservedDomain{BaseType: "text", Default: "upper('x')", Check: "VALUE <> ''"})
	c.Assert(rangeType, qt.DeepEquals, &pgtype.ObservedRange{Subtype: "int4", Collation: "C"})
	c.Assert(composite, qt.DeepEquals, &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "a", Type: "int"}}})
}

// TestValues_CloneIndependently pins that a clone shares no list or pointer
// with its original, so an owner can hand one out.
func TestValues_CloneIndependently(t *testing.T) {
	c := qt.New(t)
	domain := &pgtype.DesiredDomain{BaseType: "int", Normalized: &pgtype.NormalizedDomain{Check: "a"}}
	observed := &pgtype.ObservedDomain{BaseType: "int", Checks: []pgtype.DomainCheck{{Name: "c", Expression: "e"}}}
	composite := &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "a", Type: "int"}}}
	rangeType := &pgtype.DesiredRange{Subtype: "int4", Cleared: []string{pgtype.RangeCollation}}

	domainClone := domain.Clone().(*pgtype.DesiredDomain)
	observedClone := observed.Clone().(*pgtype.ObservedDomain)
	compositeClone := composite.Clone().(*pgtype.DesiredComposite)
	rangeClone := rangeType.Clone().(*pgtype.DesiredRange)
	domainClone.Normalized.Check = "changed"
	observedClone.Checks[0].Name = "changed"
	compositeClone.Attributes[0].Name = "changed"
	rangeClone.Cleared[0] = "changed"

	c.Assert(domain.Normalized.Check, qt.Equals, "a")
	c.Assert(observed.Checks[0].Name, qt.Equals, "c")
	c.Assert(composite.Attributes[0].Name, qt.Equals, "a")
	c.Assert(rangeType.Cleared[0], qt.Equals, pgtype.RangeCollation)
}

// TestCompleteCoverage_ClaimsEveryModel pins that a complete claim covers the
// three models, under this owner, so a type a source leaves out is absent.
func TestCompleteCoverage_ClaimsEveryModel(t *testing.T) {
	c := qt.New(t)

	complete := must.Must(pgtype.CompleteCoverage(schemaext.Desired))

	c.Assert(complete.KindRecords(), qt.HasLen, 3)
	c.Assert(complete.Lookup(pgtype.RangeKind, pgtype.Ref(pgtype.RangeKind, "app", "fr")).State, qt.Equals, schemaext.Complete)
	c.Assert(complete.KindRecords()[0].Model.Owner, qt.Equals, pgtype.Owner)
}
