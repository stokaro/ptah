package oratypeprovider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/oratype"
)

// TestCompareTypes_LeavesWhatAgrees pins what is no difference: another
// spelling of a type, the catalog's own spelling of a CHECK and a default,
// attribute names in another case, and a comment, which Oracle does not store.
// Each declaration names its type in lower case and each observation in upper
// case, so every row also pins that the two pair.
func TestCompareTypes_LeavesWhatAgrees(t *testing.T) {
	tests := []struct {
		name                     string
		declaration, observation schemaext.Value
	}{
		{name: "a base type in another case and spacing", declaration: &oratype.DesiredDomain{BaseType: "number( 5, 2 )"},
			observation: &oratype.ObservedDomain{BaseType: "NUMBER(5,2)"}},
		{name: "a CHECK and a default in the catalog's spelling",
			declaration: &oratype.DesiredDomain{BaseType: "VARCHAR2(9)", Check: "VALUE <> 'x'", Default: "y"},
			observation: &oratype.ObservedDomain{BaseType: "VARCHAR2(9)", Check: "VALUE<>'x'", Default: "'y'"}},
		{name: "a CHECK and a default the declaration leaves out", declaration: &oratype.DesiredDomain{BaseType: "NUMBER"},
			observation: &oratype.ObservedDomain{BaseType: "NUMBER", Check: "VALUE > 0", Default: "1"}},
		{name: "a domain comment", declaration: &oratype.DesiredDomain{BaseType: "NUMBER", Comment: "count"},
			observation: &oratype.ObservedDomain{BaseType: "NUMBER"}},
		{name: "attribute names in another case",
			declaration: &oratype.DesiredObjectType{Attributes: []oratype.Attribute{{Name: "street", Type: "varchar2(10)"}}, Comment: "address"},
			observation: &oratype.ObservedObjectType{Attributes: []oratype.Attribute{{Name: "STREET", Type: "VARCHAR2(10)"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := newRuntime(c).CompareObjects(t.Context(),
				comparison(objects(declared("t", test.declaration)), objects(held("T", test.observation))))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

// TestCompareTypes_ReducesABaseTypeToTheCatalogSpelling pins each declared
// spelling against the one ALL_DOMAIN_COLS reported for it on 23.26.3.0.0. A
// spelling compared as written plans a base type change on every run, and the
// plan refuses that change.
func TestCompareTypes_ReducesABaseTypeToTheCatalogSpelling(t *testing.T) {
	tests := []struct {
		declared, held string
	}{
		{declared: "INTEGER", held: "NUMBER(*,0)"},
		{declared: "int", held: "NUMBER(*,0)"},
		{declared: "SMALLINT", held: "NUMBER(*,0)"},
		{declared: "NUMERIC", held: "NUMBER(*,0)"},
		{declared: "DECIMAL", held: "NUMBER(*,0)"},
		{declared: "DECIMAL(10,2)", held: "NUMBER(10,2)"},
		{declared: "DECIMAL(5)", held: "NUMBER(5)"},
		{declared: "NUMERIC(5, 2)", held: "NUMBER(5,2)"},
		{declared: "NUMBER(10,0)", held: "NUMBER(10)"},
		{declared: "NUMBER(*,2)", held: "NUMBER(*,2)"},
		{declared: "FLOAT", held: "FLOAT(126)"},
		{declared: "DOUBLE PRECISION", held: "FLOAT(126)"},
		{declared: "REAL", held: "FLOAT(63)"},
		{declared: "TIMESTAMP", held: "TIMESTAMP(6)"},
		{declared: "timestamp with time zone", held: "TIMESTAMP(6) WITH TIME ZONE"},
		{declared: "TIMESTAMP WITH LOCAL TIME ZONE", held: "TIMESTAMP(6) WITH LOCAL TIME ZONE"},
		{declared: "INTERVAL DAY TO SECOND", held: "INTERVAL DAY(2) TO SECOND(6)"},
		{declared: "INTERVAL YEAR TO MONTH", held: "INTERVAL YEAR(2) TO MONTH"},
		{declared: "VARCHAR(10)", held: "VARCHAR2(10)"},
		{declared: "VARCHAR2(10 BYTE)", held: "VARCHAR2(10)"},
		{declared: "VARCHAR2(10 CHAR)", held: "VARCHAR2(10 CHAR)"},
		{declared: "VARCHAR2(4000 CHAR)", held: "VARCHAR2(4000)"},
		{declared: "CHAR", held: "CHAR(1)"},
		{declared: "CHAR(3 CHAR)", held: "CHAR(3 CHAR)"},
		{declared: "BOOLEAN", held: "BOOLEAN"},
	}
	for _, test := range tests {
		t.Run(test.declared, func(t *testing.T) {
			c := qt.New(t)

			result, err := newRuntime(c).CompareObjects(t.Context(), comparison(
				objects(declared("d", &oratype.DesiredDomain{BaseType: test.declared})),
				objects(held("D", &oratype.ObservedDomain{BaseType: test.held}))))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
		})
	}
}

// TestCompareTypes_FindsABaseTypeChange is the control: a spelling that names
// another type still differs. A length in characters the catalog shows is not
// one in bytes.
func TestCompareTypes_FindsABaseTypeChange(t *testing.T) {
	tests := []struct {
		declared, held string
	}{
		{declared: "INTEGER", held: "NUMBER(10)"},
		{declared: "NUMBER(10,2)", held: "NUMBER(10)"},
		{declared: "TIMESTAMP", held: "TIMESTAMP(3)"},
		{declared: "FLOAT", held: "FLOAT(63)"},
		{declared: "VARCHAR2(10)", held: "VARCHAR2(10 CHAR)"},
		{declared: "VARCHAR2(20 CHAR)", held: "VARCHAR2(10 CHAR)"},
		{declared: "CHAR", held: "CHAR(2)"},
		{declared: "BOOLEAN", held: "NUMBER(1)"},
	}
	for _, test := range tests {
		t.Run(test.declared+" against "+test.held, func(t *testing.T) {
			c := qt.New(t)
			domain := &oratype.DesiredDomain{BaseType: test.declared}
			observed := &oratype.ObservedDomain{BaseType: test.held}

			result, err := newRuntime(c).CompareObjects(t.Context(), comparison(objects(declared("d", domain)), objects(held("D", observed))))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.DeepEquals, []schemaext.ChangeRecord{change(oratype.DomainKind, "d",
				&oratype.DomainChange{Before: observed, After: domain, Differs: []string{oratype.DiffBaseType}})})
		})
	}
}

// TestCompareTypes_ReducesAnAttributeTypeToTheCatalogSpelling pins each
// declared attribute spelling against the one the read composes from
// ALL_TYPE_ATTRS on 23.26.3.0.0. A spelling compared as written plans CREATE
// OR REPLACE TYPE on every run, which Oracle refuses while a column uses the
// type.
func TestCompareTypes_ReducesAnAttributeTypeToTheCatalogSpelling(t *testing.T) {
	tests := []struct {
		declared, held string
	}{
		{declared: "INT", held: "INTEGER"},
		{declared: "NUMERIC", held: "DECIMAL"},
		{declared: "NUMERIC(5,2)", held: "DECIMAL(5,2)"},
		{declared: "NUMBER(10,2)", held: "NUMBER(10,2)"},
		{declared: "NUMBER(10,0)", held: "NUMBER(10)"},
		{declared: "DECIMAL(5)", held: "DECIMAL(5)"},
		{declared: "DOUBLE PRECISION", held: "DOUBLE PRECISION"},
		{declared: "TIMESTAMP", held: "TIMESTAMP(6)"},
		{declared: "TIMESTAMP(3)", held: "TIMESTAMP(3)"},
		{declared: "TIMESTAMP WITH TIME ZONE", held: "TIMESTAMP(6) WITH TIME ZONE"},
		{declared: "TIMESTAMP WITH LOCAL TIME ZONE", held: "TIMESTAMP(6) WITH LOCAL TIME ZONE"},
		{declared: "INTERVAL DAY TO SECOND", held: "INTERVAL DAY(2) TO SECOND(6)"},
		{declared: "INTERVAL YEAR TO MONTH", held: "INTERVAL YEAR(2) TO MONTH"},
		{declared: "VARCHAR(10)", held: "VARCHAR2(10)"},
		{declared: "VARCHAR2(10 BYTE)", held: "VARCHAR2(10)"},
		{declared: "varchar2(10 char)", held: "VARCHAR2(10 CHAR)"},
		{declared: "CHAR", held: "CHAR(1)"},
		{declared: "BOOLEAN", held: "BOOLEAN"},
	}
	for _, test := range tests {
		t.Run(test.declared, func(t *testing.T) {
			c := qt.New(t)
			declaration := &oratype.DesiredObjectType{Attributes: []oratype.Attribute{{Name: "a", Type: test.declared}}}
			observation := &oratype.ObservedObjectType{Attributes: []oratype.Attribute{{Name: "A", Type: test.held}}}

			result, err := newRuntime(c).CompareObjects(t.Context(),
				comparison(objects(declared("p", declaration)), objects(held("P", observation))))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
		})
	}
}

// TestCompareTypes_FindsAnAttributeTypeChange is the control: an attribute
// spelling that names another type still differs, and ALL_TYPE_ATTRS reports
// the length unit, so a length in characters and one in bytes differ too.
func TestCompareTypes_FindsAnAttributeTypeChange(t *testing.T) {
	tests := []struct {
		declared, held string
	}{
		{declared: "VARCHAR2(10 CHAR)", held: "VARCHAR2(10)"},
		{declared: "VARCHAR2(10)", held: "VARCHAR2(10 CHAR)"},
		{declared: "TIMESTAMP", held: "TIMESTAMP(3)"},
		{declared: "INTEGER", held: "NUMBER(10)"},
		{declared: "TIMESTAMP WITH TIME ZONE", held: "TIMESTAMP(6) WITH LOCAL TIME ZONE"},
	}
	for _, test := range tests {
		t.Run(test.declared+" against "+test.held, func(t *testing.T) {
			c := qt.New(t)
			declaration := &oratype.DesiredObjectType{Attributes: []oratype.Attribute{{Name: "a", Type: test.declared}}}
			observation := &oratype.ObservedObjectType{Attributes: []oratype.Attribute{{Name: "A", Type: test.held}}}

			result, err := newRuntime(c).CompareObjects(t.Context(),
				comparison(objects(declared("p", declaration)), objects(held("P", observation))))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.DeepEquals, []schemaext.ChangeRecord{change(oratype.ObjectTypeKind, "p",
				&oratype.ObjectTypeChange{Before: observation, After: declaration, Differs: []string{oratype.DiffAttributes}})})
		})
	}
}

// TestCompareTypes_FindsChanges pins each difference, a creation and a drop.
// A surviving type's change names the declaration's identity, which is how a
// statement spells the type; a drop names the catalog's.
func TestCompareTypes_FindsChanges(t *testing.T) {
	email := &oratype.DesiredDomain{BaseType: "VARCHAR2(255)", NotNull: true}
	point := []oratype.Attribute{{Name: "X", Type: "NUMBER"}, {Name: "Y", Type: "NUMBER"}}
	tests := []struct {
		name             string
		desired, current []schemaext.Object
		want             schemaext.ChangeRecord
	}{
		{name: "a base type", desired: objects(declared("d", email)),
			current: objects(held("D", &oratype.ObservedDomain{BaseType: "VARCHAR2(100)", NotNull: true})),
			want: change(oratype.DomainKind, "d", &oratype.DomainChange{Before: &oratype.ObservedDomain{BaseType: "VARCHAR2(100)", NotNull: true},
				After: email, Differs: []string{oratype.DiffBaseType}})},
		{name: "NOT NULL", desired: objects(declared("d", email)),
			current: objects(held("D", &oratype.ObservedDomain{BaseType: "VARCHAR2(255)"})),
			want: change(oratype.DomainKind, "d", &oratype.DomainChange{Before: &oratype.ObservedDomain{BaseType: "VARCHAR2(255)"},
				After: email, Differs: []string{oratype.DiffNotNull}})},
		{name: "both", desired: objects(declared("d", email)),
			current: objects(held("D", &oratype.ObservedDomain{BaseType: "NUMBER"})),
			want: change(oratype.DomainKind, "d", &oratype.DomainChange{Before: &oratype.ObservedDomain{BaseType: "NUMBER"},
				After: email, Differs: []string{oratype.DiffBaseType, oratype.DiffNotNull}})},
		{name: "attributes in another order", desired: objects(declared("p", &oratype.DesiredObjectType{Attributes: point})),
			current: objects(held("P", &oratype.ObservedObjectType{Attributes: []oratype.Attribute{point[1], point[0]}})),
			want: change(oratype.ObjectTypeKind, "p", &oratype.ObjectTypeChange{
				Before: &oratype.ObservedObjectType{Attributes: []oratype.Attribute{point[1], point[0]}},
				After:  &oratype.DesiredObjectType{Attributes: point}, Differs: []string{oratype.DiffAttributes}})},
		{name: "an attribute's type", desired: objects(declared("p", &oratype.DesiredObjectType{Attributes: point})),
			current: objects(held("P", &oratype.ObservedObjectType{Attributes: []oratype.Attribute{point[0], {Name: "Y", Type: "VARCHAR2(5)"}}})),
			want: change(oratype.ObjectTypeKind, "p", &oratype.ObjectTypeChange{
				Before: &oratype.ObservedObjectType{Attributes: []oratype.Attribute{point[0], {Name: "Y", Type: "VARCHAR2(5)"}}},
				After:  &oratype.DesiredObjectType{Attributes: point}, Differs: []string{oratype.DiffAttributes}})},
		{name: "a creation", desired: objects(declared("d", email)),
			want: change(oratype.DomainKind, "d", &oratype.DomainChange{After: email})},
		{name: "a drop", current: objects(held("P", &oratype.ObservedObjectType{Attributes: point})),
			want: change(oratype.ObjectTypeKind, "P", &oratype.ObjectTypeChange{Before: &oratype.ObservedObjectType{Attributes: point}})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := newRuntime(c).CompareObjects(t.Context(), comparison(test.desired, test.current))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Changes, qt.DeepEquals, []schemaext.ChangeRecord{test.want})
		})
	}
}

// TestCompareTypes_PairsTheConnectionsSchema pins that a declaration naming the
// connection's schema and an observation naming none are one type, as they are
// on the server.
func TestCompareTypes_PairsTheConnectionsSchema(t *testing.T) {
	c := qt.New(t)
	request := comparison(
		objects(must.Must(oratype.DesiredObject(oratype.Ref(oratype.DomainKind, "ptah", "d"), &oratype.DesiredDomain{BaseType: "NUMBER"}))),
		objects(held("D", &oratype.ObservedDomain{BaseType: "NUMBER"})))
	request.Identifiers = identifier.ForDialect("oracle")
	request.Identifiers.DefaultSchema = "PTAH"

	result, err := newRuntime(c).CompareObjects(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
}

// TestCompareTypes_WhatTheSourcesDoNotDescribe pins the coverage rules: a
// creation is undecided where the read did not establish the type's absence,
// except on a target that cannot hold the kind, where the plan refuses it by
// name; and a type the desired source does not describe is not dropped.
func TestCompareTypes_WhatTheSourcesDoNotDescribe(t *testing.T) {
	domain := &oratype.DesiredDomain{BaseType: "NUMBER"}
	t.Run("an addition the read did not establish", func(t *testing.T) {
		c := qt.New(t)
		request := comparison(objects(declared("d", domain)), nil)
		request.Current.Coverage = schemaext.Coverage{}

		result, err := newRuntime(c).CompareObjects(t.Context(), request)

		c.Assert(err, qt.IsNil)
		c.Assert(result.Changes, qt.HasLen, 0)
		c.Assert(result.Undecided, qt.HasLen, 1)
		c.Assert(result.Undecided[0].Reason, qt.Matches, `whether the domain exists was not established: .*`)
	})
	t.Run("an addition on a target without domains", func(t *testing.T) {
		c := qt.New(t)
		request := comparison(objects(declared("d", domain)), nil)
		request.Current.Coverage = schemaext.Coverage{}
		request.Capabilities = capability.Oracle21()

		result, err := newRuntime(c).CompareObjects(t.Context(), request)

		c.Assert(err, qt.IsNil)
		c.Assert(result.Changes, qt.DeepEquals, []schemaext.ChangeRecord{change(oratype.DomainKind, "d", &oratype.DomainChange{After: domain})})
		c.Assert(result.Undecided, qt.HasLen, 0)
	})
	t.Run("a removal the desired source does not describe", func(t *testing.T) {
		c := qt.New(t)
		request := comparison(nil, objects(held("D", &oratype.ObservedDomain{BaseType: "NUMBER"})))
		request.Desired.Coverage = schemaext.Coverage{}

		result, err := newRuntime(c).CompareObjects(t.Context(), request)

		c.Assert(err, qt.IsNil)
		c.Assert(result.Changes, qt.HasLen, 0)
	})
}

// TestCompareTypes_LeavesWhatTheReadCannotDescribe pins a declaration of a
// type the read found and could not describe, such as a domain of two columns
// or an object type with methods. Creating it would plan CREATE OR REPLACE
// TYPE over the type the schema holds, so the comparison reports it undecided
// instead. The read names the type without a schema, so each row also pins
// that a declaration naming the connection's schema finds the claim.
func TestCompareTypes_LeavesWhatTheReadCannotDescribe(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		value  schemaext.Value
		want   string
	}{
		{name: "a domain", value: &oratype.DesiredDomain{BaseType: "NUMBER"},
			want: `the domain exists and the read cannot describe it: the domain has 2 columns`},
		{name: "a domain in the connection's schema", schema: "ptah", value: &oratype.DesiredDomain{BaseType: "NUMBER"},
			want: `the domain exists and the read cannot describe it: the domain has 2 columns`},
		{name: "an object type", value: &oratype.DesiredObjectType{Attributes: []oratype.Attribute{{Name: "a", Type: "NUMBER"}}},
			want: `the object type exists and the read cannot describe it: the object type has methods`},
		{name: "an object type in the connection's schema", schema: "PTAH",
			value: &oratype.DesiredObjectType{Attributes: []oratype.Attribute{{Name: "a", Type: "NUMBER"}}},
			want:  `the object type exists and the read cannot describe it: the object type has methods`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ref := oratype.Ref(test.value.Kind(), test.schema, "kept")
			request := comparison(objects(must.Must(oratype.DesiredObject(ref, test.value))), nil)
			request.Identifiers.DefaultSchema = "PTAH"
			request.Current.Coverage = undescribedTypes(c, "KEPT")

			result, err := newRuntime(c).CompareObjects(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 1)
			c.Assert(result.Undecided[0].Subject, qt.DeepEquals, ref)
			c.Assert(result.Undecided[0].Reason, qt.Equals, test.want)
		})
	}
}

// TestCompareTypes_CreatesBesideWhatTheReadCannotDescribe is the control: a
// claim about one type leaves the creation of another as it is.
func TestCompareTypes_CreatesBesideWhatTheReadCannotDescribe(t *testing.T) {
	c := qt.New(t)
	domain := &oratype.DesiredDomain{BaseType: "NUMBER"}
	request := comparison(objects(declared("other", domain)), nil)
	request.Current.Coverage = undescribedTypes(c, "KEPT")

	result, err := newRuntime(c).CompareObjects(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.DeepEquals, []schemaext.ChangeRecord{change(oratype.DomainKind, "other", &oratype.DomainChange{After: domain})})
	c.Assert(result.Undecided, qt.HasLen, 0)
}

// undescribedTypes is a read that describes every type except a domain and an
// object type both named name, which it found and could not describe.
func undescribedTypes(c *qt.C, name string) schemaext.Coverage {
	c.Helper()
	domains := must.Must(oratype.Coverage(oratype.DomainKind, schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete},
		[]schemaext.SubjectCoverage{{Kind: oratype.DomainKind, Subject: oratype.Ref(oratype.DomainKind, "", name),
			Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the domain has 2 columns"}}}))
	objectTypes := must.Must(oratype.Coverage(oratype.ObjectTypeKind, schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete},
		[]schemaext.SubjectCoverage{{Kind: oratype.ObjectTypeKind, Subject: oratype.Ref(oratype.ObjectTypeKind, "", name),
			Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the object type has methods"}}}))
	return must.Must(domains.Combine(objectTypes))
}

// TestCompareTypes_FailurePath pins the requests the service refuses: another
// target, and a declaration among the observations.
func TestCompareTypes_FailurePath(t *testing.T) {
	t.Run("another target", func(t *testing.T) {
		c := qt.New(t)
		request := comparison(objects(declared("d", &oratype.DesiredDomain{BaseType: "NUMBER"})), nil)
		request.Target, request.Kinds = "postgres", oratype.Kinds()

		result, err := oratype.CompareService{}.CompareObjects(t.Context(), request)

		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
		c.Assert(result.Changes, qt.HasLen, 0)
	})
	t.Run("a declaration among the observations", func(t *testing.T) {
		c := qt.New(t)
		request := comparison(nil, objects(declared("d", &oratype.DesiredDomain{BaseType: "NUMBER"})))
		request.Kinds = oratype.Kinds()

		result, err := oratype.CompareService{}.CompareObjects(t.Context(), request)

		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		c.Assert(result.Changes, qt.HasLen, 0)
	})
}
