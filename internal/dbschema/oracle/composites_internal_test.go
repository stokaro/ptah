package oracle

// White-box testing required: which object types this description can carry is
// decided inside the read, and readComposites is the unit that records the
// types it cannot carry; the exported read reaches it only behind every other
// catalog query.

import (
	"database/sql/driver"
	"fmt"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/feature/oratype"
	"ptah.run/internal/dbschema/dbtest"
)

// TestReadComposites_CarriesTheAttributesInDeclarationOrder pins what the read
// takes from the catalog.
//
// The order is the assertion rather than the set: a composite's fields are
// positional, so a read that sorted them by name would describe a different
// type that happens to have the same members. ATTR_NO is what the query orders
// by, and the fixture's rows arrive in that order.
func TestReadComposites_CarriesTheAttributesInDeclarationOrder(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, answeringCompositeCatalog)
	reader := NewOracleReader(db.SQL, "APP")

	read := &catalog.Database{}
	err := reader.readComposites(t.Context(), read)

	c.Assert(err, qt.IsNil)
	objects, err := read.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.HasLen, 1)
	c.Assert(objects[0].Ref, qt.DeepEquals, oratype.Ref(oratype.ObjectTypeKind, "", "ORA_POINT"))
	c.Assert(objects[0].Value, qt.DeepEquals, &oratype.ObservedObjectType{Attributes: []oratype.Attribute{
		{Name: "X", Type: "NUMBER(10,2)"},
		{Name: "Y", Type: "NUMBER(10,2)"},
	}})
}

// TestReadComposites_RecordsWhatTheModelCannotCarry pins each shape the model
// cannot describe, and that the read records it rather than leaving it out.
//
// Left out, the claim that the read describes every object type says the type
// is absent, and a declaration of that name plans CREATE OR REPLACE TYPE over
// the one the schema holds: an object type with methods loses them, and a
// collection type becomes an object type. The type with no attribute row is
// the guard on what the read FOUND rather than on what the type IS: describing
// it as a type with no fields would plan a CREATE OR REPLACE that empties it.
func TestReadComposites_RecordsWhatTheModelCannotCarry(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		return compositeCatalog(query, [][]driver.Value{
			{"ORA_CHILD", "OBJECT", int64(0), "ORA_PARENT", "NO"},
			{"ORA_EMPTY", "OBJECT", int64(0), nil, "NO"},
			{"ORA_KEPT", "OBJECT", int64(0), nil, "NO"},
			{"ORA_LIST", "COLLECTION", int64(0), nil, "NO"},
			{"ORA_METHOD", "OBJECT", int64(1), nil, "NO"},
			{"ORA_SHELL", "OBJECT", int64(0), nil, "YES"},
		}, [][]driver.Value{
			{"ORA_CHILD", "B", "NUMBER", nil, nil, nil},
			{"ORA_KEPT", "A", "NUMBER", nil, nil, nil},
			{"ORA_METHOD", "A", "NUMBER", nil, nil, nil},
		})
	})
	reader := NewOracleReader(db.SQL, "APP")

	read := &catalog.Database{}
	err := reader.readComposites(t.Context(), read)

	c.Assert(err, qt.IsNil)
	c.Assert(read.FeatureObjects.Refs(), qt.DeepEquals, []objectidentity.ID{oratype.Ref(oratype.ObjectTypeKind, "", "ORA_KEPT")})
	c.Assert(read.FeatureCoverage.SubjectRecords(), qt.DeepEquals, []schemaext.SubjectCoverage{
		undescribedObjectType("ORA_CHILD", "the object type is a subtype of ORA_PARENT, which the object type model cannot describe"),
		undescribedObjectType("ORA_EMPTY", "the read found no attribute of the object type"),
		undescribedObjectType("ORA_LIST", "the type is a collection type, which the object type model cannot describe"),
		undescribedObjectType("ORA_METHOD", "the object type has methods, which the object type model cannot describe"),
		undescribedObjectType("ORA_SHELL", "the object type is incomplete"),
	})
	c.Assert(claimedKinds(read), qt.DeepEquals, []schemaext.Kind{oratype.ObjectTypeKind})
}

// undescribedObjectType is the claim a read records about an object type it
// found and could not describe.
func undescribedObjectType(name, reason string) schemaext.SubjectCoverage {
	return schemaext.SubjectCoverage{Kind: oratype.ObjectTypeKind, Subject: oratype.Ref(oratype.ObjectTypeKind, "", name),
		Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: reason}}
}

// TestCompositeAttributeQuery_OrdersByDeclarationPosition is the other half of
// the ordering claim, at the level the query owns it.
func TestCompositeAttributeQuery_OrdersByDeclarationPosition(t *testing.T) {
	c := qt.New(t)
	c.Assert(compositeAttributeQuery, qt.Contains, "ORDER BY a.type_name, a.attr_no")
	c.Assert(compositeAttributeQuery, qt.Not(qt.Contains), "ORDER BY a.attr_name")
}

// TestReadSchema_AsksForCompositesOnlyWhereThePresetClaimsThem counts the
// queries SENT.
//
// A skipped read and a tolerated failure both return an empty list, and only
// one of them leaves the transaction usable.
func TestReadSchema_AsksForCompositesOnlyWhereThePresetClaimsThem(t *testing.T) {
	tests := []struct {
		name      string
		caps      capability.Capabilities
		wantAsked bool
	}{
		{name: "the preset claims them", caps: capability.Oracle23(), wantAsked: true},
		{
			name:      "a preset that declines them",
			caps:      capability.Oracle23().With(capability.CompositeTypes, false),
			wantAsked: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var asked bool
			db := dbtest.Open(t, func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
				asked = asked || strings.Contains(strings.ToLower(query), "from all_types")
				return emptyOrCompositeCatalog(query, args)
			})
			reader := NewOracleReaderWithCapabilities(db.SQL, "APP", test.caps)

			_, err := reader.ReadSchemaContext(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(asked, qt.Equals, test.wantAsked)
		})
	}
}

// answeringCompositeCatalog answers each view with the projection its query
// asks for, so a query that stopped selecting a column fails here rather than
// being handed the same rows.
func answeringCompositeCatalog(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	return compositeCatalog(query, [][]driver.Value{{"ORA_POINT", "OBJECT", int64(0), nil, "NO"}}, compositeAttributeRows())
}

func emptyOrCompositeCatalog(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
	folded := strings.ToLower(query)
	if strings.Contains(folded, "from all_types") || strings.Contains(folded, "from all_type_attrs") {
		return answeringCompositeCatalog(query, args)
	}
	return dbtest.QueryResult{Columns: []string{"unused"}}, nil
}

// compositeCatalog answers ALL_TYPES with types and ALL_TYPE_ATTRS with
// attributes.
func compositeCatalog(query string, types, attributes [][]driver.Value) (dbtest.QueryResult, error) {
	folded := strings.ToLower(query)
	switch {
	case strings.Contains(folded, "from all_types"):
		return dbtest.QueryResult{
			Columns: []string{"type_name", "typecode", "methods", "supertype_name", "incomplete"},
			Rows:    types,
		}, nil
	case strings.Contains(folded, "from all_type_attrs"):
		return dbtest.QueryResult{
			Columns: []string{"type_name", "attr_name", "attr_type_name", "length", "precision", "scale"},
			Rows:    attributes,
		}, nil
	}
	return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
}

// compositeAttributeRows is what ALL_TYPE_ATTRS answered for the type, in
// ATTR_NO order.
func compositeAttributeRows() [][]driver.Value {
	return [][]driver.Value{
		{"ORA_POINT", "X", "NUMBER", nil, int64(10), int64(2)},
		{"ORA_POINT", "Y", "NUMBER", nil, int64(10), int64(2)},
	}
}

// TestReadSchema_ClaimsOnlyTheTypeKindsThePresetHolds pins the coverage a read
// records for the Oracle type owner: complete knowledge of each kind the
// release line holds, and no claim at all about a kind it does not, so an
// export writes no not-described line for domains a server cannot have.
func TestReadSchema_ClaimsOnlyTheTypeKindsThePresetHolds(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		want []schemaext.Kind
	}{
		{name: "Oracle 23", caps: capability.Oracle23(), want: []schemaext.Kind{oratype.DomainKind, oratype.ObjectTypeKind}},
		{name: "Oracle 21", caps: capability.Oracle21(), want: []schemaext.Kind{oratype.ObjectTypeKind}},
		{name: "a line without object types", caps: capability.Oracle23().With(capability.CompositeTypes, false),
			want: []schemaext.Kind{oratype.DomainKind}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, emptyOrCompositeCatalog)

			read, err := NewOracleReaderWithCapabilities(db.SQL, "APP", test.caps).ReadSchemaContext(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(claimedKinds(read), qt.DeepEquals, test.want)
		})
	}
}

// claimedKinds lists the Oracle type models a read claims complete knowledge
// of, in model order.
func claimedKinds(read *catalog.Database) []schemaext.Kind {
	var kinds []schemaext.Kind
	for _, record := range read.FeatureCoverage.KindRecords() {
		if record.Knowledge.State == schemaext.Complete && slices.Contains(oratype.Kinds(), record.Model.Kind) {
			kinds = append(kinds, record.Model.Kind)
		}
	}
	slices.SortFunc(kinds, func(a, b schemaext.Kind) int {
		return slices.Index(oratype.Kinds(), a) - slices.Index(oratype.Kinds(), b)
	})
	return kinds
}
