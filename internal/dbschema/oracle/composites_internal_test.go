package oracle

// White-box testing required: which object types this description can carry is
// decided by predicates inside the query, and the exported read returns the same
// list whether a type was excluded or was never there.

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
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

	composites, err := reader.readComposites(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(composites, qt.DeepEquals, []catalog.CompositeType{{
		Name: "ORA_POINT",
		Fields: []catalog.CompositeField{
			{Name: "X", Type: "NUMBER(10,2)"},
			{Name: "Y", Type: "NUMBER(10,2)"},
		},
	}})
}

// TestReadComposites_LeavesOutATypeWithNoAttributesRead is the guard on the
// half the query cannot express.
//
// The predicates keep the incomplete shells out, so an object type reaching
// this loop with no attribute row is one the attribute read could not see.
// Describing it as a composite with no fields would plan a CREATE OR REPLACE
// that empties a type on the server.
func TestReadComposites_LeavesOutATypeWithNoAttributesRead(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, attributelessCompositeCatalog)
	reader := NewOracleReader(db.SQL, "APP")

	composites, err := reader.readComposites(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(composites, qt.HasLen, 0)
}

// TestCompositeQuery_DeclinesEveryShapeTheModelCannotCarry pins the four
// predicates, because each of them is the difference between "not described"
// and "described wrongly".
func TestCompositeQuery_DeclinesEveryShapeTheModelCannotCarry(t *testing.T) {
	tests := []struct {
		name     string
		fragment string
	}{
		{name: "only object types", fragment: "t.typecode = 'OBJECT'"},
		{name: "no methods", fragment: "t.methods = 0"},
		{name: "no subtypes", fragment: "t.supertype_name IS NULL"},
		{name: "no incomplete shells", fragment: "t.incomplete = 'NO'"},
		{name: "scoped to one owner", fragment: "t.owner = :1"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(compositeQuery, qt.Contains, test.fragment)
			// ALL_TYPES rather than USER_TYPES, so the owner predicate is the
			// scope rather than the connection.
			c.Assert(compositeQuery, qt.Contains, "all_types")
		})
	}
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
	return compositeAnswer(query, compositeAttributeRows())
}

// attributelessCompositeCatalog reports the type and no attribute for it.
func attributelessCompositeCatalog(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	return compositeAnswer(query, nil)
}

func emptyOrCompositeCatalog(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
	folded := strings.ToLower(query)
	if strings.Contains(folded, "from all_types") || strings.Contains(folded, "from all_type_attrs") {
		return answeringCompositeCatalog(query, args)
	}
	return dbtest.QueryResult{Columns: []string{"unused"}}, nil
}

func compositeAnswer(query string, attributes [][]driver.Value) (dbtest.QueryResult, error) {
	folded := strings.ToLower(query)
	switch {
	case strings.Contains(folded, "from all_types"):
		return dbtest.QueryResult{
			Columns: []string{"type_name"},
			Rows:    [][]driver.Value{{"ORA_POINT"}},
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
