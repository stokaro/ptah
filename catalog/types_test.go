package catalog_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
)

func TestDBSchema_RolesOutOfScopeIsNeverSerialized(t *testing.T) {
	c := qt.New(t)

	// The roles the description leaves out are role names from outside the
	// inspected scope -- on a shared PostgreSQL server, other tenants' role
	// names. Reporting them is the disclosure stokaro/ptah#1267 is about, so
	// they exist for the comparator and for nothing else.
	//
	// What this protects, measured rather than assumed: the schema
	// fingerprint that binds a migration plan to live state, and the golden
	// snapshots an out-of-module test helper keeps. Both serialize a Database
	// through encoding/json, so a
	// serialized field would move a fingerprint whenever an unrelated database
	// on the same server gained a role. It is NOT what keeps the field out of
	// `ptah-compat schema inspect --format '{{ json . }}'`: that surface emits
	// the Atlas-shaped document (schemas and tables), and does not serialize a
	// Database at all.
	described := catalog.Database{
		Roles: []catalog.Role{{Name: "described_role"}},
	}
	withOutOfScope := described
	withOutOfScope.RolesOutOfScope = []catalog.Role{{Name: "other_tenants_role"}}

	describedJSON, err := json.Marshal(described)
	c.Assert(err, qt.IsNil)
	withOutOfScopeJSON, err := json.Marshal(withOutOfScope)
	c.Assert(err, qt.IsNil)

	c.Assert(string(withOutOfScopeJSON), qt.Not(qt.Contains), "other_tenants_role")
	c.Assert(string(withOutOfScopeJSON), qt.Contains, "described_role")
	// Byte-identical, so a schema fingerprint cannot move because a role was
	// created in some unrelated database on the same server.
	c.Assert(string(withOutOfScopeJSON), qt.Equals, string(describedJSON))
}

func TestDBConstraint_ColumnSlicesFallbackToLegacyFields(t *testing.T) {
	c := qt.New(t)

	legacy := catalog.Constraint{
		ColumnName:    "tenant_id",
		ForeignColumn: new("id"),
	}
	c.Assert(legacy.ColumnNamesOrDefault(), qt.DeepEquals, []string{"tenant_id"})
	c.Assert(legacy.ForeignColumnsOrDefault(), qt.DeepEquals, []string{"id"})

	composite := catalog.Constraint{
		ColumnName:     "tenant_id",
		ColumnNames:    []string{"tenant_id", "owner_id"},
		ForeignColumn:  new("tenant_id"),
		ForeignColumns: []string{"tenant_id", "id"},
	}
	c.Assert(composite.ColumnNamesOrDefault(), qt.DeepEquals, []string{"tenant_id", "owner_id"})
	c.Assert(composite.ForeignColumnsOrDefault(), qt.DeepEquals, []string{"tenant_id", "id"})
}

// TestFunction_DropIdentityKeepsAnEmptyListApartFromNoList is the distinction a
// DROP statement spells.
//
// `DROP FUNCTION f()` names the overload of f that takes no arguments, while
// `DROP FUNCTION f` names whichever routine of that name the server can
// resolve, and it refuses that where the name is overloaded. A reader that
// captured an empty identity has therefore said something, and a reader that
// captured none has not.
func TestFunction_DropIdentityKeepsAnEmptyListApartFromNoList(t *testing.T) {
	tests := []struct {
		name     string
		function catalog.Function
		want     *string
	}{
		{
			name: "the identity a reader captured",
			function: catalog.Function{
				Name: "f", IdentityArguments: new("n integer"), Parameters: "n integer DEFAULT 1",
			},
			want: new("n integer"),
		},
		{
			name:     "an empty identity is a list",
			function: catalog.Function{Name: "f", IdentityArguments: new("")},
			want:     new(""),
		},
		{
			name:     "no identity falls back to the recorded parameters",
			function: catalog.Function{Name: "f", Parameters: "IN n integer"},
			want:     new("IN n integer"),
		},
		{
			name:     "a routine nothing recorded carries no list at all",
			function: catalog.Function{Name: "f"},
			want:     nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.function.DropIdentity(), qt.DeepEquals, test.want)
		})
	}
}
