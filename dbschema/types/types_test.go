package types_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema/types"
)

func TestDBSchema_RolesOutOfScopeIsNeverSerialized(t *testing.T) {
	c := qt.New(t)

	// The roles the description leaves out are role names from outside the
	// inspected scope -- on a shared PostgreSQL server, other tenants' role
	// names. Reporting them is the disclosure stokaro/ptah#1267 is about, so
	// they exist for the comparator and for nothing else.
	//
	// What this protects, measured rather than assumed: the schema
	// fingerprint that binds a migration plan to live state, and testkit's
	// golden snapshots. Both serialize a DBSchema through encoding/json, so a
	// serialized field would move a fingerprint whenever an unrelated database
	// on the same server gained a role. It is NOT what keeps the field out of
	// `ptah-compat schema inspect --format '{{ json . }}'`: that surface emits
	// the Atlas-shaped document (schemas and tables), and does not serialize a
	// DBSchema at all.
	described := types.DBSchema{
		Roles: []types.DBRole{{Name: "described_role"}},
	}
	withOutOfScope := described
	withOutOfScope.RolesOutOfScope = []types.DBRole{{Name: "other_tenants_role"}}

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

	legacy := types.DBConstraint{
		ColumnName:    "tenant_id",
		ForeignColumn: new("id"),
	}
	c.Assert(legacy.ColumnNamesOrDefault(), qt.DeepEquals, []string{"tenant_id"})
	c.Assert(legacy.ForeignColumnsOrDefault(), qt.DeepEquals, []string{"id"})

	composite := types.DBConstraint{
		ColumnName:     "tenant_id",
		ColumnNames:    []string{"tenant_id", "owner_id"},
		ForeignColumn:  new("tenant_id"),
		ForeignColumns: []string{"tenant_id", "id"},
	}
	c.Assert(composite.ColumnNamesOrDefault(), qt.DeepEquals, []string{"tenant_id", "owner_id"})
	c.Assert(composite.ForeignColumnsOrDefault(), qt.DeepEquals, []string{"tenant_id", "id"})
}
