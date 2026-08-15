package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// scopedDesiredState declares one PostgreSQL-only function and one role, both
// scoped to postgres, against an empty database.
func scopedDesiredState() *goschema.Database {
	return &goschema.Database{
		Functions: []goschema.Function{{
			StructName: "Fn",
			Name:       "get_current_tenant_id",
			Returns:    "TEXT",
			Language:   "plpgsql",
			Body:       "BEGIN RETURN 'x'; END;",
			Dialects:   []string{"postgres"},
		}},
		Roles: []goschema.Role{
			{StructName: "Rol", Name: "app_reader", Inherit: true, Dialects: []string{"postgres"}},
		},
	}
}

// TestCompare_AScopedObjectIsNotReportedAsAddedOnATargetItDoesNotName is the
// convergence guarantee, stated where convergence is decided.
//
// The comparator is what made a scoped-away object a permanent diff: the object
// was in the desired state, never in the database, and therefore in
// FunctionsAdded on every run. Measured on MariaDB 12.3.2 before this existed,
// `schema apply` exited 0 having created nothing and the very next
// `schema compare --exit-code` returned 1 naming the same object, forever.
func TestCompare_AScopedObjectIsNotReportedAsAddedOnATargetItDoesNotName(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		added   int
	}{
		{name: "the named dialect still sees the object", dialect: "postgres", added: 1},
		{name: "an accepted spelling of the named dialect", dialect: "postgresql", added: 1},
		{name: "mysql", dialect: "mysql", added: 0},
		{name: "mariadb", dialect: "mariadb", added: 0},
		{name: "sqlite", dialect: "sqlite", added: 0},
		{name: "a postgres-family target the scope did not name", dialect: "yugabytedb", added: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := schemadiff.CompareWithDialect(scopedDesiredState(), &types.DBSchema{}, test.dialect)

			c.Assert(diff.FunctionsAdded, qt.HasLen, test.added)
			c.Assert(diff.RolesAdded, qt.HasLen, test.added)
		})
	}
}

// TestCompare_AnUnscopedObjectIsStillReportedAsAdded is the compatibility
// control. Without it, a projection that emptied every desired state would pass
// the table above on five of its six rows.
func TestCompare_AnUnscopedObjectIsStillReportedAsAdded(t *testing.T) {
	c := qt.New(t)

	unscoped := scopedDesiredState()
	unscoped.Functions[0].Dialects = nil
	unscoped.Roles[0].Dialects = nil

	diff := schemadiff.CompareWithDialect(unscoped, &types.DBSchema{}, "mariadb")

	c.Assert(diff.FunctionsAdded, qt.HasLen, 1)
	c.Assert(diff.RolesAdded, qt.HasLen, 1)
}

// TestCompare_ADialectlessComparisonKeepsEveryScopedObject pins the one
// comparison that cannot project: with no dialect there is no target to measure
// a scope against, and dropping objects on a guess would report a synced schema
// for work nobody decided to skip.
func TestCompare_ADialectlessComparisonKeepsEveryScopedObject(t *testing.T) {
	c := qt.New(t)

	opts := config.DefaultCompareOptions()
	opts.Dialect = ""

	diff := schemadiff.CompareWithOptions(scopedDesiredState(), &types.DBSchema{}, opts)

	c.Assert(diff.FunctionsAdded, qt.HasLen, 1)
	c.Assert(diff.RolesAdded, qt.HasLen, 1)
}
