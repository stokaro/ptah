//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
)

// grantedCreation creates main.users and grants SELECT on it to app in the
// same plan.
func grantedCreation() *difftypes.SchemaDiff {
	rules := comparisonRules()
	return &difftypes.SchemaDiff{IdentifierSemantics: &rules,
		TablesAdded: difftypes.TableChanges{{Name: "main.users", Table: schemamodel.Table{Name: "users", Schema: "main"},
			Fields: []schemamodel.Field{{Name: "id", Type: "INTEGER"}}}},
		GrantsAdded: []difftypes.GrantRef{{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "main.users"}},
	}
}

// A grant on a table that a provider owning the table lifecycle creates in the
// same plan follows the creation. The grant reads the table and changes its
// access, so the provider's creation stays the table's only writer.
func TestProviderOwnedLifecycleGrantsOnACreatedTable_HappyPath(t *testing.T) {
	c := qt.New(t)
	statements, err := renderedPlan(c, "owned-comments", grantedCreation())
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		`CREATE TABLE "users" ("id" INTEGER NOT NULL PRIMARY KEY);`,
		`GRANT SELECT ON TABLE "users" TO "app";`,
	})
}
