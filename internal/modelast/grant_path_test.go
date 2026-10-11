package modelast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/modelast"
)

// grantNames walks database for target with lowering and lists each grant's
// and revoke's object as the render names it.
func grantNames(c *qt.C, database schemamodel.Database, target string, lowering modelast.Lowering) []string {
	c.Helper()
	var names []string
	err := modelast.WalkDatabase(database, target, func(node ast.Node) error {
		switch typed := node.(type) {
		case *ast.GrantPrivilegeNode:
			names = append(names, "GRANT "+typed.ObjectName)
		case *ast.RevokePrivilegeNode:
			names = append(names, "REVOKE "+typed.ObjectName)
		}
		return nil
	}, lowering)
	c.Assert(err, qt.IsNil)
	return names
}

func grantDatabase() schemamodel.Database {
	return schemamodel.Database{
		Grants:        []schemamodel.Grant{{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "orders"}},
		RevokedGrants: []schemamodel.Grant{{Role: "writer", Privileges: []string{"INSERT"}, OnTable: "orders"}},
	}
}

// TestWalkDatabase_NamesGrantsByTheLoweringsGrantPath names a grant's and a
// revoke's object by the target's GrantPath, handed the object and the
// database path the lowering carries. The target is no built-in one: the
// hook, not the target's name, decides.
func TestWalkDatabase_NamesGrantsByTheLoweringsGrantPath(t *testing.T) {
	c := qt.New(t)
	var calls []string
	lowering := modelast.Lowering{Context: t.Context(), DatabasePath: "/root/db",
		GrantPath: func(objectType, objectName, databasePath string) string {
			calls = append(calls, objectType+" "+objectName+" "+databasePath)
			return databasePath + "/" + objectName
		}}

	names := grantNames(c, grantDatabase(), "custom", lowering)

	c.Assert(names, qt.DeepEquals, []string{"REVOKE /root/db/orders", "GRANT /root/db/orders"})
	c.Assert(calls, qt.DeepEquals, []string{"TABLE orders /root/db", "TABLE orders /root/db"})
}

// TestWalkDatabase_KeepsGrantNamesWithoutAPathOrAHook keeps every grant's name
// where the lowering has no GrantPath, and where it has one but no database
// path, which is what a declaration renders with.
func TestWalkDatabase_KeepsGrantNamesWithoutAPathOrAHook(t *testing.T) {
	tests := []struct {
		name     string
		lowering modelast.Lowering
	}{
		{name: "no hook", lowering: modelast.Lowering{DatabasePath: "/root/db"}},
		{name: "no database path", lowering: modelast.Lowering{
			GrantPath: func(_, objectName, databasePath string) string { return databasePath + "/" + objectName }}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			lowering := test.lowering
			lowering.Context = t.Context()

			names := grantNames(c, grantDatabase(), "custom", lowering)

			c.Assert(names, qt.DeepEquals, []string{"REVOKE orders", "GRANT orders"})
		})
	}
}
