package schemadiff_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// A SQL Server role name is compared under the database collation, as a
// table name is. Measured on SQL Server 2025: CREATE ROLE [APP] beside the
// role App fails with error 15023 under a case-insensitive collation and
// succeeds under a case-sensitive one (stokaro/ptah#6741).

// roleNamedApp declares the role APP and grants it SELECT on dbo.orders.
func roleNamedApp() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		Roles:  []schemamodel.Role{{Name: "APP"}},
		Grants: []schemamodel.Grant{{Role: "APP", Privileges: []string{"SELECT"}, OnTable: "orders"}},
	}
}

// heldRoleApp is a database holding the role App with SELECT on dbo.orders.
func heldRoleApp() *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{Name: "orders"}},
		Roles:  []catalog.Role{{Name: "App"}},
		Grants: []catalog.Grant{{Role: "App", Privilege: "SELECT", ObjectType: "TABLE", Schema: "dbo", ObjectName: "orders"}},
	}
}

// roleChanges lists the roles and grants diff adds, one line each.
func roleChanges(diff *difftypes.SchemaDiff) []string {
	var changes []string
	for _, role := range diff.RolesAdded {
		changes = append(changes, "role added "+role.Name)
	}
	for _, grant := range diff.GrantsAdded {
		changes = append(changes, "grant added to "+grant.Role)
	}
	for _, grant := range diff.GrantsRemoved {
		changes = append(changes, "grant removed from "+grant.Role)
	}
	return changes
}

// TestCompareWithDatabase_SQLServerRolePairsByCollation_HappyPath declares
// the role APP, and its grant, against a database holding App.
func TestCompareWithDatabase_SQLServerRolePairsByCollation_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		database collationConnection
		want     []string
	}{
		{name: "a case-insensitive collation reads them as one role", database: caseInsensitiveDatabase},
		{
			name: "a case-sensitive collation reads them as two roles", database: caseSensitiveDatabase,
			want: []string{"role added APP", "grant added to APP"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabase(c.Context(), test.database, roleNamedApp(), heldRoleApp(), nil,
				must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(roleChanges(diff), qt.DeepEquals, test.want)
		})
	}
}

// recordingResolver answers as a server of dialect with no resolved names and
// keeps the names it was asked about.
type recordingResolver struct {
	dialect string
	asked   *[]string
}

func (r recordingResolver) Info() catalog.ServerInfo {
	return catalog.ServerInfo{Dialect: r.dialect, IdentifierSemantics: identifier.ForDialect(r.dialect)}
}

func (r recordingResolver) ResolveIdentifierSemantics(_ context.Context, names []string) (identifier.Semantics, error) {
	*r.asked = names
	return identifier.ForDialect(r.dialect), nil
}

// TestResolveIdentifierSemantics_AsksAboutRoleNamesWhereTheCatalogComparesThem
// declares the role Größe and holds the role Zähler. A SQL Server catalog
// compares role names under its collation, so its server is asked about both.
// A MySQL-family server compares role names by a rule of its own, and a
// non-ASCII name it is asked about takes one of the keys its index name probe
// has.
func TestResolveIdentifierSemantics_AsksAboutRoleNamesWhereTheCatalogComparesThem(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		asked   bool
	}{
		{name: "SQL Server", dialect: platform.SQLServer, asked: true},
		{name: "MySQL", dialect: platform.MySQL},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var asked []string

			_, err := schemadiff.ResolveIdentifierSemantics(c.Context(), recordingResolver{dialect: test.dialect, asked: &asked},
				&schemamodel.Database{Roles: []schemamodel.Role{{Name: "Größe"}}},
				&catalog.Database{Roles: []catalog.Role{{Name: "Zähler"}}}, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert([]bool{slices.Contains(asked, "Größe"), slices.Contains(asked, "Zähler")}, qt.DeepEquals,
				[]bool{test.asked, test.asked})
		})
	}
}
