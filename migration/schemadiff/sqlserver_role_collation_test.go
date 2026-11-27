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
	"ptah.run/core/ptaherr"
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

// twoRoles declares the roles App and APP, which a case-insensitive collation
// reads as one role.
func twoRoles(first, second string) *schemamodel.Database {
	return &schemamodel.Database{Roles: []schemamodel.Role{{Name: first}, {Name: second}}}
}

// TestValidateDesiredSchema_SQLServerRoleNamesakes_FailurePath declares two
// roles SQL Server may read as one. The comparison pairs one of them with the
// role the database holds and plans CREATE ROLE for the other, which fails
// with error 15023 under a case-insensitive collation.
func TestValidateDesiredSchema_SQLServerRoleNamesakes_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
		want    string
	}{
		{
			name:    "two roles whose names differ only in case",
			desired: twoRoles("App", "APP"),
			want:    `target roles App and APP may have the same catalog identity`,
		},
		{
			name:    "a non-ASCII role offline",
			desired: twoRoles("reader", "Größe"),
			want: `without a connection Ptah cannot tell whether target roles reader and Größe are one name ` +
				`under the database collation`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := schemadiff.ValidateDesiredSchema(c.Context(), must.Must(builtin.New()), test.desired,
				catalog.ServerInfo{Dialect: platform.SQLServer})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `.*`+test.want+`.*`)
		})
	}
}

// TestCompareWithDatabase_SQLServerRoleNamesakes_FailurePath declares App and
// APP on a server that reads them as one role.
func TestCompareWithDatabase_SQLServerRoleNamesakes_FailurePath(t *testing.T) {
	c := qt.New(t)

	diff, err := schemadiff.CompareWithDatabase(c.Context(), caseInsensitiveDatabase, twoRoles("App", "APP"),
		&catalog.Database{}, nil, must.Must(builtin.New()))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*target roles App and APP may have the same catalog identity.*`)
	c.Assert(diff, qt.IsNil)
}

// TestValidateDesiredSchema_RoleNames_HappyPath declares roles that are two
// names, or that a target compares by a rule its descriptor states.
func TestValidateDesiredSchema_RoleNames_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		desired *schemamodel.Database
	}{
		{name: "two ASCII roles offline", dialect: platform.SQLServer, desired: twoRoles("reader", "writer")},
		{name: "one role declared twice", dialect: platform.SQLServer, desired: twoRoles("App", "App")},
		{name: "PostgreSQL roles that differ only in case", dialect: platform.Postgres, desired: twoRoles("App", "APP")},
		// Oracle stores an unquoted role name in upper case, and the
		// comparison keys a role by that name, so App and APP declare the one
		// role APP twice. An Oracle role carries no attributes, so nothing
		// differs between the two declarations.
		{name: "an Oracle role declared under two cases", dialect: platform.Oracle, desired: twoRoles("App", "APP")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := schemadiff.ValidateDesiredSchema(c.Context(), must.Must(builtin.New()), test.desired,
				catalog.ServerInfo{Dialect: test.dialect})

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestCompareWithDatabase_SQLServerRoleNames_HappyPath is the control for the
// connected refusal: a case-sensitive server reads App and APP as two roles.
func TestCompareWithDatabase_SQLServerRoleNames_HappyPath(t *testing.T) {
	c := qt.New(t)

	diff, err := schemadiff.CompareWithDatabase(c.Context(), caseSensitiveDatabase, twoRoles("App", "APP"),
		&catalog.Database{}, nil, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(roleChanges(diff), qt.DeepEquals, []string{"role added APP", "role added App"})
}
