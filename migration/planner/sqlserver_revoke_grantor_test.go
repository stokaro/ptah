package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// sqlServerReadGrant is one catalog row of SELECT given to reader.
func sqlServerReadGrant(kind, object, column, grantor string, grantable bool) catalog.Grant {
	return catalog.Grant{
		Role: "reader", Privilege: "SELECT", ObjectType: kind, ObjectName: object,
		Column: column, GrantedBy: grantor, WithOption: grantable,
	}
}

// A comparison holds one removal per permission, but SQL Server holds one
// row per grantor. A revoke must take every matching row, and take the option
// first for every grantor that gave it. The SQL reader, comparison and public
// planner all participate so a fixture cannot supply the missing expansion.
func TestGenerateSchemaDiffSQLStatements_SQLServerRevokesEveryGrantor_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, declared string
		held           []catalog.Grant
		want           []string
	}{
		{
			name: "an object granted by another principal", declared: "REVOKE SELECT ON t1 FROM reader;",
			held: []catalog.Grant{sqlServerReadGrant("TABLE", "t1", "", "other", false)},
			want: []string{"REVOKE SELECT ON [t1] FROM [reader] AS [other]"},
		},
		{
			name: "every grantor of one object", declared: "REVOKE SELECT ON t1 FROM reader;",
			held: []catalog.Grant{sqlServerReadGrant("TABLE", "t1", "", "dbo", false), sqlServerReadGrant("TABLE", "t1", "", "other", false)},
			want: []string{"REVOKE SELECT ON [t1] FROM [reader] AS [dbo]", "REVOKE SELECT ON [t1] FROM [reader] AS [other]"},
		},
		{
			name: "an unqualified declaration and a qualified catalog", declared: "REVOKE SELECT ON t1 FROM reader;",
			held: []catalog.Grant{{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "dbo", ObjectName: "t1", GrantedBy: "other"}},
			want: []string{"REVOKE SELECT ON [dbo].[t1] FROM [reader] AS [other]"},
		},
		{
			name: "a grantable row hidden by a plain row", declared: "REVOKE SELECT ON t1 FROM reader;",
			held: []catalog.Grant{sqlServerReadGrant("TABLE", "t1", "", "dbo", false), sqlServerReadGrant("TABLE", "t1", "", "other", true)},
			want: []string{
				"REVOKE GRANT OPTION FOR SELECT ON [t1] FROM [reader] CASCADE AS [other]",
				"REVOKE SELECT ON [t1] FROM [reader] AS [dbo]", "REVOKE SELECT ON [t1] FROM [reader] AS [other]",
			},
		},
		{
			name: "every grantor of one schema", declared: "REVOKE SELECT ON SCHEMA::app FROM reader;",
			held: []catalog.Grant{sqlServerReadGrant("SCHEMA", "app", "", "dbo", false), sqlServerReadGrant("SCHEMA", "app", "", "other", false)},
			want: []string{"REVOKE SELECT ON SCHEMA::[app] FROM [reader] AS [dbo]", "REVOKE SELECT ON SCHEMA::[app] FROM [reader] AS [other]"},
		},
		{
			name: "every grantor of one column", declared: "REVOKE SELECT (id) ON t1 FROM reader;",
			held: []catalog.Grant{sqlServerReadGrant("TABLE", "t1", "id", "dbo", false), sqlServerReadGrant("TABLE", "t1", "id", "other", false)},
			want: []string{"REVOKE SELECT ([id]) ON [t1] FROM [reader] AS [dbo]", "REVOKE SELECT ([id]) ON [t1] FROM [reader] AS [other]"},
		},
		{
			name: "every grantor of an option", declared: declaredSQLServerOptionRevoke,
			held: []catalog.Grant{sqlServerReadGrant("TABLE", "t1", "", "dbo", true), sqlServerReadGrant("TABLE", "t1", "", "other", true)},
			want: []string{
				"REVOKE GRANT OPTION FOR SELECT ON [t1] FROM [reader] CASCADE AS [dbo]",
				"REVOKE GRANT OPTION FOR SELECT ON [t1] FROM [reader] CASCADE AS [other]",
			},
		},
		{
			name: "an option revoke leaves a plain grant alone", declared: declaredSQLServerOptionRevoke,
			held: []catalog.Grant{sqlServerReadGrant("TABLE", "t1", "", "dbo", false), sqlServerReadGrant("TABLE", "t1", "", "other", true)},
			want: []string{"REVOKE GRANT OPTION FOR SELECT ON [t1] FROM [reader] CASCADE AS [other]"},
		},
		{
			name: "the same row reported twice", declared: "REVOKE SELECT ON t1 FROM reader;",
			held: []catalog.Grant{sqlServerReadGrant("TABLE", "t1", "", "other", false), sqlServerReadGrant("TABLE", "t1", "", "other", false)},
			want: []string{"REVOKE SELECT ON [t1] FROM [reader] AS [other]"},
		},
		{
			name: "a bracket in the grantor name", declared: "REVOKE SELECT ON t1 FROM reader;",
			held: []catalog.Grant{sqlServerReadGrant("TABLE", "t1", "", "grant]or", false)},
			want: []string{"REVOKE SELECT ON [t1] FROM [reader] AS [grant]]or]"},
		},
		{
			name: "spaces in the grantor name", declared: "REVOKE SELECT ON t1 FROM reader;",
			held: []catalog.Grant{sqlServerReadGrant("TABLE", "t1", "", " grantor ", false)},
			want: []string{"REVOKE SELECT ON [t1] FROM [reader] AS [ grantor ]"},
		},
		{
			name: "SQL quoting is part of the catalog name", declared: "REVOKE SELECT ON t1 FROM reader;",
			held: []catalog.Grant{sqlServerReadGrant("TABLE", "t1", "", "[grantor]", false)},
			want: []string{"REVOKE SELECT ON [t1] FROM [reader] AS [[grantor]]]"},
		},
		{
			name: "another object is left alone", declared: "REVOKE SELECT ON t1 FROM reader;",
			held: []catalog.Grant{sqlServerReadGrant("TABLE", "t1", "", "other", false), sqlServerReadGrant("TABLE", "t2", "", "unrelated", true)},
			want: []string{"REVOKE SELECT ON [t1] FROM [reader] AS [other]"},
		},
		{
			name: "a read with no grantor", declared: "REVOKE SELECT ON t1 FROM reader;",
			held: []catalog.Grant{sqlServerReadGrant("TABLE", "t1", "", "", false)},
			want: []string{"REVOKE SELECT ON [t1] FROM [reader]"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, _, err := sqlschema.ReadWith(t.Context(), []byte(test.declared), platform.SQLServer, nil,
				sqlschema.Options{Owners: builtintest.Runtime().SQL()})
			c.Assert(err, qt.IsNil)
			runtime, err := builtin.New()
			c.Assert(err, qt.IsNil)
			diff, err := schemadiff.CompareWithDialect(t.Context(), &desired, &catalog.Database{Grants: test.held}, platform.SQLServer, runtime)
			c.Assert(err, qt.IsNil)

			statements, err := planner.GenerateSchemaDiffSQLStatements(t.Context(), runtime, diff, platform.SQLServer)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}

const declaredSQLServerOptionRevoke = "GRANT SELECT ON t1 TO reader WITH GRANT OPTION; " +
	"REVOKE GRANT OPTION FOR SELECT ON t1 FROM reader;"

// The comparison can retain a plain row while another grantor gave the same
// permission grantable. The reach check must inspect the grantable row before
// emitting its CASCADE, which would take the onward grant the schema keeps.
func TestGenerateSchemaDiffSQLStatements_SQLServerHiddenGrantorCascades_FailurePath(t *testing.T) {
	c := qt.New(t)
	desired, _, err := sqlschema.ReadWith(t.Context(), []byte("REVOKE SELECT ON t1 FROM reader;"), platform.SQLServer, nil,
		sqlschema.Options{Owners: builtintest.Runtime().SQL()})
	c.Assert(err, qt.IsNil)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDialect(t.Context(), &desired, &catalog.Database{Grants: []catalog.Grant{
		sqlServerReadGrant("TABLE", "t1", "", "dbo", false),
		sqlServerReadGrant("TABLE", "t1", "", "other", true),
		{Role: "kept", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t1", GrantedBy: "reader"},
	}}, platform.SQLServer, runtime)
	c.Assert(err, qt.IsNil)

	statements, err := planner.GenerateSchemaDiffSQLStatements(t.Context(), runtime, diff, platform.SQLServer)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*which also takes the SELECT that "reader" granted to "kept".*`)
	c.Assert(statements, qt.IsNil)
}
