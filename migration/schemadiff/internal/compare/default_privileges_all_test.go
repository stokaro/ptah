package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// defaultRows is what the catalog read reports for the default privileges of
// app_owner's tables in app granted to app_reader, one row per privilege.
func defaultRows(privileges ...string) []catalog.DefaultPrivilege {
	rows := make([]catalog.DefaultPrivilege, 0, len(privileges))
	for _, privilege := range privileges {
		rows = append(rows, catalog.DefaultPrivilege{
			Grantor: "app_owner", Schema: "app", ObjectType: "TABLES", Grantee: "app_reader", Privilege: privilege,
		})
	}
	return rows
}

// TestDefaultPrivilegesWithSemantics_All pins how a declared ALTER DEFAULT
// PRIVILEGES ... GRANT ALL compares with the catalog read, which reports one
// row per privilege. Without the rule every run granted ALL and revoked each
// row it stood for, since the object is declared and its rows are Ptah's to
// trim (stokaro/ptah#3579).
func TestDefaultPrivilegesWithSemantics_All(t *testing.T) {
	tests := []struct {
		name        string
		rows        []catalog.DefaultPrivilege
		wantAdded   []string
		wantRemoved []string
	}{
		{name: "PostgreSQL 16 reports seven rows", rows: defaultRows(portableTable...)},
		{name: "PostgreSQL 17 reports eight", rows: defaultRows(fullTable...)},
		{name: "a file's ALL row", rows: defaultRows("ALL")},
		{
			name:      "a missing privilege plans ALL",
			rows:      defaultRows("SELECT", "INSERT"),
			wantAdded: []string{"ALL on TABLES in app for app_owner to app_reader"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{DefaultPrivileges: []schemamodel.DefaultPrivilege{{
				Grantor: "app_owner", Schema: "app", ObjectType: "TABLES", Grantee: "app_reader",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "ALL"}},
			}}}
			diff := &difftypes.SchemaDiff{}

			compare.DefaultPrivilegesWithSemantics(desired, &catalog.Database{DefaultPrivileges: test.rows}, diff,
				identifier.ForDialect(platform.Postgres))

			c.Assert(defaultPrivilegeNames(diff.DefaultPrivilegesAdded), qt.DeepEquals, nonNil(test.wantAdded))
			c.Assert(defaultPrivilegeNames(diff.DefaultPrivilegesRemoved), qt.DeepEquals, nonNil(test.wantRemoved))
		})
	}
}

// nonNil turns a row's omitted list into the empty list defaultPrivilegeNames
// returns.
func nonNil(values []string) []string {
	return append(make([]string, 0, len(values)), values...)
}
