package postgres_test

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/postgres"
)

// isDefaultPrivilegeRead names the default-privilege read among the two dozen
// statements a full schema read sends.
//
// The catalog relation alone does not identify it: the role read joins
// pg_default_acl too, because a default privilege names a grantor and a grantee
// and a description that did not report them would reference roles it never
// defines. The role read is the one that selects from pg_roles.
func isDefaultPrivilegeRead(query string) bool {
	return strings.Contains(query, "pg_default_acl") && !strings.Contains(query, "pg_roles")
}

// answersOneDefaultPrivilege answers a full ReadSchemaContext, giving the
// default-privilege read one row so the read is observable in what comes back
// rather than in the text of a statement.
func answersOneDefaultPrivilege(asked *[]string) dbtest.QueryHandler {
	base := catalogAnswers(intact, asked)
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		result, err := base(query, args)
		return withDefaultPrivilegeRow(query, result), err
	}
}

func withDefaultPrivilegeRow(query string, result dbtest.QueryResult) dbtest.QueryResult {
	if !isDefaultPrivilegeRead(query) {
		return result
	}
	return dbtest.QueryResult{
		Columns: []string{
			"grantor", "schema_name", "object_type", "grantee", "privilege", "with_option",
		},
		Rows: [][]driver.Value{{"app_owner", "public", "TABLES", "app_reader", "SELECT", false}},
	}
}

// TestReadSchemaContext_ReadsDefaultPrivilegesUnderBothCapabilities pins that
// the family answers to two keys rather than one.
//
// RoleManagement says the server models roles and object privileges; every
// engine Ptah supports for roles satisfies it in its own vocabulary.
// CatalogDefaultPrivileges says something narrower and PostgreSQL-shaped: this
// server has pg_default_acl. A missing relation does not parse, so a read gated
// on the wider key alone costs the whole description on a server that manages
// roles without that catalog, and one gated on the narrower key alone asks a
// server that models no roles at all.
func TestReadSchemaContext_ReadsDefaultPrivilegesUnderBothCapabilities(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		want []catalog.DefaultPrivilege
	}{
		{
			name: "role management and the catalog relation",
			caps: capability.Postgres16(),
			want: []catalog.DefaultPrivilege{{
				Grantor:    "app_owner",
				ObjectType: "TABLES",
				Grantee:    "app_reader",
				Privilege:  "SELECT",
			}},
		},
		{
			name: "role management without the catalog relation",
			caps: capability.Postgres16().With(capability.CatalogDefaultPrivileges, false),
			want: nil,
		},
		{
			name: "the catalog relation without role management",
			caps: capability.Postgres16().With(capability.RoleManagement, false),
			want: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var asked []string
			db := dbtest.Open(c, answersOneDefaultPrivilege(&asked))
			reader := postgres.NewPostgreSQLReaderWithCapabilities(db.SQL, "public", test.caps)

			schema, err := reader.ReadSchemaContext(c.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(schema, qt.IsNotNil)
			c.Assert(schema.DefaultPrivileges, qt.DeepEquals, test.want)
		})
	}
}
