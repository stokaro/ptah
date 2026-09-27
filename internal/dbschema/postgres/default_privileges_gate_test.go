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
// The catalog relation alone does not identify it. The role read joins
// pg_default_acl too, and so does the read of the grantees it counts, because a
// default privilege names a grantor and a grantee and a description that did
// not report them would reference roles it never defines. The role read is the
// one that selects from pg_roles; the grantee read selects the ACL alone, with
// no object type.
func isDefaultPrivilegeRead(query string) bool {
	return strings.Contains(query, "pg_default_acl") && !strings.Contains(query, "pg_roles") &&
		strings.Contains(query, "AS object_type") && !isUndescribedDefaultPrivilegeRead(query)
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

// isUndescribedDefaultPrivilegeRead names the second read over pg_default_acl,
// the one listing the rows no declaration can name. It is the one that keeps a
// row with no namespace, so it joins pg_namespace from the left.
func isUndescribedDefaultPrivilegeRead(query string) bool {
	return strings.Contains(query, "pg_default_acl") && strings.Contains(query, "LEFT JOIN pg_namespace")
}

// withDefaultPrivilegeRow answers the undescribed read with every shape it
// records, plus a FOR ALL ROLES row in a schema the read does not cover, which
// has to stay out of the list the way it stays out of the description.
func withDefaultPrivilegeRow(query string, result dbtest.QueryResult) dbtest.QueryResult {
	if isUndescribedDefaultPrivilegeRead(query) {
		return dbtest.QueryResult{
			Columns: []string{"grantor", "schema_name", "object_type"},
			Rows: [][]driver.Value{
				{"app_owner", "", "FUNCTIONS"},
				{"", "", "TYPES"},
				{"", "public", "TABLES"},
				{"", "elsewhere", "SEQUENCES"},
			},
		}
	}
	if !isDefaultPrivilegeRead(query) {
		return result
	}
	return dbtest.QueryResult{
		Columns: []string{"grantor", "schema_name", "object_type", "acl"},
		Rows:    [][]driver.Value{{"app_owner", "public", "TABLES", `["app_reader=r/app_owner"]`}},
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
//
// The undescribed rows are read under the same two keys, since they live in the
// same relation. The empty grantor is CockroachDB's FOR ALL ROLES, which the
// read leaves empty rather than naming role 0; the one in a schema the read does
// not cover is dropped, as a described row there would be.
func TestReadSchemaContext_ReadsDefaultPrivilegesUnderBothCapabilities(t *testing.T) {
	tests := []struct {
		name            string
		caps            capability.Capabilities
		want            []catalog.DefaultPrivilege
		wantUndescribed []catalog.UndescribedDefaultPrivilege
	}{
		{
			name: "role management and the catalog relation",
			caps: capability.Postgres16(),
			want: []catalog.DefaultPrivilege{{
				Grantor:    "app_owner",
				Schema:     "public",
				ObjectType: "TABLES",
				Grantee:    "app_reader",
				Privilege:  "SELECT",
			}},
			wantUndescribed: []catalog.UndescribedDefaultPrivilege{
				{Grantor: "app_owner", ObjectType: "FUNCTIONS"},
				{ObjectType: "TYPES"},
				{Schema: "public", ObjectType: "TABLES"},
			},
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
			c.Assert(schema.UndescribedDefaultPrivileges, qt.DeepEquals, test.wantUndescribed)
		})
	}
}
