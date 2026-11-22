package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// createsT2RevokingSelect is a schema that creates t2 and revokes SELECT on it
// from reader.
func createsT2RevokingSelect() *schemamodel.Database {
	return &schemamodel.Database{
		Tables:        []schemamodel.Table{{StructName: "T2", Name: "t2"}},
		Fields:        []schemamodel.Field{{StructName: "T2", Name: "id", Type: "INT", Primary: true}},
		RevokedGrants: []schemamodel.Grant{{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "t2"}},
	}
}

// TestCompareWithDatabaseInfoRevokesOnACreatedTableOnlyWhereItHasSomethingToTake
// wires the target's answer into the comparison entry point: a schema that
// creates t2 and revokes SELECT on it from a role plans the revoke on
// PostgreSQL, where default privileges may hand t2 to the role, and not on
// MySQL, MariaDB or Oracle, where a REVOKE on a table takes only a grant on
// that table and the role holds none on t2. A read that does not say which
// grants cover every table plans it there too.
func TestCompareWithDatabaseInfoRevokesOnACreatedTableOnlyWhereItHasSomethingToTake(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		database    *catalog.Database
		wantRemoved int
	}{
		{name: "postgres", dialect: "postgres", database: &catalog.Database{WideGrants: make([]catalog.WideGrant, 0)}, wantRemoved: 1},
		{name: "mysql", dialect: "mysql", database: &catalog.Database{WideGrants: make([]catalog.WideGrant, 0)}},
		{name: "mariadb", dialect: "mariadb", database: &catalog.Database{WideGrants: make([]catalog.WideGrant, 0)}},
		{name: "oracle", dialect: "oracle", database: &catalog.Database{WideGrants: make([]catalog.WideGrant, 0)}},
		{name: "mysql read without the wide grants", dialect: "mysql", database: &catalog.Database{}, wantRemoved: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				t.Context(), createsT2RevokingSelect(), test.database, catalog.ServerInfo{Dialect: test.dialect}, nil, must.Must(builtin.New()),
			)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesAdded, qt.HasLen, 1)
			c.Assert(diff.TablesAdded[0].Name, qt.Equals, "t2")
			c.Assert(diff.GrantsRemoved, qt.HasLen, test.wantRemoved)
		})
	}
}

// TestCompareWithDatabaseInfoRefusesARevokeAWiderGrantKeeps wires the refusal
// into the comparison entry point, for a table the plan creates.
func TestCompareWithDatabaseInfoRefusesARevokeAWiderGrantKeeps(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		database *catalog.Database
		want     string
	}{
		{
			name:    "a grant on the database",
			dialect: "mysql",
			database: &catalog.Database{
				Grants:     []catalog.Grant{{Role: "reader", Privilege: "SELECT", ObjectType: "SCHEMA", ObjectName: "d2"}},
				WideGrants: make([]catalog.WideGrant, 0),
			},
			want: `(?s)role "reader" holds SELECT ON d2\.\*, which covers table t2, so a revoke on the table cannot take it.*`,
		},
		{
			name:     "a system privilege",
			dialect:  "oracle",
			database: &catalog.Database{WideGrants: []catalog.WideGrant{{Role: "reader", Privilege: "SELECT", Grant: "SELECT ANY TABLE"}}},
			want:     `(?s)role "reader" holds SELECT ANY TABLE, which covers table t2, so a revoke on the table cannot take it.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				t.Context(), createsT2RevokingSelect(), test.database, catalog.ServerInfo{Dialect: test.dialect}, nil, must.Must(builtin.New()),
			)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(diff, qt.IsNil)
		})
	}
}
