package aclitem_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/aclitem"
)

// TestParseJSON_HappyPath parses the defaclacl values the engines printed
// through array_to_json, copied from the measurements in the package comment.
// Each row's want is what aclexplode answered for the same list, where the
// engine has a working aclexplode.
func TestParseJSON_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		encoded string
		want    []aclitem.Item
	}{
		{
			name:    "PostgreSQL 18.6: PUBLIC, every table privilege, a quoted grantee",
			encoded: `["=r/r_owner","r_reader=arwdDxtm/r_owner","\"odd role\"=w*/r_owner"]`,
			want: []aclitem.Item{
				{Grantee: "", Grantor: "r_owner", Privileges: []aclitem.Privilege{{Name: "SELECT"}}},
				{Grantee: "r_reader", Grantor: "r_owner", Privileges: []aclitem.Privilege{
					{Name: "INSERT"}, {Name: "SELECT"}, {Name: "UPDATE"}, {Name: "DELETE"},
					{Name: "TRUNCATE"}, {Name: "REFERENCES"}, {Name: "TRIGGER"}, {Name: "MAINTAIN"},
				}},
				{Grantee: "odd role", Grantor: "r_owner", Privileges: []aclitem.Privilege{{Name: "UPDATE", Grantable: true}}},
			},
		},
		{
			name:    "PostgreSQL 18.6: a doubled quote, and a grantor that needs quoting",
			encoded: `["\"q\"\"uote=/x\"=rwU/r_owner","r_reader=U/\"odd role\"","Upper=X*/r_owner"]`,
			want: []aclitem.Item{
				{Grantee: `q"uote=/x`, Grantor: "r_owner", Privileges: []aclitem.Privilege{
					{Name: "SELECT"}, {Name: "UPDATE"}, {Name: "USAGE"},
				}},
				{Grantee: "r_reader", Grantor: "odd role", Privileges: []aclitem.Privilege{{Name: "USAGE"}}},
				{Grantee: "Upper", Grantor: "r_owner", Privileges: []aclitem.Privilege{{Name: "EXECUTE", Grantable: true}}},
			},
		},
		{
			name:    "CockroachDB v25.4.16: text[], an unquoted dashed name, no grantor",
			encoded: `["odd-role.x=w*/","r_reader=ar/"]`,
			want: []aclitem.Item{
				{Grantee: "odd-role.x", Privileges: []aclitem.Privilege{{Name: "UPDATE", Grantable: true}}},
				{Grantee: "r_reader", Privileges: []aclitem.Privilege{{Name: "INSERT"}, {Name: "SELECT"}}},
			},
		},
		{
			name:    "CockroachDB v26.3.1: a quoted dashed name, PUBLIC, no grantor",
			encoded: `["\"r-dash\"=U*/","=r/","r_reader=CradwDtmx/"]`,
			want: []aclitem.Item{
				{Grantee: "r-dash", Privileges: []aclitem.Privilege{{Name: "USAGE", Grantable: true}}},
				{Grantee: "", Privileges: []aclitem.Privilege{{Name: "SELECT"}}},
				{Grantee: "r_reader", Privileges: []aclitem.Privilege{
					{Name: "CREATE"}, {Name: "SELECT"}, {Name: "INSERT"}, {Name: "DELETE"}, {Name: "UPDATE"},
					{Name: "TRUNCATE"}, {Name: "TRIGGER"}, {Name: "MAINTAIN"}, {Name: "REFERENCES"},
				}},
			},
		},
		{
			name:    "a column holding no list",
			encoded: `null`,
			want:    make([]aclitem.Item, 0),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := aclitem.ParseJSON(test.encoded)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestParse_EveryLetterPostgreSQLDefines pins the letter table against the
// whole of PostgreSQL 18's ACL_ALL_RIGHTS_STR, in its order, with the names
// aclexplode reports.
func TestParse_EveryLetterPostgreSQLDefines(t *testing.T) {
	c := qt.New(t)

	got, err := aclitem.Parse("r=arwdDxtXUCTcsAm/o")

	c.Assert(err, qt.IsNil)
	c.Assert(got.Privileges, qt.DeepEquals, []aclitem.Privilege{
		{Name: "INSERT"}, {Name: "SELECT"}, {Name: "UPDATE"}, {Name: "DELETE"}, {Name: "TRUNCATE"},
		{Name: "REFERENCES"}, {Name: "TRIGGER"}, {Name: "EXECUTE"}, {Name: "USAGE"}, {Name: "CREATE"},
		{Name: "TEMPORARY"}, {Name: "CONNECT"}, {Name: "SET"}, {Name: "ALTER SYSTEM"}, {Name: "MAINTAIN"},
	})
}

// TestParseJSON_FailurePath refuses what is not an ACL rather than reading part
// of it: a privilege dropped because its letter was not understood would be a
// default privilege the description silently loses.
func TestParseJSON_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		encoded string
		wantErr string
	}{
		{
			name:    "not a JSON array",
			encoded: `{r_reader=r/}`,
			wantErr: `malformed ACL item: "\{r_reader=r/\}" is not a JSON array of strings: .*`,
		},
		{
			name:    "no '=' after the grantee",
			encoded: `["r_reader"]`,
			wantErr: `malformed ACL item: "r_reader" has no '=' after the grantee`,
		},
		{
			name:    "no '/' before the grantor",
			encoded: `["r_reader=r"]`,
			wantErr: `malformed ACL item: "r_reader=r" has no '/' before the grantor`,
		},
		{
			name:    "a letter PostgreSQL does not define",
			encoded: `["r_reader=rZ/o"]`,
			wantErr: `malformed ACL item: "r_reader=rZ/o": privilege letter 'Z' is not one PostgreSQL defines`,
		},
		{
			name:    "a quoted name that is not closed",
			encoded: `["\"odd role=r/o"]`,
			wantErr: `malformed ACL item: "\\"odd role=r/o": a quoted name is not closed`,
		},
		{
			name:    "text after a quoted grantor",
			encoded: `["r=r/\"o\"x"]`,
			wantErr: `malformed ACL item: "r=r/\\"o\\"x" has "x" after the grantor`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := aclitem.ParseJSON(test.encoded)

			c.Assert(err, qt.ErrorIs, aclitem.ErrMalformed)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.IsNil)
		})
	}
}
