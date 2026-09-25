package privilegefold_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/google/go-cmp/cmp/cmpopts"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/privilegefold"
)

func grant(role, table string, privileges ...string) schemamodel.Grant {
	return schemamodel.Grant{Role: role, OnTable: table, Privileges: privileges}
}

// TestMerge_HappyPath pins the fold of one source onto another as though its
// statements ran later: what src revokes leaves dst's grants, and what src
// grants leaves dst's revoked set.
func TestMerge_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		dst         schemamodel.Database
		src         schemamodel.Database
		wantGrants  []schemamodel.Grant
		wantRevoked []schemamodel.Grant
	}{
		{
			name:        "a later revoke takes one privilege out of an earlier grant",
			dst:         schemamodel.Database{Grants: []schemamodel.Grant{grant("r", "t", "SELECT", "INSERT")}},
			src:         schemamodel.Database{RevokedGrants: []schemamodel.Grant{grant("r", "t", "INSERT")}},
			wantGrants:  []schemamodel.Grant{grant("r", "t", "SELECT")},
			wantRevoked: []schemamodel.Grant{grant("r", "t", "INSERT")},
		},
		{
			name:       "a later grant takes the privilege out of the revoked set",
			dst:        schemamodel.Database{RevokedGrants: []schemamodel.Grant{grant("r", "t", "SELECT")}},
			src:        schemamodel.Database{Grants: []schemamodel.Grant{grant("r", "t", "SELECT")}},
			wantGrants: []schemamodel.Grant{grant("r", "t", "SELECT")},
		},
		{
			name: "a later GRANT ALL takes every revoked privilege of its grantee on its object",
			dst: schemamodel.Database{RevokedGrants: []schemamodel.Grant{
				grant("r", "t", "SELECT"), grant("r", "t", "DELETE"), grant("other", "t", "SELECT"),
			}},
			src:         schemamodel.Database{Grants: []schemamodel.Grant{grant("r", "t", "ALL")}},
			wantGrants:  []schemamodel.Grant{grant("r", "t", "ALL")},
			wantRevoked: []schemamodel.Grant{grant("other", "t", "SELECT")},
		},
		{
			name:        "a revoke is recorded once however often it is repeated",
			dst:         schemamodel.Database{RevokedGrants: []schemamodel.Grant{grant("r", "t", "SELECT")}},
			src:         schemamodel.Database{RevokedGrants: []schemamodel.Grant{grant("r", "t", "SELECT")}},
			wantRevoked: []schemamodel.Grant{grant("r", "t", "SELECT")},
		},
		{
			name:        "a revoke naming no privilege is carried as it came",
			src:         schemamodel.Database{RevokedGrants: []schemamodel.Grant{{Role: "r", OnTable: "t"}}},
			wantRevoked: []schemamodel.Grant{{Role: "r", OnTable: "t"}},
		},
		{
			name: "a revoke on another object or grantee leaves the grant alone",
			dst:  schemamodel.Database{Grants: []schemamodel.Grant{grant("r", "t", "SELECT")}},
			src: schemamodel.Database{RevokedGrants: []schemamodel.Grant{
				grant("r", "u", "SELECT"), grant("other", "t", "SELECT"),
			}},
			wantGrants:  []schemamodel.Grant{grant("r", "t", "SELECT")},
			wantRevoked: []schemamodel.Grant{grant("r", "u", "SELECT"), grant("other", "t", "SELECT")},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			dst := test.dst
			privilegefold.Merge(&dst, &test.src)

			// Empty and nil are one answer here: whether a fold that removed
			// everything leaves a slice behind is not what these rows are about.
			c.Assert(dst.Grants, qt.CmpEquals(cmpopts.EquateEmpty()), test.wantGrants)
			c.Assert(dst.RevokedGrants, qt.CmpEquals(cmpopts.EquateEmpty()), test.wantRevoked)
		})
	}
}

// TestWithout_LeavesItsInputAlone pins that Without returns a new slice. A
// caller folding one source onto another still holds the first, and a fold
// that rewrote it in place would change a database nobody asked to change.
func TestWithout_LeavesItsInputAlone(t *testing.T) {
	c := qt.New(t)
	grants := []schemamodel.Grant{grant("r", "t", "SELECT", "INSERT")}

	got := privilegefold.Without(grants, "r", "TABLE t", "INSERT")

	c.Assert(got, qt.DeepEquals, []schemamodel.Grant{grant("r", "t", "SELECT")})
	c.Assert(grants, qt.DeepEquals, []schemamodel.Grant{grant("r", "t", "SELECT", "INSERT")})
}
