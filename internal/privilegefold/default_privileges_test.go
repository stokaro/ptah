package privilegefold_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/privilegefold"
)

func defaultGranting(privileges ...string) schemamodel.DefaultPrivilege {
	declaration := schemamodel.DefaultPrivilege{Grantor: "o", Schema: "app", ObjectType: "TABLES", Grantee: "r"}
	for _, privilege := range privileges {
		declaration.Privileges = append(declaration.Privileges, schemamodel.PrivilegeGrant{Privilege: privilege})
	}
	return declaration
}

func defaultRevoking(privileges ...string) schemamodel.DefaultPrivilege {
	declaration := defaultGranting()
	declaration.Revoked = privileges
	return declaration
}

// TestMergeDefaultPrivileges pins the fold of later default privilege
// statements onto earlier ones, which a schema directory applies file by file
// as one file applies them statement by statement.
func TestMergeDefaultPrivileges(t *testing.T) {
	type view struct {
		Granted []string
		Revoked []string
	}
	tests := []struct {
		name string
		dst  []schemamodel.DefaultPrivilege
		src  []schemamodel.DefaultPrivilege
		want []view
	}{
		{
			name: "a later revoke takes a privilege out of an earlier grant",
			dst:  []schemamodel.DefaultPrivilege{defaultGranting("SELECT", "INSERT")},
			src:  []schemamodel.DefaultPrivilege{defaultRevoking("INSERT")},
			want: []view{{Granted: []string{"SELECT"}, Revoked: []string{"INSERT"}}},
		},
		{
			name: "a later grant takes a privilege out of an earlier revoke",
			dst:  []schemamodel.DefaultPrivilege{defaultRevoking("SELECT", "INSERT")},
			src:  []schemamodel.DefaultPrivilege{defaultGranting("SELECT")},
			want: []view{{Granted: []string{"SELECT"}, Revoked: []string{"INSERT"}}},
		},
		{
			name: "a revoked ALL takes every grant",
			dst:  []schemamodel.DefaultPrivilege{defaultGranting("SELECT", "INSERT")},
			src:  []schemamodel.DefaultPrivilege{defaultRevoking("ALL")},
			want: []view{{Granted: make([]string, 0), Revoked: []string{"ALL"}}},
		},
		{
			name: "another identity is appended",
			dst:  []schemamodel.DefaultPrivilege{defaultGranting("SELECT")},
			src:  []schemamodel.DefaultPrivilege{{Grantor: "o", Schema: "app", ObjectType: "SEQUENCES", Grantee: "r", Revoked: []string{"USAGE"}}},
			want: []view{{Granted: []string{"SELECT"}}, {Granted: make([]string, 0), Revoked: []string{"USAGE"}}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			before := len(test.dst)

			merged := privilegefold.MergeDefaultPrivileges(test.dst, test.src)

			got := make([]view, 0, len(merged))
			for _, declaration := range merged {
				granted := make([]string, 0, len(declaration.Privileges))
				for _, grant := range declaration.Privileges {
					granted = append(granted, grant.Privilege)
				}
				got = append(got, view{Granted: granted, Revoked: declaration.Revoked})
			}
			c.Assert(got, qt.DeepEquals, test.want)
			c.Assert(test.dst, qt.HasLen, before)
		})
	}
}
