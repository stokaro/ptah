package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
)

// TestParseSource_DefaultPrivilegeRevoked_HappyPath pins the revoked
// attribute: privileges the grantee must not hold by default, beside or
// instead of the ones it is granted.
func TestParseSource_DefaultPrivilegeRevoked_HappyPath(t *testing.T) {
	c := qt.New(t)
	source := `package models

//ptah:schema:defaultprivilege for_role="app_owner" schema="app" object_type="TABLES" grantee="app_reader" privileges="SELECT" revoked="insert, update"
//ptah:schema:defaultprivilege for_role="app_owner" schema="app" object_type="FUNCTIONS" grantee="PUBLIC" revoked="EXECUTE"
type AccessControl struct{}
`

	db := mustParseSource(c, "access.go", source)

	c.Assert(db.DefaultPrivileges, qt.HasLen, 2)
	c.Assert(db.DefaultPrivileges[0].Revoked, qt.DeepEquals, []string{"INSERT", "UPDATE"})
	c.Assert(db.DefaultPrivileges[0].Privileges, qt.HasLen, 1)
	c.Assert(db.DefaultPrivileges[1].Revoked, qt.DeepEquals, []string{"EXECUTE"})
	c.Assert(db.DefaultPrivileges[1].Privileges, qt.HasLen, 0)
}

// TestParseSource_DefaultPrivilegeRevoked_FailurePath pins the directives
// refused: one granting and revoking nothing, and a package granting and
// revoking one privilege of one identity.
func TestParseSource_DefaultPrivilegeRevoked_FailurePath(t *testing.T) {
	c := qt.New(t)
	source := "package models\n\n" +
		`//ptah:schema:defaultprivilege for_role="o" schema="app" object_type="TABLES" grantee="r"` +
		"\ntype AccessControl struct{}\n"

	db, err := goschema.ParseSource("access.go", source)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrMissingRequiredAttribute)
	c.Assert(err, qt.ErrorMatches, `missing required annotation attribute "privileges" on //ptah:schema:defaultprivilege at .*: a default privilege grants privileges, revokes them, or both`)
	c.Assert(db.DefaultPrivileges, qt.IsNil)
}

// TestParseDir_RefusesADefaultPrivilegeBothGrantedAndRevoked drives the
// contradiction across two files of one package.
func TestParseDir_RefusesADefaultPrivilegeBothGrantedAndRevoked(t *testing.T) {
	c := qt.New(t)
	dir := c.TB.TempDir()
	writeGoFile(c, dir, "a.go", "package models\n\n"+
		`//ptah:schema:defaultprivilege for_role="o" schema="app" object_type="TABLES" grantee="r" privileges="SELECT,INSERT"`+
		"\ntype Granted struct{}\n")
	writeGoFile(c, dir, "b.go", "package models\n\n"+
		`//ptah:schema:defaultprivilege for_role="o" schema="app" object_type="TABLES" grantee="r" revoked="INSERT"`+
		"\ntype Revoked struct{}\n")

	db, err := goschema.ParseDir(dir)

	c.Assert(err, qt.ErrorMatches, `(?s).*default privilege INSERT on TABLES in schema app for role o is both granted to and revoked from "r"; declare one or the other.*`)
	c.Assert(db, qt.IsNil)
}
