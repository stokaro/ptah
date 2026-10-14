package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/feature/pgdefaultprivilege"
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

	db, err := goschema.ParseSource(defaultPrivilegeOwners, "access.go", source)
	c.Assert(err, qt.IsNil)
	c.Assert(defaultPrivilegeDeclarations(c, db), qt.ContentEquals, []defaultPrivilegeDeclaration{
		{Identity: pgdefaultprivilege.Identity{Grantor: "app_owner", Schema: "app", ObjectType: "TABLES", Grantee: "app_reader"}, Value: pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}, Revoked: []string{"INSERT", "UPDATE"}}},
		{Identity: pgdefaultprivilege.Identity{Grantor: "app_owner", Schema: "app", ObjectType: "FUNCTIONS", Grantee: "PUBLIC"}, Value: pgdefaultprivilege.Desired{Revoked: []string{"EXECUTE"}}},
	})
}

// TestParseSource_DefaultPrivilegeRevoked_FailurePath pins the directives
// refused: one granting and revoking nothing, and a package granting and
// revoking one privilege of one identity.
func TestParseSource_DefaultPrivilegeRevoked_FailurePath(t *testing.T) {
	c := qt.New(t)
	source := "package models\n\n" +
		`//ptah:schema:defaultprivilege for_role="o" schema="app" object_type="TABLES" grantee="r"` +
		"\ntype AccessControl struct{}\n"

	db, err := goschema.ParseSource(defaultPrivilegeOwners, "access.go", source)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
	c.Assert(err, qt.ErrorMatches, `(?s).*a default privilege requires privileges or revoked.*`)
	c.Assert(db.FeatureObjects.Len(), qt.Equals, 0)
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

	db, err := goschema.ParseDir(defaultPrivilegeOwners, dir)

	c.Assert(err, qt.ErrorMatches, `(?s).*default privilege INSERT is both granted and revoked.*`)
	c.Assert(db, qt.IsNil)
}
