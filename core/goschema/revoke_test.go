package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
)

// TestParseSource_RoutineGrantAndRevoke_HappyPath pins how the grant and revoke
// directives name a function or procedure: the value carries the argument
// types in parentheses, because PostgreSQL overloads a routine name by them.
func TestParseSource_RoutineGrantAndRevoke_HappyPath(t *testing.T) {
	c := qt.New(t)
	source := `package models

//ptah:schema:grant role="wpmgr_app" privilege="EXECUTE" on_function="purge_workspace(uuid)"
//ptah:schema:revoke role="PUBLIC" privilege="EXECUTE" on_function="purge_workspace(uuid)" comment="SECURITY DEFINER"
//ptah:schema:revoke role="wpmgr_app" privileges="INSERT,update" on_table="plugin_signatures"
//ptah:schema:grant role="ops" privilege="EXECUTE" on_procedure="app.archive(integer, text)"
type AccessControl struct{}
`

	db := mustParseSource(c, "access.go", source)

	c.Assert(db.Grants, qt.DeepEquals, []schemamodel.Grant{
		{
			StructName: "AccessControl", Role: "wpmgr_app", Privileges: []string{"EXECUTE"},
			OnRoutine: "purge_workspace", RoutineArguments: "uuid", RoutineKind: "FUNCTION",
		},
		{
			StructName: "AccessControl", Role: "ops", Privileges: []string{"EXECUTE"},
			OnRoutine: "app.archive", RoutineArguments: "integer, text", RoutineKind: "PROCEDURE",
		},
	})
	c.Assert(db.RevokedGrants, qt.DeepEquals, []schemamodel.Grant{
		{
			StructName: "AccessControl", Role: "PUBLIC", Privileges: []string{"EXECUTE"},
			OnRoutine: "purge_workspace", RoutineArguments: "uuid", RoutineKind: "FUNCTION", Comment: "SECURITY DEFINER",
		},
		{
			StructName: "AccessControl", Role: "wpmgr_app", Privileges: []string{"INSERT", "UPDATE"},
			OnTable: "plugin_signatures",
		},
	})
}

// TestParseSource_RoutineGrantAndRevoke_FailurePath pins the directives refused
// while the file is parsed rather than kept as something no statement can say.
func TestParseSource_RoutineGrantAndRevoke_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		directive string
		wantIs    error
		wantErr   string
	}{
		{
			name:      "a function named without its argument types",
			directive: `//ptah:schema:grant role="app" privilege="EXECUTE" on_function="purge_workspace"`,
			wantIs:    ptaherr.ErrInvalidAttributeValue,
			wantErr:   `on_function="purge_workspace" on //ptah:schema:grant at .* needs the argument types in parentheses, as in purge\(uuid\): PostgreSQL tells overloaded routines apart by them`,
		},
		{
			name:      "a revoke naming no role",
			directive: `//ptah:schema:revoke privilege="SELECT" on_table="t"`,
			wantIs:    ptaherr.ErrMissingRequiredAttribute,
			wantErr:   `missing required annotation attribute "role" on //ptah:schema:revoke at .*`,
		},
		{
			name:      "a revoke naming no privilege",
			directive: `//ptah:schema:revoke role="app" on_table="t"`,
			wantIs:    ptaherr.ErrMissingRequiredAttribute,
			wantErr:   `missing required annotation attribute "privilege" on //ptah:schema:revoke at .*`,
		},
		{
			name:      "a revoke with an attribute it does not have",
			directive: `//ptah:schema:revoke role="app" privilege="SELECT" on_table="t" with_option="true"`,
			wantIs:    ptaherr.ErrUnknownAttribute,
			wantErr:   `.*with_option.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := "package models\n\n" + test.directive + "\ntype AccessControl struct{}\n"

			db, err := goschema.ParseSource("access.go", source)

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db.Grants, qt.IsNil)
			c.Assert(db.RevokedGrants, qt.IsNil)
		})
	}
}

// TestParseDir_RefusesAPrivilegeBothGrantedAndRevoked drives the contradiction
// through the directory parser. Declarations carry no order, so neither can
// win the way a later SQL statement does.
func TestParseDir_RefusesAPrivilegeBothGrantedAndRevoked(t *testing.T) {
	c := qt.New(t)
	dir := c.TB.TempDir()
	writeGoFile(c, dir, "a.go", "package models\n\n"+
		`//ptah:schema:grant role="app" privilege="EXECUTE" on_function="purge(uuid)"`+"\ntype Granted struct{}\n")
	writeGoFile(c, dir, "b.go", "package models\n\n"+
		`//ptah:schema:revoke role="app" privilege="EXECUTE" on_function="purge(UUID)"`+"\ntype Revoked struct{}\n")

	db, err := goschema.ParseDir(dir)

	c.Assert(err, qt.ErrorMatches, `(?s).*EXECUTE on ROUTINE purge\(uuid\) is both granted to and revoked from "app"; declare one or the other.*`)
	c.Assert(db, qt.IsNil)
}
