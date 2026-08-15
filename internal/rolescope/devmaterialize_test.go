package rolescope_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/rolescope"
)

// roleNames renders a role list as its names, so a failing assertion prints
// the names rather than every attribute of every role.
func roleNames(roles []goschema.Role) []string {
	var names []string
	for _, role := range roles {
		names = append(names, role.Name)
	}
	return names
}

// TestRolesToCreateOnDevSkipsWhatTheServerAlreadyHas is the guard on the
// property stokaro/ptah#1267 asks for: Ptah's own inspect output has to replay
// into a clean dev database, and the dev database's server is usually the one
// the output was inspected from.
//
// The first row is the reviewer's shape on PR #1305 -- a schema with one GRANT,
// whose description therefore names the grantee and the owner the explicit ACL
// carries. Both exist on the server, and re-creating either fails at SQLSTATE
// 42710 with the dev database freshly reset, because resetting a database does
// not clear a server's roles.
//
// The rows after it are what keeps that from becoming "never create a role".
// A role the server does not have is still created: that is the case a second
// cluster or an empty CI catalog is, and it never produced the error in the
// first place.
func TestRolesToCreateOnDevSkipsWhatTheServerAlreadyHas(t *testing.T) {

	tests := []struct {
		name             string
		declared         []goschema.Role
		present          []dbschematypes.DBRole
		wantCreate       []string
		wantAlreadyThere []string
	}{
		{
			name:             "the grantee and the owner a one-GRANT description names",
			declared:         []goschema.Role{{Name: "ptah_user"}, {Name: "app_reader"}},
			present:          []dbschematypes.DBRole{{Name: "ptah_user"}, {Name: "app_reader"}},
			wantCreate:       nil,
			wantAlreadyThere: []string{"ptah_user", "app_reader"},
		},
		{
			name:             "a role the server has never seen is still created",
			declared:         []goschema.Role{{Name: "brand_new"}},
			present:          []dbschematypes.DBRole{{Name: "ptah_user"}},
			wantCreate:       []string{"brand_new"},
			wantAlreadyThere: nil,
		},
		{
			name:             "a mixed document creates only the missing half",
			declared:         []goschema.Role{{Name: "app_reader"}, {Name: "brand_new"}},
			present:          []dbschematypes.DBRole{{Name: "app_reader"}},
			wantCreate:       []string{"brand_new"},
			wantAlreadyThere: []string{"app_reader"},
		},
		{
			name:             "an empty server creates every declared role",
			declared:         []goschema.Role{{Name: "app_reader"}, {Name: "app_writer"}},
			present:          nil,
			wantCreate:       []string{"app_reader", "app_writer"},
			wantAlreadyThere: nil,
		},
		{
			name:             "a document declaring no role asks the server for nothing",
			declared:         nil,
			present:          []dbschematypes.DBRole{{Name: "ptah_user"}},
			wantCreate:       nil,
			wantAlreadyThere: nil,
		},
		{
			name:             "role names match by exact spelling",
			declared:         []goschema.Role{{Name: "App_Reader"}},
			present:          []dbschematypes.DBRole{{Name: "app_reader"}},
			wantCreate:       []string{"App_Reader"},
			wantAlreadyThere: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			create, alreadyThere := rolescope.RolesToCreateOnDev(test.declared, test.present)

			c.Assert(roleNames(create), qt.DeepEquals, test.wantCreate)
			c.Assert(roleNames(alreadyThere), qt.DeepEquals, test.wantAlreadyThere)
		})
	}
}

// TestRolesToCreateOnDevKeepsTheDeclaredAttributes pins that the split hands
// back the roles the document wrote, not a reconstruction of them.
//
// The skipped half is what the note names, and an operator who is told
// `"app_reader"` was left alone has to be able to see what the document asked
// for. A split that returned bare names would make the note true and useless.
func TestRolesToCreateOnDevKeepsTheDeclaredAttributes(t *testing.T) {
	c := qt.New(t)

	create, alreadyThere := rolescope.RolesToCreateOnDev(
		[]goschema.Role{
			{Name: "app_reader", Login: true, CreateDB: true},
			{Name: "brand_new", Superuser: true},
		},
		[]dbschematypes.DBRole{{Name: "app_reader"}},
	)

	c.Assert(alreadyThere, qt.DeepEquals, []goschema.Role{{Name: "app_reader", Login: true, CreateDB: true}})
	c.Assert(create, qt.DeepEquals, []goschema.Role{{Name: "brand_new", Superuser: true}})
}

// TestReportNotCreatedOnDevNamesEveryRoleItSkipped is the disclosure half. What
// the default leaves out is reported rather than dropped in silence (AGENTS.md,
// "what the default leaves out is reported"), and here the report has to name
// the roles: a role kept as the server has it may differ from the one the
// document declares, and nobody can check which without knowing which.
//
// Naming them is not the disclosure [rolescope.ReportUndescribed] withholds.
// Every name here came out of the caller's own document.
func TestReportNotCreatedOnDevNamesEveryRoleItSkipped(t *testing.T) {

	tests := []struct {
		name            string
		alreadyOnServer []goschema.Role
		want            string
	}{
		{
			name:            "one role is singular",
			alreadyOnServer: []goschema.Role{{Name: "app_reader"}},
			want: "note: 1 role the schema declares already exists on the server hosting the dev database" +
				" and was not created there: \"app_reader\"; roles are server-scoped rather than" +
				" database-scoped, so a dev database cannot hold its own copy, and the inspected result" +
				" describes each of them as the server has it.\n",
		},
		{
			name:            "two roles are plural and sorted",
			alreadyOnServer: []goschema.Role{{Name: "ptah_user"}, {Name: "app_reader"}},
			want: "note: 2 roles the schema declares already exist on the server hosting the dev database" +
				" and were not created there: \"app_reader\", \"ptah_user\"; roles are server-scoped rather" +
				" than database-scoped, so a dev database cannot hold its own copy, and the inspected" +
				" result describes each of them as the server has it.\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer

			rolescope.ReportNotCreatedOnDev(&out, test.alreadyOnServer)

			c.Assert(out.String(), qt.Equals, test.want)
		})
	}
}

// TestReportNotCreatedOnDevStaysSilentWhenItCreatedEverything keeps the note
// from becoming noise on every materialization.
//
// Silence has to mean something: an operator who sees a note on one run and
// none on the next has learned a fact about the two. The nil writer row is the
// inspect surfaces' spelling of "no diagnostics stream", and a note that
// panicked there would fail a materialization that succeeded.
func TestReportNotCreatedOnDevStaysSilentWhenItCreatedEverything(t *testing.T) {

	tests := []struct {
		name   string
		report func(*bytes.Buffer)
	}{
		{
			name: "nothing was skipped",
			report: func(out *bytes.Buffer) {
				rolescope.ReportNotCreatedOnDev(out, nil)
			},
		},
		{
			name: "an empty list is not a skipped role",
			report: func(out *bytes.Buffer) {
				rolescope.ReportNotCreatedOnDev(out, []goschema.Role{})
			},
		},
		{
			name: "a nil writer takes the note with it",
			report: func(_ *bytes.Buffer) {
				rolescope.ReportNotCreatedOnDev(nil, []goschema.Role{{Name: "app_reader"}})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer

			test.report(&out)

			c.Assert(out.String(), qt.Equals, "")
		})
	}
}
