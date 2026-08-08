package rolescope_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/rolescope"
)

// TestDescribeAllReadsTheOptIn pins how the opt-in is parsed.
//
// The rows mirror [go.5x5.cz/ptah/internal/atlashclrender]'s and
// [go.5x5.cz/ptah/internal/atlassource]'s opt-ins, so an operator who learned
// one spelling is not surprised by this one: unset, empty, false and
// unparsable all keep the scoped default.
func TestDescribeAllReadsTheOptIn(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "unset keeps the scoped description", value: "", want: false},
		{name: "1 restores the full cluster read", value: "1", want: true},
		{name: "true restores the full cluster read", value: "true", want: true},
		{name: "TRUE restores the full cluster read", value: "TRUE", want: true},
		{name: "0 keeps the scoped description", value: "0", want: false},
		{name: "false keeps the scoped description", value: "false", want: false},
		{name: "an unparsable value keeps the scoped description", value: "all of them", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(rolescope.DescribeAllEnvVar, test.value)

			c.Assert(rolescope.DescribeAll(), qt.Equals, test.want)
		})
	}
}

// TestDescribeAllEnvVarIsSpelledLikeTheDocumentationQuotesIt pins the name the
// note, the documentation and the feature matrix all quote.
//
// The spelling is part of the interface: an operator meets it in a stderr note
// and types it back. Renaming it silently would leave every sentence that
// quotes it wrong.
func TestDescribeAllEnvVarIsSpelledLikeTheDocumentationQuotesIt(t *testing.T) {
	c := qt.New(t)

	c.Assert(rolescope.DescribeAllEnvVar, qt.Equals, "PTAH_POSTGRES_INSPECT_ALL_ROLES")
}

// TestReportUndescribedNamesTheCountAndTheWayBack is the guard on the half of
// the policy that is not the variable: what the default leaves out has to be
// reported, not dropped in silence (AGENTS.md, "what the default leaves out is
// reported"). A reader shown no role block and no note cannot tell a server
// with no roles from a server whose roles this description declined to
// mention.
func TestReportUndescribedNamesTheCountAndTheWayBack(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		omitted []dbschematypes.DBRole
		want    string
	}{
		{
			name:    "one role is singular",
			omitted: []dbschematypes.DBRole{{Name: "other_tenant"}},
			want: "note: 1 role Ptah manages on this server is not described, because nothing in the" +
				" inspected schemas refers to them; comparison still treats them as present, so none of" +
				" them is planned as a CREATE ROLE. Set PTAH_POSTGRES_INSPECT_ALL_ROLES=1 to describe" +
				" every role Ptah manages.\n",
		},
		{
			name: "three roles are plural",
			omitted: []dbschematypes.DBRole{
				{Name: "other_tenant"}, {Name: "pgbouncer"}, {Name: "reporting"},
			},
			want: "note: 3 roles Ptah manages on this server are not described, because nothing in the" +
				" inspected schemas refers to them; comparison still treats them as present, so none of" +
				" them is planned as a CREATE ROLE. Set PTAH_POSTGRES_INSPECT_ALL_ROLES=1 to describe" +
				" every role Ptah manages.\n",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			var out bytes.Buffer

			rolescope.ReportUndescribed(&out, &dbschematypes.DBSchema{RolesOutOfScope: test.omitted})

			c.Assert(out.String(), qt.Equals, test.want)
		})
	}
}

// TestReportUndescribedNeverPrintsARoleName is the other half of the note's
// contract, and it is a security property rather than a style one.
//
// [dbschematypes.DBSchema.RolesOutOfScope] is `json:"-"` precisely so that role
// names from outside the inspected scope never reach output; on a shared
// instance they are other tenants' names. A note that printed them would leak
// through the diagnostics stream exactly what scoping the description exists
// to stop leaking, so the count is reported and the names are not.
func TestReportUndescribedNeverPrintsARoleName(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	rolescope.ReportUndescribed(&out, &dbschematypes.DBSchema{
		RolesOutOfScope: []dbschematypes.DBRole{
			{Name: "acme_tenant_billing"},
			{Name: "zeta_tenant_reporting"},
		},
	})

	c.Assert(out.String(), qt.Contains, "2 roles")
	c.Assert(out.String(), qt.Not(qt.Contains), "acme_tenant_billing")
	c.Assert(out.String(), qt.Not(qt.Contains), "zeta_tenant_reporting")
}

// TestReportUndescribedStaysSilentWhenNothingWasLeftOut keeps the note from
// becoming noise on every read.
//
// Silence has to mean something: an operator who sees a note on one database
// and none on the next has learned a fact about the two databases. The nil
// writer row is the inspect surfaces' spelling of "no diagnostics stream", and
// a note that panicked there would fail a read that succeeded.
func TestReportUndescribedStaysSilentWhenNothingWasLeftOut(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		report func(*bytes.Buffer)
	}{
		{
			name: "no roles were left out",
			report: func(out *bytes.Buffer) {
				rolescope.ReportUndescribed(out, &dbschematypes.DBSchema{
					Roles: []dbschematypes.DBRole{{Name: "app_user"}},
				})
			},
		},
		{
			name: "a dialect that reports no roles at all",
			report: func(out *bytes.Buffer) {
				rolescope.ReportUndescribed(out, &dbschematypes.DBSchema{})
			},
		},
		{
			name: "a nil schema",
			report: func(out *bytes.Buffer) {
				rolescope.ReportUndescribed(out, nil)
			},
		},
		{
			name: "a nil writer takes the roles with it",
			report: func(_ *bytes.Buffer) {
				rolescope.ReportUndescribed(nil, &dbschematypes.DBSchema{
					RolesOutOfScope: []dbschematypes.DBRole{{Name: "other_tenant"}},
				})
			},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			var out bytes.Buffer

			test.report(&out)

			c.Assert(out.String(), qt.Equals, "")
		})
	}
}
