package globaldefaults_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/globaldefaults"
)

// TestReportUndescribed_HappyPath pins the note for one global default and for
// several. The several arrive out of order and include CockroachDB's FOR ALL
// ROLES, which the reader records with no grantor, so the row also pins that
// the note sorts what it names and says "all roles" instead of an empty name.
func TestReportUndescribed_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		privileges []catalog.GlobalDefaultPrivilege
		want       string
	}{
		{
			name:       "one",
			privileges: []catalog.GlobalDefaultPrivilege{{Grantor: "app_owner", ObjectType: "FUNCTIONS"}},
			want: "note: 1 global default privilege, set by ALTER DEFAULT PRIVILEGES without IN SCHEMA," +
				" is not described, because no schema source can declare one; a description applied" +
				" to another database does not carry it: FUNCTIONS for app_owner.\n",
		},
		{
			name: "several, one of them for all roles",
			privileges: []catalog.GlobalDefaultPrivilege{
				{Grantor: "app_owner", ObjectType: "TABLES"},
				{ObjectType: "TYPES"},
				{Grantor: "app_owner", ObjectType: "FUNCTIONS"},
			},
			want: "note: 3 global default privileges, set by ALTER DEFAULT PRIVILEGES without IN SCHEMA," +
				" are not described, because no schema source can declare one; a description applied" +
				" to another database does not carry them: FUNCTIONS for app_owner, TABLES for app_owner," +
				" TYPES for all roles.\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer

			globaldefaults.ReportUndescribed(&out, &catalog.Database{GlobalDefaultPrivileges: test.privileges})

			c.Assert(out.String(), qt.Equals, test.want)
		})
	}
}

// TestReportUndescribed_StaysSilentWithNothingLeftOut keeps the note off every
// read that met no global default, which is almost every read. A note printed
// regardless would teach an operator to skip it.
func TestReportUndescribed_StaysSilentWithNothingLeftOut(t *testing.T) {
	tests := []struct {
		name   string
		schema *catalog.Database
	}{
		{name: "no global default", schema: &catalog.Database{
			DefaultPrivileges: []catalog.DefaultPrivilege{{
				Grantor: "app_owner", Schema: "public", ObjectType: "TABLES",
				Grantee: "app_reader", Privilege: "SELECT",
			}},
		}},
		{name: "no description", schema: nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer

			globaldefaults.ReportUndescribed(&out, test.schema)

			c.Assert(out.String(), qt.Equals, "")
		})
	}
}

// TestReportUndescribed_AcceptsNoDiagnosticsStream is the inspect surfaces'
// spelling of "nowhere to write": a nil writer drops the note rather than
// failing the read that produced it.
func TestReportUndescribed_AcceptsNoDiagnosticsStream(_ *testing.T) {
	globaldefaults.ReportUndescribed(nil, &catalog.Database{
		GlobalDefaultPrivileges: []catalog.GlobalDefaultPrivilege{{Grantor: "app_owner", ObjectType: "TABLES"}},
	})
}
