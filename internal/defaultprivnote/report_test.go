package defaultprivnote_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/defaultprivnote"
)

// TestReportUndescribed_HappyPath pins the note for one undescribed default and
// for several. The several arrive out of order and cover each shape the reader
// records: a default without IN SCHEMA for a role, CockroachDB's FOR ALL ROLES
// in a schema, and FOR ALL ROLES without IN SCHEMA. So the row also pins that
// the note sorts what it names, and says "every schema" and "all roles" where
// the reader leaves a name empty.
func TestReportUndescribed_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		privileges []catalog.UndescribedDefaultPrivilege
		want       string
	}{
		{
			name:       "one",
			privileges: []catalog.UndescribedDefaultPrivilege{{Grantor: "app_owner", ObjectType: "FUNCTIONS"}},
			want: "note: 1 default privilege is not described, because no schema source can declare one" +
				" set without IN SCHEMA or FOR ALL ROLES; a description applied to another database" +
				" does not carry it: FUNCTIONS in every schema for app_owner.\n",
		},
		{
			name: "several of every shape",
			privileges: []catalog.UndescribedDefaultPrivilege{
				{Schema: "public", ObjectType: "TABLES"},
				{ObjectType: "TYPES"},
				{Grantor: "app_owner", ObjectType: "FUNCTIONS"},
			},
			want: "note: 3 default privileges are not described, because no schema source can declare one" +
				" set without IN SCHEMA or FOR ALL ROLES; a description applied to another database" +
				" does not carry them: FUNCTIONS in every schema for app_owner, TABLES in public for all roles," +
				" TYPES in every schema for all roles.\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer

			defaultprivnote.ReportUndescribed(&out, &catalog.Database{UndescribedDefaultPrivileges: test.privileges})

			c.Assert(out.String(), qt.Equals, test.want)
		})
	}
}

// TestReportUndescribed_StaysSilentWithNothingLeftOut keeps the note off every
// read that met no undescribed default, which is almost every read. A note
// printed regardless would teach an operator to skip it.
func TestReportUndescribed_StaysSilentWithNothingLeftOut(t *testing.T) {
	tests := []struct {
		name   string
		schema *catalog.Database
	}{
		{name: "only described defaults", schema: &catalog.Database{
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

			defaultprivnote.ReportUndescribed(&out, test.schema)

			c.Assert(out.String(), qt.Equals, "")
		})
	}
}

// TestReportUndescribed_AcceptsNoDiagnosticsStream is the inspect surfaces'
// spelling of "nowhere to write": a nil writer drops the note rather than
// failing the read that produced it.
func TestReportUndescribed_AcceptsNoDiagnosticsStream(_ *testing.T) {
	defaultprivnote.ReportUndescribed(nil, &catalog.Database{
		UndescribedDefaultPrivileges: []catalog.UndescribedDefaultPrivilege{{Grantor: "app_owner", ObjectType: "TABLES"}},
	})
}
