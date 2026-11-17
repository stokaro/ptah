package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
)

// TestParseViewOwner_HappyPath pins that the owner attribute of a view or a
// materialized view directive declares its owner, as written: a Go annotation
// names a role by the name the server stores, quotes and case included. A
// directive without it declares none, so the owner is unmanaged.
func TestParseViewOwner_HappyPath(t *testing.T) {
	tests := []struct {
		name                       string
		annotation                 string
		wantView, wantMaterialized string
	}{
		{
			name:       "a view",
			annotation: `//ptah:schema:view name="report" body="SELECT 1 AS x" owner="reporting"`,
			wantView:   "reporting",
		},
		{
			name:             "a materialized view with a name in mixed case",
			annotation:       `//ptah:schema:matview name="totals" body="SELECT 1 AS x" owner="Report Owner"`,
			wantMaterialized: "Report Owner",
		},
		{
			name:       "no attribute",
			annotation: `//ptah:schema:view name="report" body="SELECT 1 AS x"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := goschema.ParseSource(noOwners, "s.go", "package m\n\n"+test.annotation+"\ntype S struct{}\n")

			c.Assert(err, qt.IsNil)
			c.Assert(len(db.Views)+len(db.MaterializedViews), qt.Equals, 1)
			var view, materialized string
			for _, declared := range db.Views {
				view += declared.Owner
			}
			for _, declared := range db.MaterializedViews {
				materialized += declared.Owner
			}
			c.Assert(view, qt.Equals, test.wantView)
			c.Assert(materialized, qt.Equals, test.wantMaterialized)
		})
	}
}

// TestParseViewOwner_FailurePath pins the owners the parser refuses, naming
// the attribute and why: an empty one, which would read as no owner, the
// session's role and PUBLIC.
func TestParseViewOwner_FailurePath(t *testing.T) {
	tests := []struct {
		name, annotation, wantErr string
	}{
		{
			name:       "an empty owner",
			annotation: `//ptah:schema:view name="report" body="SELECT 1 AS x" owner=""`,
			wantErr:    `invalid "owner" value "" on //ptah:schema:view at .*: invalid owner: the role's name is empty`,
		},
		{
			name:       "the session's role",
			annotation: `//ptah:schema:matview name="totals" body="SELECT 1 AS x" owner="current_user"`,
			wantErr:    `invalid "owner" value "current_user" on //ptah:schema:matview at .*: invalid owner CURRENT_USER: it names the role of the session .*`,
		},
		{
			name:       "every role",
			annotation: `//ptah:schema:view name="report" body="SELECT 1 AS x" owner="PUBLIC"`,
			wantErr:    `invalid "owner" value "PUBLIC" on //ptah:schema:view at .*: invalid owner "PUBLIC": PUBLIC is every role, .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := goschema.ParseSource(noOwners, "s.go", "package m\n\n"+test.annotation+"\ntype S struct{}\n")

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db.Views, qt.HasLen, 0)
			c.Assert(db.MaterializedViews, qt.HasLen, 0)
		})
	}
}
