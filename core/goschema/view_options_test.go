package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
)

// TestParseViewOptions_HappyPath pins that a Go annotation declares a view's
// PostgreSQL options and a materialized view's storage parameters, in the
// spellings the WITH list of CREATE VIEW takes. Without the attribute the
// directive had no way to state them, so a view declared through it was
// planned without the options the database held (stokaro/ptah#5017).
func TestParseViewOptions_HappyPath(t *testing.T) {
	tests := []struct {
		name             string
		annotation       string
		wantView         []string
		wantMaterialized []string
	}{
		{
			name:       "view options, a bare name and a folded one",
			annotation: `//ptah:schema:view name="report" body="SELECT 1 AS x" options="Security_Invoker=on, security_barrier"`,
			wantView:   []string{"security_invoker=on", "security_barrier=true"},
		},
		{
			name:             "storage parameters",
			annotation:       `//ptah:schema:matview name="totals" body="SELECT 1 AS x" options="fillfactor=70,autovacuum_enabled=false"`,
			wantMaterialized: []string{"fillfactor=70", "autovacuum_enabled=false"},
		},
		{
			name:       "an empty attribute declares none",
			annotation: `//ptah:schema:view name="report" body="SELECT 1 AS x" options=""`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource(noOwners, "s.go", "package m\n\n"+test.annotation+"\ntype S struct{}\n")
			c.Assert(err, qt.IsNil)
			c.Assert(len(db.Views)+len(db.MaterializedViews), qt.Equals, 1)
			c.Assert(viewOptionsOf(db.Views), qt.DeepEquals, test.wantView)
			c.Assert(materializedOptionsOf(db.MaterializedViews), qt.DeepEquals, test.wantMaterialized)
		})
	}
}

// TestParseViewOptions_FailurePath pins the option lists the parser refuses,
// naming the attribute and why.
func TestParseViewOptions_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		annotation string
		wantErr    string
	}{
		{
			name:       "the check option",
			annotation: `//ptah:schema:view name="report" body="SELECT 1 AS x" options="check_option=local"`,
			wantErr:    `invalid "options" value "check_option=local" on //ptah:schema:view at .*: invalid view option: check_option is a view's check option; declare it as WITH CHECK OPTION`,
		},
		{
			name:       "a TOAST option",
			annotation: `//ptah:schema:matview name="totals" body="SELECT 1 AS x" options="toast.autovacuum_enabled=false"`,
			wantErr:    `invalid "options" value "toast.autovacuum_enabled=false" on //ptah:schema:matview at .*: invalid view option: toast.autovacuum_enabled is kept on the TOAST table, .*`,
		},
		{
			name:       "a name that is not one",
			annotation: `//ptah:schema:view name="report" body="SELECT 1 AS x" options="security-invoker=true"`,
			wantErr:    `invalid "options" value "security-invoker=true" on //ptah:schema:view at .*: invalid view option: "security-invoker=true" is not an option name: .*`,
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

// viewOptionsOf is the options of every view in views, nil for none.
func viewOptionsOf(views []schemamodel.View) []string {
	var options []string
	for _, view := range views {
		options = append(options, view.Options...)
	}
	return options
}

// materializedOptionsOf is [viewOptionsOf] for materialized views.
func materializedOptionsOf(views []schemamodel.MaterializedView) []string {
	var options []string
	for _, view := range views {
		options = append(options, view.Options...)
	}
	return options
}
