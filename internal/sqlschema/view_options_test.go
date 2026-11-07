package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/viewoptions"
)

// TestRead_ViewOptions_HappyPath pins that a PostgreSQL schema file declares a
// view's options and a materialized view's storage parameters in the WITH list
// before AS, in each spelling the server takes. The reader refused the list
// with "expected 'AS', got 'WITH'", so no schema file could ask for
// security_invoker (stokaro/ptah#5017).
//
// pg_dump writes each value as a string literal, as the first row does.
func TestRead_ViewOptions_HappyPath(t *testing.T) {
	tests := []struct {
		name             string
		sql              string
		wantView         []string
		wantMaterialized []string
	}{
		{
			name:     "the spelling pg_dump writes",
			sql:      "CREATE VIEW public.v WITH (security_invoker='true', security_barrier='true') AS SELECT 1 AS x;",
			wantView: []string{"security_invoker=true", "security_barrier=true"},
		},
		{
			name:     "a bare name, a keyword and a folded name",
			sql:      "CREATE OR REPLACE VIEW v WITH (security_barrier, Security_Invoker = ON) AS SELECT 1 AS x;",
			wantView: []string{"security_barrier=true", "security_invoker=ON"},
		},
		{
			name:     "a column list before the options",
			sql:      "CREATE VIEW v (a) WITH (security_invoker = true) AS SELECT 1;",
			wantView: []string{"security_invoker=true"},
		},
		{
			name:             "storage parameters, a negative number and a doubled quote",
			sql:              "CREATE MATERIALIZED VIEW m WITH (fillfactor = 70, log_autovacuum_min_duration = -1, x_y = 'it''s') AS SELECT 1 AS x;",
			wantMaterialized: []string{"fillfactor=70", "log_autovacuum_min_duration=-1", "x_y=it's"},
		},
		{
			name:             "a guarded materialized view and a comment inside the list",
			sql:              "CREATE MATERIALIZED VIEW IF NOT EXISTS m WITH (/* pages */ fillfactor = 70) AS SELECT 1 AS x;",
			wantMaterialized: []string{"fillfactor=70"},
		},
		{
			name: "no WITH list declares none",
			sql:  "CREATE VIEW v AS WITH x AS (SELECT 1 AS a) SELECT a FROM x;",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := readWithOwners([]byte(test.sql), platform.Postgres)
			c.Assert(err, qt.IsNil)
			c.Assert(len(database.Views)+len(database.MaterializedViews), qt.Equals, 1)
			c.Assert(viewOptionsOf(database.Views), qt.DeepEquals, test.wantView)
			c.Assert(materializedOptionsOf(database.MaterializedViews), qt.DeepEquals, test.wantMaterialized)
		})
	}
}

// TestRead_ViewOptions_FailurePath pins the lists the reader refuses: the ones
// PostgreSQL refuses or reads in a way a declaration cannot hold, and a list
// that is not one.
func TestRead_ViewOptions_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{name: "the check option", sql: "CREATE VIEW v WITH (check_option = local) AS SELECT 1 AS x;",
			wantErr: `(?s).*view v: invalid view option: check_option is a view's check option; declare it as WITH CHECK OPTION`},
		{name: "a TOAST option", sql: "CREATE MATERIALIZED VIEW m WITH (toast.autovacuum_enabled = false) AS SELECT 1 AS x;",
			wantErr: `(?s).*materialized view m: invalid view option: toast.autovacuum_enabled is kept on the TOAST table.*`},
		{name: "a name given twice", sql: "CREATE VIEW v WITH (security_barrier, security_barrier = false) AS SELECT 1 AS x;",
			wantErr: `(?s).*view v: invalid view option: "security_barrier" is given more than once`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := readWithOwners([]byte(test.sql), platform.Postgres)
			c.Assert(err, qt.ErrorIs, viewoptions.ErrInvalidOption)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Views, qt.HasLen, 0)
			c.Assert(database.MaterializedViews, qt.HasLen, 0)
		})
	}
}

// TestRead_ViewOptions_UnreadableList pins the lists the parser cannot read:
// an option without a name, and one whose value is missing.
func TestRead_ViewOptions_UnreadableList(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{name: "an empty entry", sql: "CREATE VIEW v WITH (security_barrier, ) AS SELECT 1 AS x;",
			wantErr: `(?s).*expected an option name in the view options.*`},
		{name: "a missing value", sql: "CREATE MATERIALIZED VIEW m WITH (fillfactor = ) AS SELECT 1 AS x;",
			wantErr: `(?s).*expected a value after fillfactor= in the materialized view options.*`},
		{name: "an unclosed list", sql: "CREATE VIEW v WITH (security_barrier AS SELECT 1 AS x;",
			wantErr: `(?s).*expected ',' or '\)' in the view options.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := readWithOwners([]byte(test.sql), platform.Postgres)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Views, qt.HasLen, 0)
			c.Assert(database.MaterializedViews, qt.HasLen, 0)
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
