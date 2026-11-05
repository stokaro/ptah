package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/yamlschema"
	"ptah.run/internal/viewoptions"
)

// TestParse_ViewOptions_HappyPath pins that a YAML schema declares a view's
// PostgreSQL options and a materialized view's storage parameters as a list of
// `name=value` entries, as it declares a routine's settings.
func TestParse_ViewOptions_HappyPath(t *testing.T) {
	c := qt.New(t)

	db, err := yamlschema.Parse(t.Context(), noOwners, []byte(`
views:
  report:
    body: SELECT id FROM orders
    options: [Security_Invoker=on, security_barrier]
matviews:
  totals:
    body: SELECT count(*) AS n FROM orders
    options:
      - fillfactor=70
      - autovacuum_enabled='false'
`))

	c.Assert(err, qt.IsNil)
	c.Assert(db.Views, qt.HasLen, 1)
	c.Assert(db.Views[0].Options, qt.DeepEquals, []string{"security_invoker=on", "security_barrier=true"})
	c.Assert(db.MaterializedViews, qt.HasLen, 1)
	c.Assert(db.MaterializedViews[0].Options, qt.DeepEquals, []string{"fillfactor=70", "autovacuum_enabled=false"})
}

// TestParse_ViewOptions_FailurePath pins the lists the YAML reader refuses.
func TestParse_ViewOptions_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		document string
		wantErr  string
	}{
		{
			name:     "the check option on a view",
			document: "views:\n  report:\n    body: SELECT 1 AS x\n    options: [check_option=cascaded]\n",
			wantErr:  `.*view "report": invalid view option: check_option is a view's check option; declare it as WITH CHECK OPTION`,
		},
		{
			name:     "a parameter given twice on a materialized view",
			document: "matviews:\n  totals:\n    body: SELECT 1 AS x\n    options: [fillfactor=70, fillfactor=80]\n",
			wantErr:  `.*materialized view "totals": invalid view option: "fillfactor" is given more than once`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := yamlschema.Parse(t.Context(), noOwners, []byte(test.document))
			c.Assert(err, qt.ErrorIs, viewoptions.ErrInvalidOption)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
