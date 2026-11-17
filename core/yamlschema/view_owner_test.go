package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/yamlschema"
	"ptah.run/internal/objectowner"
)

// TestParse_ViewOwner_HappyPath pins that a YAML schema declares the owner of
// a view and of a materialized view with an owner key, as written, and that a
// view without the key declares none.
func TestParse_ViewOwner_HappyPath(t *testing.T) {
	c := qt.New(t)

	db, err := yamlschema.Parse(t.Context(), noOwners, []byte(`
views:
  report:
    body: SELECT id FROM orders
    owner: reporting
  plain:
    body: SELECT id FROM orders
matviews:
  totals:
    body: SELECT count(*) AS n FROM orders
    owner: "Report Owner"
`))

	c.Assert(err, qt.IsNil)
	owners := make(map[string]string)
	for _, view := range db.Views {
		owners[view.Name] = view.Owner
	}
	for _, view := range db.MaterializedViews {
		owners[view.Name] = view.Owner
	}
	c.Assert(owners, qt.DeepEquals, map[string]string{"report": "reporting", "plain": "", "totals": "Report Owner"})
}

// TestParse_ViewOwner_FailurePath pins the owners the YAML reader refuses: one
// that is not a string, one with no name, and the session's role.
func TestParse_ViewOwner_FailurePath(t *testing.T) {
	tests := []struct {
		name, document, wantErr string
	}{
		{
			name:     "a list",
			document: "views:\n  report:\n    body: SELECT 1 AS x\n    owner: [a, b]\n",
			wantErr:  `.*view "report": owner: invalid owner: write the role's name as a string`,
		},
		{
			name:     "a null",
			document: "matviews:\n  totals:\n    body: SELECT 1 AS x\n    owner: ~\n",
			wantErr:  `.*materialized view "totals": owner: invalid owner: write the role's name as a string`,
		},
		{
			name:     "an empty string",
			document: "views:\n  report:\n    body: SELECT 1 AS x\n    owner: \"\"\n",
			wantErr:  `.*view "report": owner: invalid owner: the role's name is empty`,
		},
		{
			name:     "the session's role",
			document: "views:\n  report:\n    body: SELECT 1 AS x\n    owner: SESSION_USER\n",
			wantErr:  `.*view "report": owner: invalid owner SESSION_USER: it names the role of the session .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := yamlschema.Parse(t.Context(), noOwners, []byte(test.document))

			c.Assert(err, qt.ErrorIs, objectowner.ErrInvalidOwner)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
