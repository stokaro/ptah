package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/yamlschema"
)

// TestParse_TriggerClauses reads the clauses a PostgreSQL trigger adds to its
// event: a condition and transition tables, with the events canonicalized.
func TestParse_TriggerClauses(t *testing.T) {
	c := qt.New(t)

	db, err := yamlschema.Parse([]byte(`
tables:
  orders:
    columns:
      id: { type: SERIAL, primary: true }
triggers:
  orders_touch:
    table: orders
    timing: before
    event: update of total or insert
    when: NEW.total > 0
    body: RETURN NEW;
  orders_audit:
    table: orders
    timing: after
    event: update
    for: statement
    old_table: old_rows
    new_table: new_rows
    body: RETURN NULL;
`))

	c.Assert(err, qt.IsNil)
	got := make([][]string, len(db.Triggers))
	for i, trigger := range db.Triggers {
		got[i] = []string{trigger.Name, trigger.Event, trigger.ForEach, trigger.When, trigger.OldTable, trigger.NewTable}
	}
	c.Assert(got, qt.ContentEquals, [][]string{
		{"orders_touch", "INSERT OR UPDATE OF total", "ROW", "NEW.total > 0", "", ""},
		{"orders_audit", "UPDATE", "STATEMENT", "", "old_rows", "new_rows"},
	})
}
