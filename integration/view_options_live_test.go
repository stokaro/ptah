//go:build integration

package integration_test

import (
	"database/sql"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// viewOptionsCase is one change to the options a view or a materialized view
// holds, applied through a SQL schema file.
type viewOptionsCase struct {
	name string
	// live is the schema the database holds, and the declaration the
	// rollback returns to; declared is the schema the change goes to.
	live, declared string
	// after is what [heldViewOptions] reads back after the change. The
	// rollback has to read back what the database held before it.
	after []string
}

const (
	optionsTable = "CREATE TABLE orders (id integer PRIMARY KEY, amount numeric NOT NULL);\nINSERT INTO orders VALUES (1, 10);\n"
	optionsView  = "CREATE VIEW report%s AS SELECT id, amount FROM orders;\n"
	optionsWide  = "CREATE VIEW report%s AS SELECT id, amount, amount * 2 AS doubled FROM orders;\n"
	optionsTotal = "CREATE MATERIALIZED VIEW totals%s AS SELECT sum(amount) AS total FROM orders;\n"
)

// optionsSchema is the table, the view and the materialized view, each with
// the WITH list given, or none for an empty one.
func optionsSchema(view, viewOptions, totalsOptions string) string {
	return optionsTable + strings.Replace(view, "%s", withList(viewOptions), 1) +
		strings.Replace(optionsTotal, "%s", withList(totalsOptions), 1)
}

// withList is options as the WITH list of a CREATE statement.
func withList(options string) string {
	if options == "" {
		return ""
	}
	return " WITH (" + options + ")"
}

// heldViewOptions reads back each view and materialized view of public with
// the options pg_class.reloptions holds for it.
func heldViewOptions(c *qt.C, db *sql.DB) []string {
	c.Helper()
	return queryStrings(c, db, `SELECT c.relname || ' ' || COALESCE(array_to_string(c.reloptions, ','), '')
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind IN ('v', 'm') ORDER BY 1`)
}

// viewLikeOIDs reads back each view and materialized view of public with its
// oid, which a drop and a create change and an ALTER keeps.
func viewLikeOIDs(c *qt.C, db *sql.DB) []string {
	c.Helper()
	return queryStrings(c, db, `SELECT c.relname || ' ' || c.oid::text FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relkind IN ('v', 'm') ORDER BY 1`)
}

// viewOptionsInPlaceCases change the options of objects the database holds,
// and nothing else. The spellings in live are ones PostgreSQL stores as
// written, so the database holds `security_invoker=on` and `fillfactor=070`
// where a declaration would state true and 70.
var viewOptionsInPlaceCases = []viewOptionsCase{
	{
		name:     "options changed alone",
		live:     optionsSchema(optionsView, "security_invoker = on", "fillfactor = 70"),
		declared: optionsSchema(optionsView, "security_barrier = true", "fillfactor = 80, autovacuum_enabled = false"),
		after:    []string{"report security_barrier=true", "totals fillfactor=80,autovacuum_enabled=false"},
	},
	{
		// A view declared without options is a view without them.
		name:     "options the declaration leaves out",
		live:     optionsSchema(optionsView, "security_invoker = on, security_barrier = 'yes'", "fillfactor = '070'"),
		declared: optionsSchema(optionsView, "", ""),
		after:    []string{"report ", "totals "},
	},
}

// TestViewOptionsInPlaceLive applies each of [viewOptionsInPlaceCases] through
// a SQL schema file on PostgreSQL, in both statement styles: up, down and up
// again, each in one transaction. After each step pg_class.reloptions holds
// the options the step's declaration states and a comparison finds nothing
// left to do, and every view and materialized view keeps its oid: the change
// is made in place, so a materialized view keeps its rows and a view keeps
// what depends on it.
func TestViewOptionsInPlaceLive(t *testing.T) {
	for _, test := range viewOptionsInPlaceCases {
		for _, style := range viewRebuildStyles {
			t.Run(test.name+" in "+style.name, func(t *testing.T) {
				c := qt.New(t)
				url, db, _ := viewRebuildTarget(c, test.live)
				before, objects := heldViewOptions(c, db), viewLikeOIDs(c, db)

				up, down := planRebuildBothWays(c, url, test.declared, style.style)
				c.Logf("up:\n%s\ndown:\n%s", strings.Join(up, ";\n"), strings.Join(down, ";\n"))

				runRebuildStatements(c, db, up)
				c.Assert(heldViewOptions(c, db), qt.DeepEquals, test.after)
				c.Assert(viewLikeOIDs(c, db), qt.DeepEquals, objects)
				c.Assert(rebuildPending(c, url, test.declared).HasChanges(), qt.IsFalse)
				runRebuildStatements(c, db, down)
				c.Assert(heldViewOptions(c, db), qt.DeepEquals, before)
				c.Assert(viewLikeOIDs(c, db), qt.DeepEquals, objects)
				c.Assert(rebuildPending(c, url, test.live).HasChanges(), qt.IsFalse)
				runRebuildStatements(c, db, up)
				c.Assert(heldViewOptions(c, db), qt.DeepEquals, test.after)
			})
		}
	}
}

// viewOptionsWrittenCases are changes whose statement writes the object
// itself, and states the options its declaration holds: a CREATE for an object
// the plan adds, and a CREATE OR REPLACE VIEW, which clears every option it
// does not state.
var viewOptionsWrittenCases = []viewOptionsCase{
	{
		name:     "options declared on objects the plan creates",
		live:     optionsTable,
		declared: optionsSchema(optionsView, "security_invoker = true, security_barrier", "fillfactor = 70"),
		after:    []string{"report security_invoker=true,security_barrier=true", "totals fillfactor=70"},
	},
	{
		name:     "a view replaced in place keeps the options it declares",
		live:     optionsSchema(optionsView, "security_invoker = true", ""),
		declared: optionsSchema(optionsWide, "security_invoker = true", ""),
		after:    []string{"report security_invoker=true", "totals "},
	},
}

// TestViewOptionsWrittenLive applies each of [viewOptionsWrittenCases] as
// [TestViewOptionsInPlaceLive] does, without the oid check: the rollback of
// the replaced view drops a column, which CREATE OR REPLACE VIEW cannot do, so
// it drops the view and creates it again with the options it held.
func TestViewOptionsWrittenLive(t *testing.T) {
	for _, test := range viewOptionsWrittenCases {
		for _, style := range viewRebuildStyles {
			t.Run(test.name+" in "+style.name, func(t *testing.T) {
				c := qt.New(t)
				url, db, _ := viewRebuildTarget(c, test.live)
				before := heldViewOptions(c, db)

				up, down := planRebuildBothWays(c, url, test.declared, style.style)
				c.Logf("up:\n%s\ndown:\n%s", strings.Join(up, ";\n"), strings.Join(down, ";\n"))

				runRebuildStatements(c, db, up)
				c.Assert(heldViewOptions(c, db), qt.DeepEquals, test.after)
				c.Assert(rebuildPending(c, url, test.declared).HasChanges(), qt.IsFalse)
				runRebuildStatements(c, db, down)
				c.Assert(heldViewOptions(c, db), qt.DeepEquals, before)
				c.Assert(rebuildPending(c, url, test.live).HasChanges(), qt.IsFalse)
				runRebuildStatements(c, db, up)
				c.Assert(heldViewOptions(c, db), qt.DeepEquals, test.after)
			})
		}
	}
}

// TestViewOptionsSpelledAnotherWayLive pins that options the database stored
// in another spelling of the declared values plan nothing, read through the
// live reader and the comparison a connected command runs.
func TestViewOptionsSpelledAnotherWayLive(t *testing.T) {
	c := qt.New(t)
	url, db, _ := viewRebuildTarget(c, optionsSchema(optionsView, "security_invoker = on, security_barrier = 'yes'", "fillfactor = '070'"))
	c.Assert(heldViewOptions(c, db), qt.Contains, "report security_invoker=on,security_barrier=yes")

	pending := rebuildPending(c, url, optionsSchema(optionsView, "security_barrier, security_invoker = true", "fillfactor = 70"))

	c.Assert(pending.HasChanges(), qt.IsFalse)
}
