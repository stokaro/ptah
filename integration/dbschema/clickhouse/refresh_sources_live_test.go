//go:build integration

package clickhouse_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
)

func refreshSourceDeclaration(c *qt.C, format, source, view string, clause *string) *schemamodel.Database {
	c.Helper()
	runtime := must.Must(builtin.New())
	attribute := ""
	var db *schemamodel.Database
	var err error
	switch format {
	case "yaml":
		if clause != nil {
			attribute = fmt.Sprintf("    refresh: %q\n", *clause)
		}
		db, err = yamlschema.Parse(runtime.YAML(), []byte(fmt.Sprintf("matviews:\n  %s:\n    body: SELECT count() AS c FROM %s\n%s", view, source, attribute)))
	case "hcl":
		if clause != nil {
			attribute = fmt.Sprintf("  refresh = %q\n", *clause)
		}
		db, err = atlashcl.ParseWithOptions([]byte(fmt.Sprintf("materialized %q {\n  as = %q\n%s}\n", view, "SELECT count() AS c FROM "+source, attribute)), "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})
	}
	c.Assert(err, qt.IsNil)
	c.Assert(db, qt.IsNotNil)
	// The table is a fixture; the view and its coverage come from the source.
	fixture := refreshDeclaration(source, view, "")
	db.Tables, db.Fields = fixture.Tables, fixture.Fields
	return db
}

func TestRefreshYAMLAndHCLApplyAndReverseLive(t *testing.T) {
	for _, format := range []string{"yaml", "hcl"} {
		for _, test := range []struct {
			name, want string
			clause     *string
			replace    bool
		}{
			{name: "every", clause: new("every 120 minute"), want: "EVERY 2 HOUR"},
			{name: "after", clause: new("AFTER 90 SECOND"), want: "AFTER 1 MINUTE 30 SECOND"},
			{name: "append", clause: new("EVERY 1 DAY OFFSET 120 MINUTE RANDOMIZE FOR 1800 SECOND APPEND"), want: "EVERY 1 DAY OFFSET 2 HOUR RANDOMIZE FOR 30 MINUTE APPEND", replace: true},
			{name: "empty", clause: new(""), replace: true},
			{name: "omitted", replace: true},
		} {
			t.Run(format+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				conn, source, view := refreshFixture(c, "EVERY 1 HOUR")
				before := viewUUID(c, conn, view)
				desired := refreshSourceDeclaration(c, format, source, view, test.clause)
				forward, reverse := planRefresh(c, conn, desired, readRefreshFixture(c, conn, source, view))
				c.Assert(forward, qt.Not(qt.HasLen), 0)
				applyStatements(c, conn, forward)
				current := readRefreshFixture(c, conn, source, view)
				c.Assert(observedSchedule(c, current), qt.Equals, test.want)
				c.Assert(viewUUID(c, conn, view) != before, qt.Equals, test.replace)
				second, _ := planRefresh(c, conn, desired, current)
				c.Assert(second, qt.HasLen, 0)
				exported, err := atlashclrender.Render(desired, atlashclrender.Options{HCL: must.Must(builtin.New()).HCL(), Dialect: "clickhouse"})
				c.Assert(err, qt.IsNil)
				c.Assert(exported.Diagnostics, qt.HasLen, 0)
				reloaded, err := atlashcl.ParseWithOptions(exported.Data, "copy.hcl", atlashcl.Options{HCL: must.Must(builtin.New()).HCL()})
				c.Assert(err, qt.IsNil)
				assertConverged(c, conn, reloaded, current)
				applyStatements(c, conn, reverse)
				restored := readRefreshFixture(c, conn, source, view)
				c.Assert(observedSchedule(c, restored), qt.Equals, "EVERY 1 HOUR")
				original := "EVERY 1 HOUR"
				assertConverged(c, conn, refreshSourceDeclaration(c, format, source, view, &original), restored)
			})
		}
	}
}
