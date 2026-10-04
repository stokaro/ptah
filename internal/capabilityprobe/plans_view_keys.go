package capabilityprobe

import (
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/ydbview"
)

// withViewKeys adds the question a planner decides a changed view with:
// whether the target replaces a view's query in one statement, or the view
// has to be dropped and created again.
//
// Every engine is asked the same experiment in its own spelling: create a view
// over a table with a row, replace its query with one that filters the row
// out, and count. A replacement that was accepted and changed nothing is not
// a replacement. YDB is asked the standard spelling, which no line takes, so
// the line that does turns its row red.
func withViewKeys(p plan, dialect string) plan {
	normalized := platform.NormalizeDialect(dialect)
	spelling, ok := typeKeySpellingFor(normalized)
	if !ok {
		return p
	}
	p.experiments = append(p.experiments, viewReplacement(normalized, spelling))
	return p
}

// viewReplacement is the replacement experiment in t's table spelling.
// SQL Server spells the statement CREATE OR ALTER VIEW, and YDB creates no
// view without its security clause.
func viewReplacement(dialect string, t typeKeySpelling) experiment {
	replace, clause := "CREATE OR REPLACE VIEW", ""
	switch dialect {
	case platform.SQLServer:
		replace = "CREATE OR ALTER VIEW"
	case platform.YDB:
		clause = " " + ydbview.SecurityClause
	}
	view := func(verb, filter string) string {
		return fmt.Sprintf("%s vk_v%s AS SELECT n FROM vk_src%s", verb, clause, filter)
	}
	replacement := proven(capability.CreateOrReplaceView, schemaChange{
		setup: []string{
			t.table("vk_src", "n "+t.integer),
			"INSERT INTO vk_src (id, n) VALUES (1, 1)",
			view("CREATE VIEW", ""),
		},
		before: []check{counts("SELECT COUNT(*) FROM vk_v", 1)},
		change: []string{view(replace, " WHERE n > 1")},
		after:  []check{counts("SELECT COUNT(*) FROM vk_v", 0)},
	})
	replacement.requires = []capability.Capability{capability.Views}
	return replacement
}
