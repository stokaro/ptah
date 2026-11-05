package capabilityprobe

import (
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// withViewKeys adds the question a planner decides a changed view with:
// whether the target replaces a view's query in one statement, or the view
// has to be dropped and created again.
//
// Every engine is asked the same experiment in its own spelling: create a view
// over a table with a row, replace its query with one that filters the row
// out, and count. A replacement that was accepted and changed nothing is not
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
// SQL Server spells the statement CREATE OR ALTER VIEW.
func viewReplacement(dialect string, t typeKeySpelling) experiment {
	replace := "CREATE OR REPLACE VIEW"
	if dialect == platform.SQLServer {
		replace = "CREATE OR ALTER VIEW"
	}
	view := func(verb, filter string) string {
		return fmt.Sprintf("%s vk_v AS SELECT n FROM vk_src%s", verb, filter)
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
