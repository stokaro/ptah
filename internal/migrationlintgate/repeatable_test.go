package migrationlintgate_test

import (
	"fmt"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
	"ptah.run/internal/migrationlintgate"
	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// repeatableGateFS is a Ptah directory whose versioned migration is safe and
// whose repeatable migration drops a table.
func repeatableGateFS() fstest.MapFS {
	return fstest.MapFS{
		"0000000001_widgets.up.sql":   {Data: []byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n")},
		"0000000001_widgets.down.sql": {Data: []byte("DROP TABLE widgets;\n")},
		"R__cleanup.sql":              {Data: []byte("DROP TABLE gadgets;\n")},
	}
}

func TestAnalyzeRepeatables_HappyPath(t *testing.T) {
	t.Run("a repeatable migration the run does not execute is not judged", func(t *testing.T) {
		c := qt.New(t)
		findings, err := migrationlintgate.Analyze(c.Context(), builtintest.Runtime(), repeatableGateFS(),
			migrationlintgate.Pending{Versions: []int64{1}}, "sqlite", "")
		c.Assert(err, qt.IsNil)
		c.Assert(findings, qt.HasLen, 0)
	})
}

func TestAnalyzeRepeatables_FailurePath(t *testing.T) {
	t.Run("a repeatable migration the run executes is judged", func(t *testing.T) {
		c := qt.New(t)
		findings, err := migrationlintgate.Analyze(c.Context(), builtintest.Runtime(), repeatableGateFS(),
			migrationlintgate.Pending{Repeatables: []string{"R__cleanup.sql"}}, "sqlite", "migrations")
		c.Assert(err, qt.IsNil)
		c.Assert(gateSites(findings), qt.DeepEquals, []string{"migrations/R__cleanup.sql:1:destructive/drop-table"})
	})

	t.Run("the gate reads the directory in the format the apply reads it in", func(t *testing.T) {
		c := qt.New(t)
		fsys := fstest.MapFS{"R__cleanup.sql": {Data: []byte("DROP TABLE gadgets;\n")}}
		policy, err := migrationlintgate.LoadPolicy(builtintest.Runtime(), fsys, "sqlite")
		c.Assert(err, qt.IsNil)
		findings, err := migrationlintgate.AnalyzeWithPolicy(c.Context(), builtintest.Runtime(), fsys,
			migrationlintgate.Pending{Repeatables: []string{"R__cleanup.sql"}},
			policy.WithDirFormat(migrationfile.DirFormatPtah), "")
		c.Assert(err, qt.IsNil)
		c.Assert(gateSites(findings), qt.DeepEquals, []string{"R__cleanup.sql:1:destructive/drop-table"})
	})
}

// gateSites returns each finding as file:line:rule.
func gateSites(findings []lint.Finding) []string {
	sites := make([]string, 0, len(findings))
	for _, finding := range findings {
		sites = append(sites, fmt.Sprintf("%s:%d:%s", finding.File, finding.Line, finding.Rule))
	}
	return sites
}
