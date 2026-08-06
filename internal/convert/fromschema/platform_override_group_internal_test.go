package fromschema

// White-box testing required: platformOverrideGroup is unexported and its
// tie-break is not observable through any exported converter, because every
// caller resolves an engine that has exactly one override group in the schemas
// this repository ships. Reaching the discard needs two sibling spellings in
// one map, which only this level can construct.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestPlatformOverrideGroup_ResolvesBySpelling pins the lookup platformOverrideGroup
// performs, including the tie-break, which nothing else in the suite reaches.
//
// The function has no test of its own, so before this file the tie-break could be
// changed from "lowest key" to "highest key" — or to a merge of the sibling groups —
// and every test in the repository would stay green while `--dialect tsql` started
// answering with a different override.
//
// The tie-break is deliberately arbitrary and it discards. With both
// `platform.tsql.type` and `platform.mssql.type` present, one group wins and the
// other becomes unreachable from every spelling, silently. That is measured here
// rather than left to be discovered: agreement across spellings is the property
// #929 asked for, and this is its cost.
func TestPlatformOverrideGroup_ResolvesBySpelling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		overrides map[string]map[string]string
		target    string
		wantOK    bool
		wantType  string
	}{
		{
			name:      "the canonical name wins over a sibling spelling",
			overrides: map[string]map[string]string{"sqlserver": {"type": "A"}, "mssql": {"type": "B"}},
			target:    "sqlserver",
			wantOK:    true,
			wantType:  "A",
		},
		{
			name:      "a non-canonical spelling reaches the canonical group",
			overrides: map[string]map[string]string{"sqlserver": {"type": "A"}},
			target:    "tsql",
			wantOK:    true,
			wantType:  "A",
		},
		{
			name:      "a canonical target reaches a group keyed by a sibling spelling",
			overrides: map[string]map[string]string{"mssql": {"type": "B"}},
			target:    "sqlserver",
			wantOK:    true,
			wantType:  "B",
		},
		{
			// The discard this pins: "mssql" sorts below "tsql", so the tsql group
			// is unreachable from every spelling of SQL Server, with no diagnostic.
			name:      "two sibling spellings tie-break to the lowest key and drop the other",
			overrides: map[string]map[string]string{"tsql": {"type": "FROM_TSQL"}, "mssql": {"type": "FROM_MSSQL"}},
			target:    "tsql",
			wantOK:    true,
			wantType:  "FROM_MSSQL",
		},
		{
			name:      "the same tie-break answers every spelling of that engine",
			overrides: map[string]map[string]string{"tsql": {"type": "FROM_TSQL"}, "mssql": {"type": "FROM_MSSQL"}},
			target:    "sql_server",
			wantOK:    true,
			wantType:  "FROM_MSSQL",
		},
		{
			name:      "an override for another engine is not borrowed",
			overrides: map[string]map[string]string{"postgres": {"type": "PG"}},
			target:    "mysql",
			wantOK:    false,
		},
		{
			name:      "no overrides at all",
			overrides: nil,
			target:    "postgres",
			wantOK:    false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			group, ok := platformOverrideGroup(test.overrides, test.target)

			c.Assert(ok, qt.Equals, test.wantOK)
			c.Assert(group["type"], qt.Equals, test.wantType)
		})
	}
}
