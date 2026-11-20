package lintcatalog_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/internal/lintcatalog"
)

// entry is a catalog row with the fields the naming rule reads and a meaning,
// so a failure row breaks the naming rule and nothing else.
func entry(name string, kind lintcatalog.Kind, owner lintcatalog.Owner, severity string, dialects ...string) lintcatalog.Entry {
	return lintcatalog.Entry{Name: name, Kind: kind, Owner: owner, Severity: severity, Dialects: dialects, Summary: "a meaning",
		YDB: lintcatalog.YDBApplies, YDBNote: "a statement YQL has"}
}

// TestValidateAcceptsTheShippedNames pins the shapes the naming rule allows,
// as the shipped catalog has them: one hazard checked per engine under one
// entry, one name in both linters, and an engine category on a provider's rule
// for its engine.
func TestValidateAcceptsTheShippedNames(t *testing.T) {
	c := qt.New(t)
	entries := must.Must(lintcatalog.Entries())
	type nameKind struct {
		name string
		kind lintcatalog.Kind
	}
	counts := make(map[string]int)
	seen := make(map[nameKind]bool)
	for _, e := range entries {
		counts[e.Name]++
		seen[nameKind{e.Name, e.Kind}] = true
	}

	c.Assert(counts["locking/add-foreign-key"], qt.Equals, 1)
	c.Assert(seen[nameKind{"naming/identifier-too-long", lintcatalog.KindMigration}], qt.IsTrue)
	c.Assert(seen[nameKind{"naming/identifier-too-long", lintcatalog.KindSQL}], qt.IsTrue)
	c.Assert(counts["ydb/add-column-form"], qt.Equals, 1)
	c.Assert(lintcatalog.Validate(entries), qt.IsNil)
}

func TestValidate_RefusesABrokenName(t *testing.T) {
	host, provider := lintcatalog.OwnerHost, lintcatalog.OwnerProvider
	migration, sql := lintcatalog.KindMigration, lintcatalog.KindSQL
	rows := []struct {
		name    string
		entries []lintcatalog.Entry
		problem string
	}{
		{name: "no name", entries: []lintcatalog.Entry{entry("", migration, host, "error", "postgres")}, problem: `rule "" is not two lowercase hyphenated segments joined by a slash`},
		{name: "an upper-case segment", entries: []lintcatalog.Entry{entry("Locking/add-check", migration, host, "error", "postgres")}, problem: `is not two lowercase hyphenated segments`},
		{name: "one segment", entries: []lintcatalog.Entry{entry("locking", migration, host, "error", "postgres")}, problem: `is not two lowercase hyphenated segments`},
		{name: "three segments", entries: []lintcatalog.Entry{entry("locking/add/check", migration, host, "error", "postgres")}, problem: `is not two lowercase hyphenated segments`},
		{name: "a doubled hyphen", entries: []lintcatalog.Entry{entry("locking/add--check", migration, host, "error", "postgres")}, problem: `is not two lowercase hyphenated segments`},
		{name: "a code", entries: []lintcatalog.Entry{entry("PG305", migration, host, "error", "postgres")}, problem: `is not two lowercase hyphenated segments`},
		{name: "a category no one declared", entries: []lintcatalog.Entry{entry("nosuch/add-check", migration, host, "error", "postgres")}, problem: `names a category "nosuch" that is not declared`},
		{name: "an engine category on a host rule", entries: []lintcatalog.Entry{entry("ydb/add-check", migration, host, "error", "ydb")}, problem: `under the engine category "ydb"`},
		{name: "an engine category on another engine's rule", entries: []lintcatalog.Entry{entry("ydb/add-check", migration, provider, "error", "spanner")}, problem: `under the engine category "ydb"`},
		{name: "an engine category on a rule for every dialect", entries: []lintcatalog.Entry{entry("ydb/add-check", migration, provider, "error")}, problem: `under the engine category "ydb"`},
		{
			name: "a name at two severities across the linters",
			entries: []lintcatalog.Entry{
				entry("naming/identifier-too-long", migration, host, "warning", "postgres"),
				entry("naming/identifier-too-long", sql, host, "error"),
			},
			problem: `one name has one default severity`,
		},
		{
			name: "a name twice in one linter",
			entries: []lintcatalog.Entry{
				entry("locking/add-check", migration, host, "warning", "postgres"),
				entry("locking/add-check", migration, host, "warning", "mysql"),
			},
			problem: `rule locking/add-check has two entries in migration lint`,
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			err := lintcatalog.Validate(row.entries)
			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, row.problem)
		})
	}
}

// TestShippedRulesHaveTheirOwner reads who publishes each rule: a rule an
// official provider's recorded catalog publishes alone is the provider's, and
// every other rule, SQL lint's and a name the host shares with a provider
// included, is the host's.
func TestShippedRulesHaveTheirOwner(t *testing.T) {
	c := qt.New(t)
	owners := make(map[string]lintcatalog.Owner)
	for _, e := range must.Must(lintcatalog.Entries()) {
		owners[e.Name] = e.Owner
	}

	c.Assert([]lintcatalog.Owner{
		owners["ydb/add-column-form"], owners["side-effect/drop-row-deletion-policy"], owners["destructive/drop-table"],
		owners["rewrite/alter-column-type"], owners["correctness/syntax-error"], owners["data-dependent/set-not-null"],
	}, qt.DeepEquals, []lintcatalog.Owner{
		lintcatalog.OwnerProvider, lintcatalog.OwnerProvider, lintcatalog.OwnerHost, lintcatalog.OwnerHost, lintcatalog.OwnerHost, lintcatalog.OwnerHost,
	})
}
