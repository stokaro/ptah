package lintcatalog_test

import (
	"regexp"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/internal/lintcatalog"
)

// shippedWith returns the shipped catalog with edit applied to the entry of
// each code, in order.
func shippedWith(c *qt.C, edits map[string]func(*lintcatalog.Entry)) []lintcatalog.Entry {
	entries := must.Must(lintcatalog.Entries())
	for code, edit := range edits {
		index := slices.IndexFunc(entries, func(entry lintcatalog.Entry) bool { return entry.Name == code })
		c.Assert(index >= 0, qt.IsTrue, qt.Commentf("the shipped catalog has no rule %s", code))
		edit(&entries[index])
	}
	return entries
}

// everyDialect widens a rule to every dialect, with the YDB verdict such a
// rule has to carry, so the edit breaks no rule but the naming rule.
func everyDialect(entry *lintcatalog.Entry) {
	entry.Dialects, entry.YDB, entry.YDBNote = nil, lintcatalog.YDBApplies, "a statement YQL has"
}

// TestValidateAcceptsTheShippedNames pins the shapes the naming rule allows,
// as the shipped catalog has them: one hazard checked per engine under one
// name, one name in both linters, and an engine category on a provider's rule
// for its engine. A gate that refused a shared name outright would pass every
// failure row below and redden this one.
func TestValidateAcceptsTheShippedNames(t *testing.T) {
	c := qt.New(t)
	names := make(map[string]string)
	for _, entry := range must.Must(lintcatalog.Entries()) {
		names[entry.Name] = entry.Name
	}

	c.Assert([]string{names["PG306"], names["MY131"], names["PG401P"], names["DDL003"], names["YD103"]}, qt.DeepEquals, []string{
		"locking/add-foreign-key", "locking/add-foreign-key", "naming/identifier-too-long", "naming/identifier-too-long", "ydb/add-column-form",
	})
	c.Assert(lintcatalog.Validate(must.Must(lintcatalog.Entries())), qt.IsNil)
}

// TestValidateRefusesABrokenName edits one shipped rule at a time so that it
// breaks the naming rule once, and reads the one problem the gate reports.
func TestValidateRefusesABrokenName(t *testing.T) {
	rows := []struct {
		name    string
		edits   map[string]func(*lintcatalog.Entry)
		problem string
	}{
		{
			name:    "no name",
			edits:   map[string]func(*lintcatalog.Entry){"DS101": func(e *lintcatalog.Entry) { e.Name = "" }},
			problem: "rule DS101 has no Ptah name",
		},
		{
			name:    "an upper-case segment",
			edits:   map[string]func(*lintcatalog.Entry){"DS101": func(e *lintcatalog.Entry) { e.Name = "Destructive/drop-table" }},
			problem: `rule DS101 is named "Destructive/drop-table", which is not two lowercase hyphenated segments joined by a slash`,
		},
		{
			name:    "one segment",
			edits:   map[string]func(*lintcatalog.Entry){"DS101": func(e *lintcatalog.Entry) { e.Name = "drop-table" }},
			problem: `rule DS101 is named "drop-table", which is not two lowercase hyphenated segments joined by a slash`,
		},
		{
			name:    "three segments",
			edits:   map[string]func(*lintcatalog.Entry){"DS101": func(e *lintcatalog.Entry) { e.Name = "destructive/drop/table" }},
			problem: `rule DS101 is named "destructive/drop/table", which is not two lowercase hyphenated segments joined by a slash`,
		},
		{
			name:    "a doubled hyphen",
			edits:   map[string]func(*lintcatalog.Entry){"DS101": func(e *lintcatalog.Entry) { e.Name = "destructive/drop--table" }},
			problem: `rule DS101 is named "destructive/drop--table", which is not two lowercase hyphenated segments joined by a slash`,
		},
		{
			name:    "an Atlas code",
			edits:   map[string]func(*lintcatalog.Entry){"DS101": func(e *lintcatalog.Entry) { e.Name = "DS102" }},
			problem: `rule DS101 is named "DS102", which is not two lowercase hyphenated segments joined by a slash`,
		},
		{
			name:    "a category no one declared",
			edits:   map[string]func(*lintcatalog.Entry){"DS101": func(e *lintcatalog.Entry) { e.Name = "lossy/drop-table" }},
			problem: `rule DS101 is named lossy/drop-table, whose category "lossy" is not declared`,
		},
		{
			name:    "an engine category on a host rule",
			edits:   map[string]func(*lintcatalog.Entry){"YD103": func(e *lintcatalog.Entry) { e.Owner = lintcatalog.OwnerHost }},
			problem: `rule YD103 is named ydb/add-column-form under the engine category "ydb", which holds only rules a provider publishes for ydb alone`,
		},
		{
			name:    "an engine category on another engine's rule",
			edits:   map[string]func(*lintcatalog.Entry){"YD103": func(e *lintcatalog.Entry) { e.Dialects = []string{"postgres"} }},
			problem: `rule YD103 is named ydb/add-column-form under the engine category "ydb", which holds only rules a provider publishes for ydb alone`,
		},
		{
			name:    "an engine category on a rule for every dialect",
			edits:   map[string]func(*lintcatalog.Entry){"YD103": everyDialect},
			problem: `rule YD103 is named ydb/add-column-form under the engine category "ydb", which holds only rules a provider publishes for ydb alone`,
		},
		{
			name:  "a shared name on overlapping dialects",
			edits: map[string]func(*lintcatalog.Entry){"PG306": func(e *lintcatalog.Entry) { e.Dialects = []string{"mariadb", "postgres"} }},
			problem: "rules MY131 and PG306 share the name locking/add-foreign-key on overlapping dialects (mysql, mariadb; mariadb, postgres): " +
				"one name is one hazard, checked at most once on each dialect",
		},
		{
			name:  "a shared name beside a rule for every dialect",
			edits: map[string]func(*lintcatalog.Entry){"PG306": everyDialect},
			problem: "rules MY131 and PG306 share the name locking/add-foreign-key on overlapping dialects (mysql, mariadb; all): " +
				"one name is one hazard, checked at most once on each dialect",
		},
		{
			name:    "a shared name with two severities",
			edits:   map[string]func(*lintcatalog.Entry){"PG306": func(e *lintcatalog.Entry) { e.Severity = "error" }},
			problem: "rules MY131 and PG306 share the name locking/add-foreign-key with different severities, warning and error",
		},
		{
			name: "a category no rule uses",
			edits: map[string]func(*lintcatalog.Entry){
				"PG110":  func(e *lintcatalog.Entry) { e.Name = "correctness/column-alignment" },
				"PG320":  func(e *lintcatalog.Entry) { e.Name = "correctness/disable-autovacuum" },
				"DDL001": func(e *lintcatalog.Entry) { e.Name = "correctness/no-primary-key" },
			},
			problem: "category design is declared and names no rule",
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			err := lintcatalog.Validate(shippedWith(c, row.edits))
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta("lint catalog is inconsistent with the code:\n  "+row.problem))
		})
	}
}

// TestShippedRulesHaveTheirOwner reads who publishes each rule: a rule an
// official provider's recorded catalog publishes is the provider's, and every
// other rule, SQL lint's included, is the host's.
func TestShippedRulesHaveTheirOwner(t *testing.T) {
	c := qt.New(t)
	owners := make(map[string]lintcatalog.Owner)
	for _, entry := range must.Must(lintcatalog.Entries()) {
		owners[entry.Name] = entry.Owner
	}

	c.Assert([]lintcatalog.Owner{owners["YD103"], owners["SP101"], owners["DS101"], owners["PG301"], owners["SQL001"]}, qt.DeepEquals,
		[]lintcatalog.Owner{lintcatalog.OwnerProvider, lintcatalog.OwnerProvider, lintcatalog.OwnerHost, lintcatalog.OwnerHost, lintcatalog.OwnerHost})
}
