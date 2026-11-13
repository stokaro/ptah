package lint_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/lintext"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// The columns of items before the change, as Ptah declares them.
var (
	itemsID    = schemamodel.Field{Name: "id", Type: "INTEGER", Primary: true}
	itemsN     = schemamodel.Field{Name: "n", Type: "INTEGER", Nullable: true}
	itemsLabel = schemamodel.Field{Name: "label", Type: "TEXT", Nullable: true}
)

// itemsTable declares items with the columns given, in order.
func itemsTable(fields ...schemamodel.Field) *schemamodel.Database {
	declared := slices.Clone(fields)
	for i := range declared {
		declared[i].StructName = "Item"
	}
	return &schemamodel.Database{Tables: []schemamodel.Table{{Name: "items", StructName: "Item"}}, Fields: declared}
}

// plannedChange is the migration Ptah's SQLite planner writes to turn the
// items of current into the items of desired.
func plannedChange(c *qt.C, current, desired *schemamodel.Database) string {
	c.Helper()
	runtime := must.Must(builtin.New())
	diff, err := schemadiff.CompareSchemas(c.Context(), desired, current, platform.SQLite, runtime)
	c.Assert(err, qt.IsNil)
	sql, err := planner.GenerateSchemaDiffSQL(c.Context(), runtime, diff, platform.SQLite)
	c.Assert(err, qt.IsNil)
	return sql
}

// planLintRun is one way a planned migration is linted.
type planLintRun struct {
	name          string
	compatibility lint.CompatibilityProfile
	// baseline is the columns of items the dev database reports before the
	// change, empty for a run without one.
	baseline []string
}

var (
	nativeWithDevDatabase    = planLintRun{name: "native, with a dev database", baseline: []string{"id", "n", "label"}}
	nativeWithoutDevDatabase = planLintRun{name: "native, without a dev database"}
	atlasWithDevDatabase     = planLintRun{
		name: "Atlas profile, with a dev database", compatibility: lint.CompatibilityProfileAtlas,
		baseline: []string{"id", "n", "label"},
	}
)

// plannedRebuildRules lints the planned change as the second version of a
// directory and returns the rules a rebuild meets that report on it.
func plannedRebuildRules(c *qt.C, change string, run planLintRun) []string {
	c.Helper()
	opts := lint.Options{Dialect: "sqlite", Compatibility: run.compatibility}
	for _, name := range run.baseline {
		opts.Baseline = append(opts.Baseline, lintext.BaselineColumn{Version: 2, Table: "items", Name: name})
	}
	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
		"0000000001_items.up.sql":  sqliteItems,
		"0000000002_change.up.sql": change,
	}), opts)
	c.Assert(err, qt.IsNil)
	var rules []string
	for _, finding := range findings {
		rules = append(rules, finding.File+":"+finding.Rule)
	}
	return slices.DeleteFunc(rules, func(rule string) bool {
		return !strings.HasPrefix(rule, "0000000002_change.up.sql:") || !slices.Contains(
			[]string{"DS101", "DS102", "BC101", "BC103"}, strings.TrimPrefix(rule, "0000000002_change.up.sql:"))
	})
}

// Each row is a change SQLite's ALTER TABLE cannot express, planned by Ptah's
// SQLite planner into a rebuild that keeps every value the author did not ask
// to lose. The copy statement is asserted first, so a row tests the SELECT
// list the planner writes: the column itself, and IFNULL with the declared
// default where a column becomes NOT NULL, which changes a NULL and no value.
// Lint reads each as a rebuild on every surface, with or without the old
// table's columns.
func TestPlannedSQLiteRebuild_IsNotADrop(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
		copy    string
	}{
		{
			name:    "a column's type",
			desired: itemsTable(itemsID, schemamodel.Field{Name: "n", Type: "TEXT", Nullable: true}, itemsLabel),
			copy:    `INSERT INTO "new_items" ("id", "n", "label") SELECT "id", "n", "label" FROM "items";`,
		},
		{
			name:    "a column made NOT NULL with a default",
			desired: itemsTable(itemsID, itemsN, schemamodel.Field{Name: "label", Type: "TEXT", Default: "'x'"}),
			copy:    `INSERT INTO "new_items" ("id", "n", "label") SELECT "id", "n", IFNULL("label", 'x') AS "label" FROM "items";`,
		},
		{
			name: "a column ALTER TABLE cannot add",
			desired: itemsTable(itemsID, itemsN, itemsLabel,
				schemamodel.Field{Name: "code", Type: "TEXT", Nullable: true, Unique: true}),
			copy: `INSERT INTO "new_items" ("id", "n", "label") SELECT "id", "n", "label" FROM "items";`,
		},
	}
	for _, test := range tests {
		for _, run := range []planLintRun{nativeWithDevDatabase, nativeWithoutDevDatabase, atlasWithDevDatabase} {
			t.Run(test.name+", "+run.name, func(t *testing.T) {
				c := qt.New(t)
				change := plannedChange(c, itemsTable(itemsID, itemsN, itemsLabel), test.desired)
				c.Assert(change, qt.Contains, test.copy)
				c.Assert(plannedRebuildRules(c, change, run), qt.HasLen, 0)
			})
		}
	}
}

// A rebuild that drops a column the author removed loses that column's
// values, and lint says so where it knows the old table: the native surface
// reports the drop, and the Atlas profile the column.
func TestPlannedSQLiteRebuild_ReportsTheDroppedColumn(t *testing.T) {
	tests := []struct {
		run  planLintRun
		want []string
	}{
		{
			run:  nativeWithDevDatabase,
			want: []string{"0000000002_change.up.sql:BC103", "0000000002_change.up.sql:DS101"},
		},
		{
			run:  atlasWithDevDatabase,
			want: []string{"0000000002_change.up.sql:DS102"},
		},
	}
	for _, test := range tests {
		t.Run(test.run.name, func(t *testing.T) {
			c := qt.New(t)
			change := plannedChange(c, itemsTable(itemsID, itemsN, itemsLabel), itemsTable(itemsID, itemsN))
			c.Assert(change, qt.Contains, `INSERT INTO "new_items" ("id", "n") SELECT "id", "n" FROM "items";`)
			c.Assert(plannedRebuildRules(c, change, test.run), qt.DeepEquals, test.want)
		})
	}
}
