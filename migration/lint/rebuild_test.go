package lint_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
)

// A table rebuild as Ptah's SQLite planner writes it: it drops the old table
// and renames the copy over it.
const (
	sqliteItems = "CREATE TABLE items (id INTEGER PRIMARY KEY, n INTEGER, label TEXT);\n"

	sqliteRebuild = "PRAGMA foreign_keys = off;\n" +
		"-- SQLite table rebuild for changes ALTER TABLE cannot express on items\n" +
		"CREATE TABLE \"new_items\" (\n  \"id\" INTEGER PRIMARY KEY,\n  \"n\" TEXT NOT NULL,\n  \"label\" TEXT\n);\n" +
		"INSERT INTO \"new_items\" (\"id\", \"n\", \"label\") SELECT \"id\", \"n\", \"label\" FROM \"items\";\n" +
		"DROP TABLE \"items\";\n" +
		"ALTER TABLE \"new_items\" RENAME TO \"items\";\n" +
		"PRAGMA foreign_keys = on;\n"
)

// rebuildCase is a directory whose second version is the statement sequence
// under test.
type rebuildCase struct {
	name    string
	dialect string
	first   string
	second  string
	// baseline is the columns of items the dev database reports before the
	// second version, empty for a run without one.
	baseline []string
}

// destructiveSites lints c's directory and keeps the findings of the rules a
// rebuild meets: the dropped table, the rename and the retired name.
func destructiveSites(c *qt.C, test rebuildCase) []string {
	c.Helper()
	opts := lint.Options{Dialect: test.dialect}
	for _, name := range test.baseline {
		opts.Baseline = append(opts.Baseline, lintext.BaselineColumn{Version: 2, Table: "items", Name: name})
	}
	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
		"0000000001_items.up.sql":  test.first,
		"0000000002_change.up.sql": test.second,
	}), opts)
	c.Assert(err, qt.IsNil)
	return slices.DeleteFunc(findingSites(findings), func(site string) bool {
		return !strings.HasSuffix(site, ":DS101") && !strings.HasSuffix(site, ":BC101") && !strings.HasSuffix(site, ":BC103")
	})
}

// The rebuild keeps every row under the old name, so neither its drop nor its
// renames are reported: not when the old table's columns are unknown, and not
// when they are known and the copy carries each of them.
func TestTableRebuild_IsNotADrop(t *testing.T) {
	tests := []rebuildCase{
		{name: "SQLite, without a dev database", dialect: "sqlite", first: sqliteItems, second: sqliteRebuild},
		{
			name: "SQLite, with the dev database's columns", dialect: "sqlite", first: sqliteItems, second: sqliteRebuild,
			baseline: []string{"id", "n", "label"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(destructiveSites(c, test), qt.HasLen, 0)
		})
	}
}

// Each row is a drop the rebuild shape does not cover, and the rules report
// it as they report any drop.
func TestTableRebuild_ReportsWhatIsNotARebuild(t *testing.T) {
	tests := []struct {
		rebuildCase
		want []string
	}{
		{
			rebuildCase: rebuildCase{name: "a plain drop", dialect: "sqlite", first: sqliteItems,
				second: "DROP TABLE items;\n"},
			want: []string{"0000000002_change.up.sql:1:BC103", "0000000002_change.up.sql:1:DS101"},
		},
		{
			rebuildCase: rebuildCase{name: "a copy that leaves out a column the dev database shows", dialect: "sqlite",
				first: sqliteItems, second: strings.Replace(sqliteRebuild,
					`("id", "n", "label") SELECT "id", "n", "label"`, `("id", "n") SELECT "id", "n"`, 1),
				baseline: []string{"id", "n", "label"}},
			want: []string{"0000000002_change.up.sql:9:BC103", "0000000002_change.up.sql:9:DS101"},
		},
		{
			rebuildCase: rebuildCase{name: "a copy that keeps some rows", dialect: "sqlite", first: sqliteItems,
				second: strings.Replace(sqliteRebuild, `FROM "items";`, `FROM "items" WHERE "id" > 10;`, 1)},
			want: []string{"0000000002_change.up.sql:9:BC103", "0000000002_change.up.sql:9:DS101"},
		},
		{
			rebuildCase: rebuildCase{name: "a copy that folds equal rows", dialect: "sqlite", first: sqliteItems,
				second: strings.Replace(sqliteRebuild, `SELECT "id"`, `SELECT DISTINCT "id"`, 1)},
			want: []string{"0000000002_change.up.sql:9:BC103", "0000000002_change.up.sql:9:DS101"},
		},
		{
			rebuildCase: rebuildCase{name: "a write to the old table between the copy and the drop", dialect: "sqlite",
				first: sqliteItems, second: strings.Replace(sqliteRebuild, "DROP TABLE \"items\";\n",
					"INSERT INTO items (id, n) VALUES (99, 1);\nDROP TABLE \"items\";\n", 1)},
			want: []string{"0000000002_change.up.sql:10:BC103", "0000000002_change.up.sql:10:DS101"},
		},
		{
			rebuildCase: rebuildCase{name: "a copy of another table", dialect: "sqlite",
				first:  sqliteItems + "CREATE TABLE archive (id INTEGER PRIMARY KEY, n INTEGER, label TEXT);\n",
				second: strings.Replace(sqliteRebuild, `FROM "items";`, `FROM "archive";`, 1)},
			want: []string{"0000000002_change.up.sql:9:BC103", "0000000002_change.up.sql:9:DS101"},
		},
		{
			rebuildCase: rebuildCase{name: "a copy into another table than the one just created", dialect: "sqlite",
				first:  sqliteItems + "CREATE TABLE archive (id INTEGER PRIMARY KEY, n TEXT, label TEXT);\n",
				second: strings.Replace(sqliteRebuild, `INSERT INTO "new_items"`, `INSERT INTO "archive"`, 1)},
			want: []string{"0000000002_change.up.sql:9:BC103", "0000000002_change.up.sql:9:DS101"},
		},
		{
			rebuildCase: rebuildCase{name: "a copy into a table the file did not just create", dialect: "sqlite",
				first: sqliteItems + "CREATE TABLE \"new_items\" (id INTEGER PRIMARY KEY, n TEXT, label TEXT);\n",
				second: "INSERT INTO \"new_items\" (\"id\", \"n\", \"label\") SELECT \"id\", \"n\", \"label\" FROM \"items\";\n" +
					"DROP TABLE \"items\";\nALTER TABLE \"new_items\" RENAME TO \"items\";\n"},
			want: []string{
				"0000000002_change.up.sql:2:BC103", "0000000002_change.up.sql:2:DS101", "0000000002_change.up.sql:3:BC101",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(destructiveSites(c, test.rebuildCase), qt.DeepEquals, test.want)
		})
	}
}

// Without a dev database the copy is not checked against the old table's
// columns, and the run says so.
func TestTableRebuild_AsksForTheStartingStateItCannotRead(t *testing.T) {
	tests := []struct {
		rebuildCase
		want []string
	}{
		{
			rebuildCase: rebuildCase{name: "SQLite without a dev database", dialect: "sqlite", first: sqliteItems, second: sqliteRebuild},
			want:        []string{"DS101"},
		},
		{
			rebuildCase: rebuildCase{name: "SQLite with the dev database's columns", dialect: "sqlite", first: sqliteItems,
				second: sqliteRebuild, baseline: []string{"id", "n", "label"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			opts := lint.Options{Dialect: test.dialect}
			for _, name := range test.baseline {
				opts.Baseline = append(opts.Baseline, lintext.BaselineColumn{Version: 2, Table: "items", Name: name})
			}
			analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
				"0000000001_items.up.sql":  test.first,
				"0000000002_change.up.sql": test.second,
			}), opts)
			c.Assert(err, qt.IsNil)

			var rules []string
			for _, unmet := range analysis.UnmetInputs() {
				rules = append(rules, unmet.Rule)
			}
			c.Assert(rules, qt.DeepEquals, test.want)
		})
	}
}

// The compatibility surface reports the rebuild's drop as it reports any
// drop: the analyzer it is compatible with was not measured on this shape.
func TestTableRebuild_IsADropOnTheCompatibilitySurface(t *testing.T) {
	c := qt.New(t)
	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
		"0000000001_items.up.sql":  sqliteItems,
		"0000000002_change.up.sql": sqliteRebuild,
	}), lint.Options{Dialect: "sqlite", Compatibility: lint.CompatibilityProfileAtlas})

	c.Assert(err, qt.IsNil)
	c.Assert(findingSites(findings), qt.Contains, "0000000002_change.up.sql:9:DS101")
}

func TestTableRebuild_AtlasSQLiteReportsOnlyLostColumns(t *testing.T) {
	tests := []struct {
		name   string
		second string
		want   []string
	}{
		{name: "full copy", second: sqliteRebuild, want: []string{}},
		{name: "lost label", second: strings.Replace(sqliteRebuild,
			`("id", "n", "label") SELECT "id", "n", "label"`,
			`("id", "n") SELECT "id", "n"`, 1),
			want: []string{"0000000002_change.up.sql:3:DS102"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
				"0000000001_items.up.sql":  sqliteItems,
				"0000000002_change.up.sql": test.second,
			}), lint.Options{
				Dialect: "sqlite", Compatibility: lint.CompatibilityProfileAtlas,
				Baseline: []lintext.BaselineColumn{
					{Version: 2, Table: "items", Name: "id"},
					{Version: 2, Table: "items", Name: "n"},
					{Version: 2, Table: "items", Name: "label"},
				},
			})
			c.Assert(err, qt.IsNil)
			got := slices.DeleteFunc(findingSites(analysis.Findings()), func(site string) bool {
				return !strings.HasPrefix(site, "0000000002_change.up.sql:") ||
					(!strings.HasSuffix(site, ":DS101") && !strings.HasSuffix(site, ":DS102") &&
						!strings.HasSuffix(site, ":BC101") && !strings.HasSuffix(site, ":BC103"))
			})
			got = append([]string{}, got...)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
