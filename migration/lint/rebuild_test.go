package lint_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
)

// A table rebuild as Ptah's planners write it: SQLite drops the old table and
// renames the copy over it; YDB moves the old table aside, moves the copy in
// and drops the old one under its new name.
const (
	sqliteItems = "CREATE TABLE items (id INTEGER PRIMARY KEY, n INTEGER, label TEXT);\n"

	sqliteRebuild = "PRAGMA foreign_keys = off;\n" +
		"-- SQLite table rebuild for changes ALTER TABLE cannot express on items\n" +
		"CREATE TABLE \"__ptah_rebuild_items\" (\n  \"id\" INTEGER PRIMARY KEY,\n  \"n\" TEXT NOT NULL,\n  \"label\" TEXT\n);\n" +
		"INSERT INTO \"__ptah_rebuild_items\" (\"id\", \"n\", \"label\") SELECT \"id\", \"n\", \"label\" FROM \"items\";\n" +
		"DROP TABLE \"items\";\n" +
		"ALTER TABLE \"__ptah_rebuild_items\" RENAME TO \"items\";\n" +
		"PRAGMA foreign_keys = on;\n"

	ydbItems = "CREATE TABLE `app/items` (\n    `id` Int64 NOT NULL,\n    `n` Int32,\n    `label` Utf8,\n" +
		"    PRIMARY KEY (`id`)\n);\n"

	ydbRebuild = "CREATE TABLE `app/__ptah_rebuild_items` (\n    `id` Int64 NOT NULL,\n    `n` Int64,\n" +
		"    `label` Utf8,\n    PRIMARY KEY (`id`)\n);\n" +
		"INSERT INTO `app/__ptah_rebuild_items` (`id`, `n`, `label`) SELECT " +
		"Unwrap(`id`, 'rebuilding table app.items: column id holds NULL'u) AS `id`, " +
		"IF(`n` IS NULL, NULL, Unwrap(CAST(`n` AS Int64), 'rebuilding table app.items: column n does not convert'u)) AS `n`, " +
		"`label` AS `label` FROM `app/items`;\n" +
		"ALTER TABLE `app/items` RENAME TO `app/__ptah_replaced_items`;\n" +
		"ALTER TABLE `app/__ptah_rebuild_items` RENAME TO `app/items`;\n" +
		"DROP TABLE `app/__ptah_replaced_items`;\n"
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
		opts.Baseline = append(opts.Baseline, lint.BaselineColumn{Version: 2, Table: "items", Name: name})
	}
	if test.dialect == "ydb" {
		target, err := lint.ResolveTarget("ydb", "")
		c.Assert(err, qt.IsNil)
		opts.Target = target
	}
	findings, err := lint.LintFS(fixture(map[string]string{
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
		{name: "YDB, with the table's history in the directory", dialect: "ydb", first: ydbItems, second: ydbRebuild},
		{
			name: "YDB, with a table the directory did not create", dialect: "ydb",
			first: "CREATE TABLE `app/other` (`id` Int64 NOT NULL, PRIMARY KEY (`id`));\n", second: ydbRebuild,
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
			rebuildCase: rebuildCase{name: "a copy that leaves out a column the directory created", dialect: "ydb",
				first: ydbItems, second: strings.NewReplacer("(`id`, `n`, `label`) SELECT", "(`id`, `n`) SELECT",
					", `label` AS `label` FROM", " FROM").Replace(ydbRebuild)},
			want: []string{
				"0000000002_change.up.sql:8:BC101", "0000000002_change.up.sql:10:BC103", "0000000002_change.up.sql:10:DS101",
			},
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
				second: strings.Replace(sqliteRebuild, `INSERT INTO "__ptah_rebuild_items"`, `INSERT INTO "archive"`, 1)},
			want: []string{"0000000002_change.up.sql:9:BC103", "0000000002_change.up.sql:9:DS101"},
		},
		{
			rebuildCase: rebuildCase{name: "a copy into a table the file did not just create", dialect: "sqlite",
				first: sqliteItems + "CREATE TABLE \"__ptah_rebuild_items\" (id INTEGER PRIMARY KEY, n TEXT, label TEXT);\n",
				second: "INSERT INTO \"__ptah_rebuild_items\" (\"id\", \"n\", \"label\") SELECT \"id\", \"n\", \"label\" FROM \"items\";\n" +
					"DROP TABLE \"items\";\nALTER TABLE \"__ptah_rebuild_items\" RENAME TO \"items\";\n"},
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
// columns, and the run says so. YDB reads the directory's history instead and
// asks for nothing.
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
		{rebuildCase: rebuildCase{name: "YDB", dialect: "ydb", first: ydbItems, second: ydbRebuild}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			opts := lint.Options{Dialect: test.dialect}
			for _, name := range test.baseline {
				opts.Baseline = append(opts.Baseline, lint.BaselineColumn{Version: 2, Table: "items", Name: name})
			}
			analysis, err := lint.AnalyzeFS(fixture(map[string]string{
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
	findings, err := lint.LintFS(fixture(map[string]string{
		"0000000001_items.up.sql":  sqliteItems,
		"0000000002_change.up.sql": sqliteRebuild,
	}), lint.Options{Dialect: "sqlite", Compatibility: lint.CompatibilityProfileAtlas})

	c.Assert(err, qt.IsNil)
	c.Assert(findingSites(findings), qt.Contains, "0000000002_change.up.sql:9:DS101")
}
