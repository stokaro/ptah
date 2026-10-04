package lintcatalog_test

import (
	"maps"
	"regexp"
	"slices"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/lintcatalog"
	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// ydbAnalysis lints a migration directory as a YDB run, with a naming
// convention every name below violates.
func ydbAnalysis(c *qt.C, files map[string]string) lint.Analysis {
	c.Helper()
	fsys := fstest.MapFS{}
	for name, content := range files {
		fsys[name] = &fstest.MapFile{Data: []byte(content)}
	}
	analysis, err := lint.AnalyzeFS(fsys, lint.Options{
		Dialect:   "ydb",
		DirFormat: migrationfile.DirFormatPtah,
		Naming:    &lint.NamingConfig{Match: "^z+$"},
	})
	c.Assert(err, qt.IsNil)
	return analysis
}

func reported(analysis lint.Analysis) []string {
	var codes []string
	for _, finding := range analysis.Findings() {
		codes = append(codes, finding.Rule)
	}
	slices.Sort(codes)
	return slices.Compact(codes)
}

// ydbBase is a version creating the table the fixtures below change.
var ydbBase = map[string]string{
	"0000000001_users.up.sql": "CREATE TABLE `shop/users` (id Uint64 NOT NULL, email Utf8 NOT NULL, " +
		"name Utf8, PRIMARY KEY (id), INDEX users_name GLOBAL ON (name));\n",
	"0000000001_users.down.sql": "DROP TABLE `shop/users`;\n",
}

// withVersion adds a second version to the base directory.
func withVersion(up, down string) map[string]string {
	files := maps.Clone(ydbBase)
	files["0000000002_change.up.sql"] = up
	if down != "" {
		files["0000000002_change.down.sql"] = down
	}
	return files
}

// ydbAppliesFixtures is YQL each rule marked YDBApplies must report on a YDB
// run. The keys are compared with the catalog both ways, so a verdict cannot
// claim a rule reads YQL without a fixture showing it does.
var ydbAppliesFixtures = map[string]map[string]string{
	"DS101":  withVersion("DROP TABLE `shop/users`;\n", "-- nothing\n"),
	"BC103":  withVersion("DROP TABLE `shop/users`;\n", "-- nothing\n"),
	"DS102":  withVersion("ALTER TABLE `shop/users` DROP COLUMN email;\n", "-- nothing\n"),
	"BC104":  withVersion("ALTER TABLE `shop/users` DROP COLUMN email;\n", "-- nothing\n"),
	"DS104":  withVersion("ALTER TABLE `shop/users` ALTER COLUMN email DROP NOT NULL;\n", "-- nothing\n"),
	"DS108":  withVersion("TRUNCATE TABLE `shop/users`;\n", "-- nothing\n"),
	"BC101":  withVersion("ALTER TABLE `shop/users` RENAME TO `shop/accounts`;\n", "-- nothing\n"),
	"MF101P": withVersion("ALTER TABLE `shop/users` ADD COLUMN zz Utf8;\n", ""),
	"MF102P": withVersion("-- nothing here\n", "-- nothing\n"),
	"MF103": {
		"0000000001_users.up.sql":   ydbBase["0000000001_users.up.sql"],
		"0000000001_users.down.sql": ydbBase["0000000001_users.down.sql"],
		"0000000002-change.up.sql":  "ALTER TABLE `shop/users` ADD COLUMN zz Utf8;\n",
	},
	"NM102": withVersion("CREATE TABLE `shop/Orders` (zz Uint64 NOT NULL, PRIMARY KEY (zz));\n", "-- nothing\n"),
	"NM103": withVersion("ALTER TABLE `shop/users` ADD COLUMN Score Int32;\n", "-- nothing\n"),
	"NM104": withVersion("ALTER TABLE `shop/users` ADD INDEX Users_Score GLOBAL ON (email);\n", "-- nothing\n"),
}

// ydbAppliesWithTheFlagFixtures is the same for MF101 and MF102, which read
// YQL's unique index and report on a target that adds one to an existing
// table; a target that does not is YD101's.
var ydbAppliesWithTheFlagFixtures = map[string]map[string]string{
	"MF101": withVersion("ALTER TABLE `shop/users` ADD INDEX users_email GLOBAL UNIQUE SYNC ON (email);\n", "-- nothing\n"),
	"MF102": withVersion("ALTER TABLE `shop/users` DROP INDEX users_name;\n"+
		"ALTER TABLE `shop/users` ADD INDEX users_name GLOBAL UNIQUE SYNC ON (name);\n", "-- nothing\n"),
}

func codesWithVerdict(c *qt.C, verdict lintcatalog.YDBVerdict) []string {
	entries, err := lintcatalog.MigrationEntries()
	c.Assert(err, qt.IsNil)
	var codes []string
	for _, entry := range entries {
		if entry.YDB == verdict {
			codes = append(codes, entry.Code)
		}
	}
	slices.Sort(codes)
	return codes
}

func TestYDBVerdicts_EveryRuleThatAppliesHasAFixture(t *testing.T) {
	c := qt.New(t)

	var fixtures []string
	for code := range ydbAppliesFixtures {
		fixtures = append(fixtures, code)
	}
	for code := range ydbAppliesWithTheFlagFixtures {
		fixtures = append(fixtures, code)
	}
	slices.Sort(fixtures)

	c.Assert(codesWithVerdict(c, lintcatalog.YDBApplies), qt.DeepEquals, fixtures)
}

func TestYDBVerdicts_RulesThatApplyReportYQL(t *testing.T) {
	for code, files := range ydbAppliesFixtures {
		t.Run(code, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(reported(ydbAnalysis(c, files)), qt.Contains, code)
		})
	}
}

func TestYDBVerdicts_UniqueIndexRulesReportYQLWhereTheTargetAllowsIt(t *testing.T) {
	for code, files := range ydbAppliesWithTheFlagFixtures {
		t.Run(code, func(t *testing.T) {
			c := qt.New(t)
			target, err := lint.ResolveTarget("ydb", "")
			c.Assert(err, qt.IsNil)
			target.Capabilities = target.Capabilities.With("unique_index_on_existing_table", true)
			fsys := fstest.MapFS{}
			for name, content := range files {
				fsys[name] = &fstest.MapFile{Data: []byte(content)}
			}

			analysis, err := lint.AnalyzeFS(fsys, lint.Options{Dialect: "ydb", Target: target, DirFormat: migrationfile.DirFormatPtah})

			c.Assert(err, qt.IsNil)
			c.Assert(reported(analysis), qt.Contains, code)
		})
	}
}

// DD101 is the one rule marked replaced: the statement it reads is one YDB
// refuses on every line, which YD103 says instead.
func TestYDBVerdicts_ReplacedRulesGiveWayToTheirYDRule(t *testing.T) {
	c := qt.New(t)

	codes := reported(ydbAnalysis(c, withVersion("ALTER TABLE `shop/users` ADD COLUMN zz Int64 NOT NULL;\n", "-- nothing\n")))

	c.Assert(codesWithVerdict(c, lintcatalog.YDBReplaced), qt.DeepEquals, []string{"DD101"})
	c.Assert(codes, qt.Contains, "YD103")
	c.Assert(codes, qt.Not(qt.Contains), "DD101")
}

// DS110P is the one rule marked as needing a dev database, and a YDB run names
// it as unmet where it asked for the state.
func TestYDBVerdicts_RulesThatNeedADevDatabaseAreNamedUnmet(t *testing.T) {
	c := qt.New(t)

	analysis := ydbAnalysis(c, withVersion("ALTER TABLE `shop/users` DROP COLUMN email;\n", "-- nothing\n"))
	var unmet []string
	for _, input := range analysis.UnmetInputs() {
		unmet = append(unmet, input.Rule)
	}

	c.Assert(codesWithVerdict(c, lintcatalog.YDBNeedsDevDatabase), qt.DeepEquals, []string{"DS110P"})
	c.Assert(unmet, qt.Contains, "DS110P")
}

// A migration lint rule that runs on every dialect has to say what it does on
// YDB, and a rule that does not run on every dialect must not.
func TestValidate_RefusesAMissingOrMisplacedYDBVerdict(t *testing.T) {
	tests := []struct {
		name    string
		entry   lintcatalog.Entry
		message string
	}{
		{
			name:    "a rule for every dialect without a verdict",
			entry:   lintcatalog.Entry{Code: "DD901", Kind: lintcatalog.KindMigration, Summary: "invented"},
			message: "rule DD901 runs on every dialect and says nothing known about YDB",
		},
		{
			name: "a verdict nobody declared",
			entry: lintcatalog.Entry{Code: "DD901", Kind: lintcatalog.KindMigration, Summary: "invented",
				YDB: "probably fine", YDBNote: "a guess"},
			message: "rule DD901 runs on every dialect and says nothing known about YDB",
		},
		{
			name: "a verdict without a note",
			entry: lintcatalog.Entry{Code: "DD901", Kind: lintcatalog.KindMigration, Summary: "invented",
				YDB: lintcatalog.YDBApplies},
			message: "rule DD901 has a YDB verdict and no note",
		},
		{
			name: "a verdict on a rule for one dialect",
			entry: lintcatalog.Entry{Code: "DD901", Kind: lintcatalog.KindMigration, Summary: "invented",
				Dialects: []string{"postgres"}, YDB: lintcatalog.YDBApplies, YDBNote: "`DROP TABLE`"},
			message: "rule DD901 declares a YDB verdict but does not run on every dialect",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := lintcatalog.Validate(append(shippedEntries(c), test.entry))
			c.Assert(err, qt.ErrorMatches, `lint catalog is inconsistent with the code:\n  `+regexp.QuoteMeta(test.message)+`.*`)
		})
	}
}

// The control for the rows above: the same shipped catalog with a rule for
// every dialect that declares a verdict and a note, a YDB rule and an SQL
// lint rule that declare none, is accepted.
func TestValidate_AcceptsDeclaredYDBVerdicts(t *testing.T) {
	c := qt.New(t)
	err := lintcatalog.Validate(append(shippedEntries(c),
		lintcatalog.Entry{Code: "DD901", Kind: lintcatalog.KindMigration, Summary: "invented", YDB: lintcatalog.YDBApplies, YDBNote: "`DROP TABLE`"},
		lintcatalog.Entry{Code: "YD901", Kind: lintcatalog.KindMigration, Summary: "invented", Dialects: []string{"ydb"}},
		lintcatalog.Entry{Code: "DDL901", Kind: lintcatalog.KindSQL, Summary: "invented"},
	))
	c.Assert(err, qt.IsNil)
}

func shippedEntries(c *qt.C) []lintcatalog.Entry {
	entries, err := lintcatalog.Entries()
	c.Assert(err, qt.IsNil)
	return entries
}
