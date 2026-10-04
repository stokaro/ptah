package lint_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/sqlutil"
	"ptah.run/migration/lint"
)

// ydbLint lints a YDB migration directory against the release line version
// names, or the newest line when it is empty, and returns each finding as
// file:line:rule.
func ydbLint(c *qt.C, files map[string]string, version string) []string {
	c.Helper()
	target, err := lint.ResolveTarget("ydb", version)
	c.Assert(err, qt.IsNil)
	findings, err := lint.LintFS(fixture(files), lint.Options{Dialect: "ydb", Target: target})
	c.Assert(err, qt.IsNil)
	return findingSites(findings)
}

func findingSites(findings []lint.Finding) []string {
	sites := make([]string, 0, len(findings))
	for _, finding := range findings {
		sites = append(sites, fmt.Sprintf("%s:%d:%s", finding.File, finding.Line, finding.Rule))
	}
	return sites
}

// ydbSites keeps the sites of the YD family.
func ydbSites(sites []string) []string {
	return slices.DeleteFunc(slices.Clone(sites), func(site string) bool {
		return !strings.Contains(site, ":YD")
	})
}

// usersTable is an up migration creating a table the YD rows alter: an index
// keying email and covering name, and a TTL on expires.
const usersTable = "CREATE TABLE `shop/users` (\n" +
	"  id Uint64 NOT NULL, email Utf8, name Utf8, score Int32, expires Timestamp,\n" +
	"  PRIMARY KEY (id),\n" +
	"  INDEX users_email GLOBAL SYNC ON (email) COVER (name)\n" +
	") WITH (TTL = Interval(\"P30D\") ON expires);\n"

func TestYDBRules_ReportWhatTheServerRefuses(t *testing.T) {
	tests := []struct {
		name    string
		version string
		files   map[string]string
		want    []string
	}{
		{
			name: "a unique index added to a table an earlier migration created",
			files: map[string]string{
				"0001_users.up.sql":  usersTable,
				"0002_unique.up.sql": "ALTER TABLE `shop/users` ADD INDEX users_score GLOBAL UNIQUE SYNC ON (score);\n",
			},
			want: []string{"0002_unique.up.sql:1:YD101"},
		},
		{
			name:    "a unique index added to a table the same file created, on the oldest line",
			version: "25.1.4.7",
			files: map[string]string{
				"0001_users.up.sql": "CREATE TABLE t (id Uint64 NOT NULL, v Utf8, PRIMARY KEY (id));\n" +
					"ALTER TABLE t ADD INDEX t_v GLOBAL UNIQUE ON (v);\n",
			},
			want: []string{"0001_users.up.sql:2:YD101"},
		},
		{
			name: "a block that writes a row and creates a table",
			files: map[string]string{
				"0001_block.up.sql": "DO BEGIN\n  UPSERT INTO t (id) VALUES (1);\n  CREATE TABLE m (id Uint64 NOT NULL, PRIMARY KEY (id));\nEND DO;\n",
			},
			want: []string{"0001_block.up.sql:1:YD102"},
		},
		{
			name: "a call of an action defined earlier in the file",
			files: map[string]string{
				"0001_action.up.sql": "DEFINE ACTION $seed() AS\n  UPSERT INTO t (id) VALUES (1);\n  DROP TABLE old;\nEND DEFINE;\nDO $seed();\n",
			},
			want: []string{"0001_action.up.sql:5:YD102"},
		},
		{
			name: "a NOT NULL column without a default, on a table the same file created",
			files: map[string]string{
				"0001_t.up.sql": "CREATE TABLE t (id Uint64 NOT NULL, PRIMARY KEY (id));\nALTER TABLE t ADD COLUMN a Int64 NOT NULL;\n",
			},
			want: []string{"0001_t.up.sql:2:YD103"},
		},
		{
			name:    "a column default on a line without one",
			version: "25.1.4.7",
			files: map[string]string{
				"0001_t.up.sql": "ALTER TABLE t ADD COLUMN a Int64 DEFAULT 7;\n",
			},
			want: []string{"0001_t.up.sql:1:YD103"},
		},
		{
			name: "a key column, a covered column and the TTL column, each dropped",
			files: map[string]string{
				"0001_users.up.sql": usersTable,
				"0002_drop.up.sql": "ALTER TABLE `shop/users` DROP COLUMN email;\n" +
					"ALTER TABLE `shop/users` DROP COLUMN name;\n" +
					"ALTER TABLE `shop/users` DROP COLUMN expires;\n",
			},
			want: []string{"0002_drop.up.sql:1:YD104", "0002_drop.up.sql:2:YD104", "0002_drop.up.sql:3:YD104"},
		},
		{
			name: "an index added and a TTL set by ALTER TABLE in earlier migrations, on a renamed table",
			files: map[string]string{
				"0001_t.up.sql": "CREATE TABLE t (id Uint64 NOT NULL, a Utf8, b Timestamp, PRIMARY KEY (id));\n",
				"0002_t.up.sql": "ALTER TABLE t ADD INDEX t_a GLOBAL ON (a);\nALTER TABLE t SET (TTL = Interval(\"P1D\") ON b);\n" +
					"ALTER TABLE t RENAME TO `dir/u`;\n",
				"0003_t.up.sql": "ALTER TABLE `dir/u` DROP COLUMN a;\nALTER TABLE `dir/u` DROP COLUMN b;\n",
			},
			want: []string{"0003_t.up.sql:1:YD104", "0003_t.up.sql:2:YD104"},
		},
		{
			name: "a down half dropping a column its up half indexed",
			files: map[string]string{
				"0001_t.up.sql":   "CREATE TABLE t (id Uint64 NOT NULL, PRIMARY KEY (id));\n",
				"0001_t.down.sql": "DROP TABLE t;\n",
				"0002_t.up.sql":   "ALTER TABLE t ADD COLUMN v Utf8;\nALTER TABLE t ADD INDEX t_v GLOBAL ON (v);\n",
				"0002_t.down.sql": "ALTER TABLE t DROP COLUMN v;\n",
			},
			want: []string{"0002_t.down.sql:1:YD104"},
		},
		{
			name: "statements a down half runs, which the server refuses or resets the same way",
			files: map[string]string{
				"0001_t.up.sql": "ALTER TABLE t DROP COLUMN a;\n",
				"0001_t.down.sql": "ALTER TABLE t ADD COLUMN a Int64 NOT NULL;\nALTER TABLE t ADD INDEX t_a GLOBAL UNIQUE ON (a);\n" +
					"ALTER TABLE t SET (AUTO_PARTITIONING_BY_SIZE = ENABLED);\n",
			},
			want: []string{"0001_t.down.sql:1:YD103", "0001_t.down.sql:2:YD101", "0001_t.down.sql:3:YD105"},
		},
		{
			// The first statement leaves the minimum at 1, so the second,
			// which would reset it to 1, loses nothing.
			name: "auto partitioning turned on without the minimum, then again",
			files: map[string]string{
				"0001_t.up.sql": "ALTER TABLE t SET (AUTO_PARTITIONING_BY_SIZE = ENABLED);\nALTER TABLE t SET AUTO_PARTITIONING_BY_LOAD ENABLED;\n",
			},
			want: []string{"0001_t.up.sql:1:YD105"},
		},
		{
			name: "auto partitioning turned on for a table created with four uniform partitions",
			files: map[string]string{
				"0001_t.up.sql": "CREATE TABLE t (id Uint64 NOT NULL, PRIMARY KEY (id)) WITH (UNIFORM_PARTITIONS = 4);\n",
				"0002_t.up.sql": "ALTER TABLE t SET (AUTO_PARTITIONING_BY_LOAD = ENABLED);\n",
			},
			want: []string{"0002_t.up.sql:1:YD105"},
		},
		{
			name: "auto partitioning turned on after an earlier migration raised the minimum",
			files: map[string]string{
				"0001_t.up.sql": "CREATE TABLE t (id Uint64 NOT NULL, PRIMARY KEY (id));\n" +
					"ALTER TABLE t SET (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 3);\n",
				"0002_t.up.sql": "ALTER TABLE t SET (AUTO_PARTITIONING_BY_SIZE = ENABLED);\n",
			},
			want: []string{"0002_t.up.sql:1:YD105"},
		},
		{
			// One split point makes two partitions, and a minimum of 2.
			name: "auto partitioning turned on for a table split at one key",
			files: map[string]string{
				"0001_t.up.sql": "CREATE TABLE t (id Uint64 NOT NULL, PRIMARY KEY (id)) WITH (PARTITION_AT_KEYS = (10));\n",
				"0002_t.up.sql": "ALTER TABLE t SET (AUTO_PARTITIONING_BY_SIZE = ENABLED);\n",
			},
			want: []string{"0002_t.up.sql:1:YD105"},
		},
		{
			name: "auto partitioning turned on for a table split at two keys",
			files: map[string]string{
				"0001_t.up.sql": "CREATE TABLE t (id Uint64 NOT NULL, PRIMARY KEY (id)) WITH (PARTITION_AT_KEYS = (10, 20));\n",
				"0002_t.up.sql": "ALTER TABLE t SET (AUTO_PARTITIONING_BY_SIZE = ENABLED);\n",
			},
			want: []string{"0002_t.up.sql:1:YD105"},
		},
		{
			name: "a table a view reads, dropped",
			files: map[string]string{
				"0001_v.up.sql":    "CREATE TABLE base (id Uint64 NOT NULL, PRIMARY KEY (id));\nCREATE VIEW v WITH (security_invoker = TRUE) AS SELECT id FROM base;\n",
				"0002_drop.up.sql": "DROP TABLE base;\n",
			},
			want: []string{"0002_drop.up.sql:1:YD106"},
		},
		{
			name: "a table a view reads, renamed",
			files: map[string]string{
				"0001_v.up.sql":      "CREATE TABLE base (id Uint64 NOT NULL, PRIMARY KEY (id));\nCREATE VIEW v WITH (security_invoker = TRUE) AS SELECT id FROM base;\n",
				"0002_rename.up.sql": "ALTER TABLE base RENAME TO moved;\n",
			},
			want: []string{"0002_rename.up.sql:1:YD106"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbSites(ydbLint(c, test.files, test.version)), qt.DeepEquals, test.want)
		})
	}
}

// Each row is the remedy for a row above, or a shape YDB runs.
func TestYDBRules_LeaveWhatTheServerRuns(t *testing.T) {
	tests := []struct {
		name    string
		version string
		files   map[string]string
	}{
		{name: "a unique index declared with its table", files: map[string]string{
			"0001_t.up.sql": "CREATE TABLE t (id Uint64 NOT NULL, v Utf8, PRIMARY KEY (id), INDEX t_v GLOBAL UNIQUE SYNC ON (v));\n"}},
		{name: "a plain index added to an existing table", files: map[string]string{
			"0001_t.up.sql": "ALTER TABLE t ADD INDEX t_v GLOBAL SYNC ON (v);\n"}},
		{name: "a write and a scheme statement apart, which run as two queries", files: map[string]string{
			"0001_t.up.sql": "UPSERT INTO t (id) VALUES (1);\nCREATE TABLE m (id Uint64 NOT NULL, PRIMARY KEY (id));\n"}},
		{name: "a block whose SELECT reads no table", files: map[string]string{
			"0001_t.up.sql": "DO BEGIN SELECT 1; CREATE TABLE m (id Uint64 NOT NULL, PRIMARY KEY (id)); END DO;\n"}},
		{name: "a NOT NULL column with a default on a line that backfills it", files: map[string]string{
			"0001_t.up.sql": "ALTER TABLE t ADD COLUMN a Int64 NOT NULL DEFAULT 0;\n"}},
		{name: "a nullable column on the oldest line", version: "25.1.4.7", files: map[string]string{
			"0001_t.up.sql": "ALTER TABLE t ADD COLUMN a Int64;\n"}},
		{name: "a column dropped after its index and the TTL", files: map[string]string{
			"0001_users.up.sql": usersTable,
			"0002_drop.up.sql": "ALTER TABLE `shop/users` DROP INDEX users_email;\nALTER TABLE `shop/users` DROP COLUMN email;\n" +
				"ALTER TABLE `shop/users` DROP COLUMN name;\nALTER TABLE `shop/users` RESET (TTL);\nALTER TABLE `shop/users` DROP COLUMN expires;\n"}},
		{name: "an index dropped in the same ALTER TABLE, before the column", files: map[string]string{
			"0001_users.up.sql": usersTable,
			"0002_drop.up.sql":  "ALTER TABLE `shop/users` DROP INDEX users_email, DROP COLUMN email;\n"}},
		{name: "an index renamed, then dropped by its new name", files: map[string]string{
			"0001_users.up.sql": usersTable,
			"0002_drop.up.sql": "ALTER TABLE `shop/users` RENAME INDEX users_email TO users_email_v2;\n" +
				"ALTER TABLE `shop/users` DROP INDEX users_email_v2;\nALTER TABLE `shop/users` DROP COLUMN email;\n"}},
		{name: "a column of a table the directory never created", files: map[string]string{
			"0001_t.up.sql": "ALTER TABLE elsewhere DROP COLUMN v;\n"}},
		{name: "a column of the same name in another table", files: map[string]string{
			"0001_users.up.sql": usersTable,
			"0002_drop.up.sql":  "ALTER TABLE users DROP COLUMN email;\n"}},
		{name: "auto partitioning turned on with the minimum in the same SET", files: map[string]string{
			"0001_t.up.sql": "ALTER TABLE t SET (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4, AUTO_PARTITIONING_BY_SIZE = ENABLED);\n"}},
		{name: "auto partitioning turned on with the minimum in another SET of the statement", files: map[string]string{
			"0001_t.up.sql": "ALTER TABLE t SET (AUTO_PARTITIONING_BY_LOAD = ENABLED), SET (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4);\n"}},
		{name: "auto partitioning turned on for a table created with the default minimum", files: map[string]string{
			"0001_t.up.sql": "CREATE TABLE t (id Uint64 NOT NULL, PRIMARY KEY (id));\n",
			"0002_t.up.sql": "ALTER TABLE t SET (AUTO_PARTITIONING_BY_SIZE = ENABLED);\n"}},
		{name: "auto partitioning turned on for a table whose explicit minimum beside its split points is 1", files: map[string]string{
			"0001_t.up.sql": "CREATE TABLE t (id Uint64 NOT NULL, PRIMARY KEY (id)) " +
				"WITH (PARTITION_AT_KEYS = (10, 20), AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1);\n",
			"0002_t.up.sql": "ALTER TABLE t SET (AUTO_PARTITIONING_BY_LOAD = ENABLED);\n"}},
		{name: "auto partitioning turned on after an earlier migration lowered the minimum to 1", files: map[string]string{
			"0001_t.up.sql": "CREATE TABLE t (id Uint64 NOT NULL, PRIMARY KEY (id)) WITH (UNIFORM_PARTITIONS = 4);\n" +
				"ALTER TABLE t SET (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1);\n",
			"0002_t.up.sql": "ALTER TABLE t SET (AUTO_PARTITIONING_BY_SIZE = ENABLED);\n"}},
		{name: "auto partitioning turned off", files: map[string]string{
			"0001_t.up.sql": "ALTER TABLE t SET (AUTO_PARTITIONING_BY_SIZE = DISABLED, AUTO_PARTITIONING_PARTITION_SIZE_MB = 100);\n"}},
		{name: "a view dropped before its table", files: map[string]string{
			"0001_v.up.sql":    "CREATE TABLE base (id Uint64 NOT NULL, PRIMARY KEY (id));\nCREATE VIEW v WITH (security_invoker = TRUE) AS SELECT id FROM base;\n",
			"0002_drop.up.sql": "DROP VIEW v;\nDROP TABLE base;\n"}},
		{name: "a table no view reads, dropped", files: map[string]string{
			"0001_v.up.sql":    "CREATE TABLE base (id Uint64 NOT NULL, PRIMARY KEY (id));\nCREATE VIEW v WITH (security_invoker = TRUE) AS SELECT 1 AS one;\n",
			"0002_drop.up.sql": "DROP TABLE base;\n"}},
		{name: "a table no view reads, renamed", files: map[string]string{
			"0001_v.up.sql":      "CREATE TABLE base (id Uint64 NOT NULL, PRIMARY KEY (id));\nCREATE VIEW v WITH (security_invoker = TRUE) AS SELECT 1 AS one;\n",
			"0002_rename.up.sql": "ALTER TABLE base RENAME TO moved;\n"}},
		{name: "a rebuild of a table a view reads, whose copy takes the name back", files: map[string]string{
			"0001_v.up.sql": "CREATE TABLE base (id Uint64 NOT NULL, n Int32, PRIMARY KEY (id));\n" +
				"CREATE VIEW v WITH (security_invoker = TRUE) AS SELECT id FROM base;\n",
			"0002_rebuild.up.sql": "CREATE TABLE __ptah_rebuild_base (id Uint64 NOT NULL, n Int64, PRIMARY KEY (id));\n" +
				"INSERT INTO __ptah_rebuild_base (id, n) SELECT id, CAST(n AS Int64) AS n FROM base;\n" +
				"ALTER TABLE base RENAME TO __ptah_replaced_base;\n" +
				"ALTER TABLE __ptah_rebuild_base RENAME TO base;\n" +
				"DROP TABLE __ptah_replaced_base;\n"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbSites(ydbLint(c, test.files, test.version)), qt.HasLen, 0)
		})
	}
}

// A cluster that turns EnableAddUniqueIndex on adds a unique index to an
// existing table, and a target that says so leaves YD101 quiet. The build can
// still meet a duplicate, which MF101 and MF102 report in YDB's words.
func TestYDBRules_ReadTheTargetCapabilities(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "a unique index added",
			sql:  "ALTER TABLE t ADD INDEX t_v GLOBAL UNIQUE SYNC ON (v);",
			want: "0001_t.up.sql:1:MF101 ADD INDEX t_v builds over the rows t already holds and fails on the first duplicate " +
				"it meets \\(YDB: Duplicate key found\\), leaving the migration half applied\\..*",
		},
		{
			name: "an index dropped and added back as unique",
			sql:  "ALTER TABLE t DROP INDEX t_v;\nALTER TABLE t ADD INDEX t_v GLOBAL UNIQUE ON (v);",
			want: "0001_t.up.sql:2:MF102 ADD INDEX t_v replaces the index t_v dropped earlier with a unique one over the rows t " +
				"already holds: the drop succeeds and the unique build then fails on the first duplicate it meets " +
				"\\(YDB: Duplicate key found\\).*",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, err := lint.ResolveTarget("ydb", "26.2.1.14")
			c.Assert(err, qt.IsNil)
			target.Capabilities = target.Capabilities.With(capability.UniqueIndexOnExistingTable, true)

			findings, err := lint.LintFS(fixture(map[string]string{"0001_t.up.sql": test.sql + "\n"}),
				lint.Options{Dialect: "ydb", Target: target})

			c.Assert(err, qt.IsNil)
			c.Assert(ydbSites(findingSites(findings)), qt.HasLen, 0)
			c.Assert(uniqueFindings(findings), qt.HasLen, 1)
			c.Assert(uniqueFindings(findings)[0], qt.Matches, test.want)
		})
	}
}

// uniqueFindings renders the MF101 and MF102 findings as site and message.
func uniqueFindings(findings []lint.Finding) []string {
	var out []string
	for _, finding := range findings {
		if finding.Rule == "MF101" || finding.Rule == "MF102" {
			out = append(out, fmt.Sprintf("%s:%d:%s %s", finding.File, finding.Line, finding.Rule, finding.Message))
		}
	}
	return out
}

// Where the target refuses the unique index, YD101 says why the statement
// fails, and the duplicate risk MF101 or MF102 would report is not reported
// beside it.
func TestYDBRules_ReplaceTheUniqueFindings(t *testing.T) {
	c := qt.New(t)

	sites := ydbLint(c, map[string]string{
		"0001_t.up.sql": "ALTER TABLE t ADD INDEX t_a GLOBAL UNIQUE SYNC ON (a);\n" +
			"ALTER TABLE t DROP INDEX t_v;\nALTER TABLE t ADD INDEX t_v GLOBAL UNIQUE ON (v);\n",
	}, "")

	c.Assert(sites, qt.DeepEquals, []string{"0001_t.up.sql:1:YD101", "0001_t.up.sql:3:YD101"})
}

// The YD rules read YQL, which only a YDB run lexes as YQL. A run naming no
// dialect runs every rule, and they say nothing there.
func TestYDBRules_StaySilentWithoutADialect(t *testing.T) {
	c := qt.New(t)

	findings, err := lint.LintFS(fixture(map[string]string{
		"0001_t.up.sql": "ALTER TABLE t ADD COLUMN a Int64 NOT NULL;\nALTER TABLE t ADD INDEX t_v GLOBAL UNIQUE SYNC ON (v);\n" +
			"ALTER TABLE t SET (AUTO_PARTITIONING_BY_SIZE = ENABLED);\n",
	}), lint.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(ydbSites(findingSites(findings)), qt.HasLen, 0)
	c.Assert(rulesOf(findings), qt.Contains, "DD101")
}

// Where a YD rule says why the statement fails, the generic rule that says it
// may fail is not reported beside it.
func TestYDBRules_ReplaceTheGenericFinding(t *testing.T) {
	c := qt.New(t)

	sites := ydbLint(c, map[string]string{
		"0001_t.up.sql": "ALTER TABLE t ADD COLUMN a Int64 NOT NULL;\n",
	}, "")

	c.Assert(sites, qt.DeepEquals, []string{"0001_t.up.sql:1:YD103"})
}

// The message names what the target lacks, and the remedy that target takes.
func TestYDBRules_SayWhatToDo(t *testing.T) {
	tests := []struct {
		name    string
		version string
		sql     string
		want    string
	}{
		{
			name: "unique index",
			sql:  "ALTER TABLE `shop/users` ADD INDEX users_score GLOBAL UNIQUE SYNC ON (score);",
			want: "ADD INDEX users_score adds a unique index to shop/users, a table that exists already, which needs target capability " +
				"unique_index_on_existing_table, unavailable on this ydb target; declare the unique index in the CREATE TABLE that creates the table",
		},
		{
			name: "NOT NULL where a default backfills",
			sql:  "ALTER TABLE t ADD COLUMN a Int64 NOT NULL;",
			want: "ADD COLUMN a is NOT NULL without a default, which YDB refuses even on an empty table " +
				"(Cannot add not null column without default value); give it a DEFAULT, which existing rows take",
		},
		{
			name:    "NOT NULL where no default is taken",
			version: "25.1.4.7",
			sql:     "ALTER TABLE t ADD COLUMN a Int64 NOT NULL;",
			want: "ADD COLUMN a is NOT NULL without a default, which YDB refuses even on an empty table " +
				"(Cannot add not null column without default value), and a default needs target capability add_column_with_default, " +
				"unavailable on this ydb target; add the column as nullable",
		},
		{
			name:    "a default where none is taken",
			version: "25.1.4.7",
			sql:     "ALTER TABLE t ADD COLUMN a Int64 NOT NULL DEFAULT 0;",
			want: "ADD COLUMN a gives the column a default, which needs target capability add_column_with_default, " +
				"unavailable on this ydb target; add the column without a default and write its values in a data statement",
		},
		{
			name: "partition minimum",
			sql:  "ALTER TABLE t SET (AUTO_PARTITIONING_BY_LOAD = ENABLED);",
			want: "setting AUTO_PARTITIONING_BY_LOAD = ENABLED resets AUTO_PARTITIONING_MIN_PARTITIONS_COUNT of t to 1, " +
				"so YDB may merge its partitions down to one; set AUTO_PARTITIONING_MIN_PARTITIONS_COUNT in the same ALTER TABLE to keep it",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, err := lint.ResolveTarget("ydb", test.version)
			c.Assert(err, qt.IsNil)

			findings, err := lint.LintFS(fixture(map[string]string{"0001_t.up.sql": test.sql + "\n"}),
				lint.Options{Dialect: "ydb", Target: target})

			c.Assert(err, qt.IsNil)
			c.Assert(findings, qt.HasLen, 1)
			c.Assert(findings[0].Message, qt.Equals, test.want)
		})
	}
}

// The messages that name objects the directory declared.
func TestYDBRules_NameWhatTheStatementBreaks(t *testing.T) {
	c := qt.New(t)

	findings, err := lint.LintFS(fixture(map[string]string{
		"0001_users.up.sql": usersTable +
			"CREATE VIEW `shop/active` WITH (security_invoker = TRUE) AS SELECT id FROM `shop/users`;\n" +
			"CREATE VIEW `shop/named` WITH (security_invoker = TRUE) AS SELECT id, name FROM `shop/users`;\n",
		"0002_drop.up.sql": "ALTER TABLE `shop/users` DROP COLUMN name;\nALTER TABLE `shop/users` DROP COLUMN expires;\n" +
			"DROP TABLE `shop/users`;\n",
	}), lint.Options{Dialect: "ydb"})

	c.Assert(err, qt.IsNil)
	messages := make([]string, 0, len(findings))
	for _, finding := range findings {
		messages = append(messages, finding.Rule+": "+finding.Message)
	}
	c.Assert(messages, qt.Contains, "YD104: DROP COLUMN name: index users_email covers the column, and YDB refuses to drop it "+
		"(Impossible drop column because table index covers that column); drop the index first")
	c.Assert(messages, qt.Contains, "YD104: DROP COLUMN expires: the table's TTL reads the column, and YDB refuses to drop it "+
		"(Can't drop TTL column, disable TTL first); run ALTER TABLE ... RESET (TTL) first")
	c.Assert(messages, qt.Contains, "YD106: DROP TABLE shop/users leaves views shop/active, shop/named reading a table that does not exist: "+
		"YDB keeps a view whose table is dropped, and every read of it fails; drop or recreate them first")
}

// A rename names the table it moves and the name to recreate the view over.
func TestYDBRules_NameTheRenameThatOrphansAView(t *testing.T) {
	c := qt.New(t)

	findings, err := lint.LintFS(fixture(map[string]string{
		"0001_users.up.sql": usersTable +
			"CREATE VIEW `shop/active` WITH (security_invoker = TRUE) AS SELECT id FROM `shop/users`;\n",
		"0002_rename.up.sql": "ALTER TABLE `shop/users` RENAME TO `shop/members`;\n",
	}), lint.Options{Dialect: "ydb"})

	c.Assert(err, qt.IsNil)
	messages := make([]string, 0, len(findings))
	for _, finding := range findings {
		messages = append(messages, finding.Rule+": "+finding.Message)
	}
	c.Assert(messages, qt.Contains, "YD106: ALTER TABLE shop/users RENAME TO shop/members leaves view shop/active reading a "+
		"table that does not exist: a view reads its table by path, so every read of it fails until a table takes the "+
		"name shop/users again; recreate it over shop/members")
}

// statementTexts returns the statements the linter cut a one-file YDB
// directory into, as written.
func statementTexts(c *qt.C, sql string) []string {
	c.Helper()
	analysis, err := lint.AnalyzeFS(fixture(map[string]string{"0001_t.up.sql": sql}), lint.Options{Dialect: "ydb"})
	c.Assert(err, qt.IsNil)
	var texts []string
	for _, file := range analysis.Files() {
		for _, statement := range file.Statements {
			texts = append(texts, statement.SQL)
		}
	}
	return texts
}

// migratorTexts returns the statements core/sqlutil cuts the same text into,
// which is the split the migrator's queries are built from, without their
// terminators.
func migratorTexts(sql string) []string {
	var texts []string
	for _, statement := range sqlutil.SplitSourceStatements(sql, "ydb") {
		texts = append(texts, strings.TrimSpace(strings.TrimSuffix(statement.Text, ";")))
	}
	return texts
}

// The linter cuts a YDB migration at the semicolons the migrator cuts it at,
// so a rule judges each statement the migrator runs, and nothing it does not.
func TestYDBStatements_SplitWhereTheMigratorDoes(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want int
	}{
		{name: "a lambda body between braces", want: 2,
			sql: "$f = ($x) -> { $y = $x + 1; RETURN $y; };\nSELECT $f(1);\n"},
		{name: "an action body", want: 2,
			sql: "DEFINE ACTION $a() AS\n  UPSERT INTO t (id) VALUES (1);\n  DROP TABLE old;\nEND DEFINE;\nDO $a();\n"},
		{name: "a loop body", want: 1,
			sql: "EVALUATE FOR $i IN AsList(1, 2) DO BEGIN\n  UPSERT INTO t (id) VALUES ($i);\nEND DO;\n"},
		{name: "semicolons inside YQL strings", want: 2,
			sql: "UPSERT INTO t (a, b, c) VALUES (\"x\\\"; y\"u, 'p\\'; q'u, @@m; n@@);\nDROP TABLE `odd;name`;\n"},
		{name: "a backticked name that spells a keyword inside a block", want: 2,
			sql: "DO BEGIN SELECT 1 AS `end`; SELECT 2 AS `do`; END DO;\nDROP TABLE t;\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := statementTexts(c, test.sql)
			c.Assert(got, qt.HasLen, test.want)
			c.Assert(got, qt.DeepEquals, migratorTexts(test.sql))
		})
	}
}
