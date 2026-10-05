//go:build integration

package ydb_test

import (
	"context"
	"fmt"
	"path"
	"regexp"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/dbschema"
	"ptah.run/internal/ydbpartition"
	"ptah.run/migration/lint"
)

// lintDir is the directory the lint tests create their tables in.
const lintDir = "ptah_ydb_lint"

// dropLintDir removes what the lint tests created.
func dropLintDir(c *qt.C, conn *dbschema.DatabaseConnection) {
	c.Helper()
	dropper, ok := conn.SchemaWriter().(interface {
		DropDirectory(ctx context.Context, dir string) error
	})
	c.Assert(ok, qt.IsTrue)
	c.Assert(dropper.DropDirectory(context.Background(), lintDir), qt.IsNil)
}

// lintAgainst lints a two-version directory, the setup and then the statement
// under test, against what the connected server established about itself,
// and returns the rules reported on the second version's first statement.
func lintAgainst(c *qt.C, conn *dbschema.DatabaseConnection, setup []string, statement string) []string {
	c.Helper()
	info := conn.Info()
	files := fstest.MapFS{
		"0000000001_setup.up.sql":  {Data: []byte(strings.Join(setup, ";\n") + ";\n")},
		"0000000002_change.up.sql": {Data: []byte(statement + ";\n")},
	}
	findings, err := lint.LintFS(files, lint.Options{
		Dialect: info.Dialect,
		Target:  lint.TargetFromServer(info.Dialect, info.Version, info.Capabilities, ""),
	})
	c.Assert(err, qt.IsNil)
	var rules []string
	for _, finding := range findings {
		if finding.File == "0000000002_change.up.sql" && finding.Line == 1 {
			rules = append(rules, finding.Rule)
		}
	}
	return rules
}

// errorText is an error's text, or the empty string for none.
func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// Each statement here a YD rule judges, run on the server it judges it for:
// the rule reports exactly the statements the server refuses, and the refusal
// is the one the rule's message quotes. Where the two lines differ, the row
// says what each answers, and an empty answer is a statement the line runs.
func TestYDBLint_RulesReportWhatTheServerRefuses(t *testing.T) {
	tests := []struct {
		name      string
		setup     []string
		statement string
		rule      string
		refusals  map[string]string
	}{
		{
			name:      "a unique index added to an existing table",
			setup:     []string{"CREATE TABLE `{dir}/t` (id Uint64 NOT NULL, v Utf8, PRIMARY KEY (id))"},
			statement: "ALTER TABLE `{dir}/t` ADD INDEX t_v GLOBAL UNIQUE SYNC ON (v)",
			rule:      "YD101",
			refusals: map[string]string{
				"26.2": "Adding a unique index to an existing table is disabled",
				"25.1": "Unknown index type: syncGlobalUnique",
			},
		},
		{
			name:      "a unique index declared with its table",
			statement: "CREATE TABLE `{dir}/t` (id Uint64 NOT NULL, v Utf8, PRIMARY KEY (id), INDEX t_v GLOBAL UNIQUE SYNC ON (v))",
			rule:      "YD101",
			refusals:  map[string]string{"26.2": "", "25.1": ""},
		},
		{
			name:  "a block that writes a row and creates a table",
			setup: []string{"CREATE TABLE `{dir}/t` (id Uint64 NOT NULL, PRIMARY KEY (id))"},
			statement: "DO BEGIN UPSERT INTO `{dir}/t` (id) VALUES (1ul); " +
				"CREATE TABLE `{dir}/m` (id Uint64 NOT NULL, PRIMARY KEY (id)); END DO",
			rule: "YD102",
			refusals: map[string]string{
				"26.2": "Queries with mixed data and scheme operations are not supported",
				"25.1": "Queries with mixed data and scheme operations are not supported",
			},
		},
		{
			name:      "a block whose SELECT reads no table",
			statement: "DO BEGIN SELECT 1; CREATE TABLE `{dir}/m` (id Uint64 NOT NULL, PRIMARY KEY (id)); END DO",
			rule:      "YD102",
			refusals:  map[string]string{"26.2": "", "25.1": ""},
		},
		{
			name:      "a NOT NULL column without a default",
			setup:     []string{"CREATE TABLE `{dir}/t` (id Uint64 NOT NULL, PRIMARY KEY (id))"},
			statement: "ALTER TABLE `{dir}/t` ADD COLUMN a Int64 NOT NULL",
			rule:      "YD103",
			refusals: map[string]string{
				"26.2": "Cannot add not null column without default value",
				"25.1": "Cannot add not null column without default value",
			},
		},
		{
			name:      "a column with a default",
			setup:     []string{"CREATE TABLE `{dir}/t` (id Uint64 NOT NULL, PRIMARY KEY (id))"},
			statement: "ALTER TABLE `{dir}/t` ADD COLUMN a Int64 NOT NULL DEFAULT 7",
			rule:      "YD103",
			refusals: map[string]string{
				"26.2": "",
				"25.1": "Column addition with default value is not supported now",
			},
		},
		{
			name:      "a nullable column",
			setup:     []string{"CREATE TABLE `{dir}/t` (id Uint64 NOT NULL, PRIMARY KEY (id))"},
			statement: "ALTER TABLE `{dir}/t` ADD COLUMN a Int64",
			rule:      "YD103",
			refusals:  map[string]string{"26.2": "", "25.1": ""},
		},
		{
			name:      "the key column of an index, dropped",
			setup:     []string{usedColumnsTable},
			statement: "ALTER TABLE `{dir}/t` DROP COLUMN k",
			rule:      "YD104",
			refusals: map[string]string{
				"26.2": "Impossible drop column because table has an index with that column",
				"25.1": "Impossible drop column because table has an index with that column",
			},
		},
		{
			name:      "a covered column, dropped",
			setup:     []string{usedColumnsTable},
			statement: "ALTER TABLE `{dir}/t` DROP COLUMN c",
			rule:      "YD104",
			refusals: map[string]string{
				"26.2": "Impossible drop column because table index covers that column",
				"25.1": "Impossible drop column because table index covers that column",
			},
		},
		{
			name:      "the TTL column, dropped",
			setup:     []string{usedColumnsTable},
			statement: "ALTER TABLE `{dir}/t` DROP COLUMN ts",
			rule:      "YD104",
			refusals: map[string]string{
				"26.2": "Can't drop TTL column: 'ts', disable TTL first",
				"25.1": "Can't drop TTL column: 'ts', disable TTL first",
			},
		},
		{
			name:      "the TTL column, dropped after RESET (TTL)",
			setup:     []string{usedColumnsTable, "ALTER TABLE `{dir}/t` RESET (TTL)"},
			statement: "ALTER TABLE `{dir}/t` DROP COLUMN ts",
			rule:      "YD104",
			refusals:  map[string]string{"26.2": "", "25.1": ""},
		},
		{
			name:      "a column no index or TTL uses, dropped",
			setup:     []string{usedColumnsTable},
			statement: "ALTER TABLE `{dir}/t` DROP COLUMN free",
			rule:      "YD104",
			refusals:  map[string]string{"26.2": "", "25.1": ""},
		},
		{
			name: "a table carrying a changefeed, renamed",
			setup: []string{"CREATE TABLE `{dir}/t` (id Uint64 NOT NULL, PRIMARY KEY (id))",
				"ALTER TABLE `{dir}/t` ADD CHANGEFEED feed WITH (MODE = 'UPDATES', FORMAT = 'JSON')"},
			statement: "ALTER TABLE `{dir}/t` RENAME TO `{dir}/u`",
			rule:      "YD109",
			refusals: map[string]string{
				"26.2": "Cannot move table with cdc streams",
				"25.1": "Cannot move table with cdc streams",
			},
		},
		{
			name: "a table renamed after its changefeed was dropped",
			setup: []string{"CREATE TABLE `{dir}/t` (id Uint64 NOT NULL, PRIMARY KEY (id))",
				"ALTER TABLE `{dir}/t` ADD CHANGEFEED feed WITH (MODE = 'UPDATES', FORMAT = 'JSON')",
				"ALTER TABLE `{dir}/t` DROP CHANGEFEED feed"},
			statement: "ALTER TABLE `{dir}/t` RENAME TO `{dir}/u`",
			rule:      "YD109",
			refusals:  map[string]string{"26.2": "", "25.1": ""},
		},
	}
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			conn := openYDB(qt.New(t), line)
			for i, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					c := qt.New(t)
					dir := fmt.Sprintf("%s/rule_%d", lintDir, i)
					c.Cleanup(func() { dropLintDir(c, conn) })
					setup := make([]string, 0, len(test.setup))
					for _, statement := range test.setup {
						setup = append(setup, strings.ReplaceAll(statement, "{dir}", dir))
					}
					statement := strings.ReplaceAll(test.statement, "{dir}", dir)
					for _, step := range setup {
						c.Assert(conn.Writer().ExecuteSQL(c.Context(), step), qt.IsNil, qt.Commentf("setup: %s", step))
					}
					refusal, measured := test.refusals[line.name]
					c.Assert(measured, qt.IsTrue, qt.Commentf("no answer recorded for YDB %s", line.name))

					reported := lintAgainst(c, conn, setup, statement)
					err := conn.Writer().ExecuteSQL(c.Context(), statement)

					c.Assert(errorText(err) != "", qt.Equals, refusal != "", qt.Commentf("YDB %s answered %v", line.name, err))
					c.Assert(errorText(err), qt.Matches, `(?s).*`+regexp.QuoteMeta(refusal)+`.*`)
					c.Assert(slices.Contains(reported, test.rule), qt.Equals, refusal != "",
						qt.Commentf("lint reported %v for a statement YDB %s answered with %v", reported, line.name, err))
				})
			}
		})
	}
}

// usedColumnsTable has a column an index keys, one it covers, one the TTL
// reads, and one nothing uses.
const usedColumnsTable = "CREATE TABLE `{dir}/t` (id Uint64 NOT NULL, k Utf8, c Utf8, ts Timestamp, free Utf8, " +
	"PRIMARY KEY (id), INDEX t_k GLOBAL SYNC ON (k) COVER (c)) WITH (TTL = Interval(\"P1D\") ON ts)"

// YDB runs a DROP TABLE a view reads, and an ALTER TABLE ... RENAME TO of it,
// and keeps the view, which reads its table by path and then fails on every
// read. YD106 reports both, and the server shows what it reports.
func TestYDBLint_DroppedOrRenamedTableLeavesItsViewFailing(t *testing.T) {
	dir := lintDir + "/view"
	setup := []string{
		"CREATE TABLE `" + dir + "/base` (id Uint64 NOT NULL, PRIMARY KEY (id))",
		"CREATE VIEW `" + dir + "/v` WITH (security_invoker = TRUE) AS SELECT id FROM `" + dir + "/base`",
	}
	tests := []struct {
		name      string
		statement string
	}{
		{name: "dropped", statement: "DROP TABLE `" + dir + "/base`"},
		{name: "renamed", statement: "ALTER TABLE `" + dir + "/base` RENAME TO `" + dir + "/moved`"},
	}
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					c := qt.New(t)
					conn := openYDB(c, line)
					c.Cleanup(func() { dropLintDir(c, conn) })
					for _, step := range setup {
						c.Assert(conn.Writer().ExecuteSQL(c.Context(), step), qt.IsNil, qt.Commentf("setup: %s", step))
					}

					reported := lintAgainst(c, conn, setup, test.statement)
					runErr := conn.Writer().ExecuteSQL(c.Context(), test.statement)
					var count int64
					readErr := conn.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM `"+dir+"/v`").Scan(&count)

					c.Assert(reported, qt.Contains, "YD106")
					c.Assert(runErr, qt.IsNil)
					c.Assert(readErr, qt.ErrorMatches, `(?s).*Cannot find table 'db\.\[/local/`+regexp.QuoteMeta(dir)+`/base\]'.*`)
				})
			}
		})
	}
}

// YD105 reports the ALTER TABLE that resets a table's minimum partition count,
// and the count the server keeps afterwards, read back through the scheme
// service, is what the rule says it is. A table whose minimum the directory
// left at 1 loses nothing, and the rule stays silent there.
func TestYDBLint_PartitioningChangeResetsTheMinimum(t *testing.T) {
	const minimumOfFour = "AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4, AUTO_PARTITIONING_BY_SIZE = DISABLED, " +
		"AUTO_PARTITIONING_BY_LOAD = DISABLED"
	tests := []struct {
		name     string
		with     string
		set      string
		reported bool
		minimum  uint64
	}{
		{name: "auto partitioning by size turned on", with: minimumOfFour,
			set: "SET (AUTO_PARTITIONING_BY_SIZE = ENABLED)", reported: true, minimum: 1},
		{name: "auto partitioning by load turned on, without parentheses", with: minimumOfFour,
			set: "SET AUTO_PARTITIONING_BY_LOAD ENABLED", reported: true, minimum: 1},
		{name: "turned on with the minimum in the same statement", with: minimumOfFour,
			set: "SET (AUTO_PARTITIONING_BY_LOAD = ENABLED), SET (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4)", minimum: 4},
		{name: "a size setting", with: minimumOfFour, set: "SET (AUTO_PARTITIONING_PARTITION_SIZE_MB = 100)", minimum: 4},
		{name: "turned on for a table created with four uniform partitions",
			with: "UNIFORM_PARTITIONS = 4, AUTO_PARTITIONING_BY_SIZE = DISABLED",
			set:  "SET (AUTO_PARTITIONING_BY_LOAD = ENABLED)", reported: true, minimum: 1},
		{name: "turned on for a table whose minimum is already 1",
			with: "AUTO_PARTITIONING_BY_SIZE = DISABLED, AUTO_PARTITIONING_BY_LOAD = DISABLED",
			set:  "SET (AUTO_PARTITIONING_BY_SIZE = ENABLED)", minimum: 1},
	}
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			conn := openYDB(qt.New(t), line)
			for i, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					c := qt.New(t)
					c.Cleanup(func() { dropLintDir(c, conn) })
					name := fmt.Sprintf("%s/partitions_%d", lintDir, i)
					create := "CREATE TABLE `" + name + "` (id Uint64 NOT NULL, PRIMARY KEY (id)) WITH (" + test.with + ")"
					c.Assert(conn.Writer().ExecuteSQL(c.Context(), create), qt.IsNil)
					statement := "ALTER TABLE `" + name + "` " + test.set

					reported := lintAgainst(c, conn, []string{create}, statement)
					c.Assert(conn.Writer().ExecuteSQL(c.Context(), statement), qt.IsNil)

					c.Assert(slices.Contains(reported, "YD105"), qt.Equals, test.reported)
					c.Assert(partitionSettings(c, conn, name).MinPartitions, qt.Equals, test.minimum)
				})
			}
		})
	}
}

// YD118 reports the ALTER TABLE that resets a table's partition size, and the
// size the server keeps afterwards, read back through Ptah's reader, is what the
// rule says it is.
func TestYDBLint_PartitioningChangeResetsTheSize(t *testing.T) {
	tests := []struct {
		name     string
		set      string
		reported bool
		size     uint64
	}{
		{name: "auto partitioning by size turned on again", set: "SET (AUTO_PARTITIONING_BY_SIZE = ENABLED)", reported: true, size: 2048},
		{name: "turned on beside the minimum",
			set: "SET (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 6, AUTO_PARTITIONING_BY_SIZE = ENABLED)", reported: true, size: 2048},
		{name: "turned on with the size in the same statement",
			set: "SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_PARTITION_SIZE_MB = 100)", size: 100},
		{name: "auto partitioning by load turned on", set: "SET (AUTO_PARTITIONING_BY_LOAD = ENABLED)", size: 100},
	}
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			conn := openYDB(qt.New(t), line)
			for i, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					c := qt.New(t)
					c.Cleanup(func() { dropLintDir(c, conn) })
					name := fmt.Sprintf("%s/sizes_%d", lintDir, i)
					create := "CREATE TABLE `" + name + "` (id Uint64 NOT NULL, PRIMARY KEY (id)) WITH (" +
						"AUTO_PARTITIONING_PARTITION_SIZE_MB = 100)"
					c.Assert(conn.Writer().ExecuteSQL(c.Context(), create), qt.IsNil)
					statement := "ALTER TABLE `" + name + "` " + test.set

					reported := lintAgainst(c, conn, []string{create}, statement)
					c.Assert(conn.Writer().ExecuteSQL(c.Context(), statement), qt.IsNil)

					c.Assert(slices.Contains(reported, "YD118"), qt.Equals, test.reported)
					c.Assert(partitionSettings(c, conn, name).PartitionSizeMB, qt.Equals, test.size)
				})
			}
		})
	}
}

// partitionSettings reads the settings a table in the lint directory holds
// through Ptah's reader, resolved: a setting at its default reads as the
// default, not as absent.
func partitionSettings(c *qt.C, conn *dbschema.DatabaseConnection, name string) ydbpartition.TableSettings {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{lintDir})
	c.Assert(err, qt.IsNil)
	described := tableNamed(c, live, lintDir, path.Base(name))
	settings, err := ydbpartition.HeldTable(described.YDBPartitioning)
	c.Assert(err, qt.IsNil)
	return settings
}

// YDB does not refuse an ALTER TABLE that names a column family the table does
// not have: it creates the family with its own settings. YD119 reports each
// such statement, and the server shows what it reports -- the family exists
// after the statement. A statement naming a family the table has is the
// control.
func TestYDBLint_UndeclaredColumnFamilyIsCreated(t *testing.T) {
	dir := lintDir + "/family"
	setup := []string{"CREATE TABLE `" + dir + "/t` (id Uint64 NOT NULL, a Utf8 FAMILY cold, PRIMARY KEY (id), " +
		"FAMILY cold (COMPRESSION = 'lz4'))"}
	tests := []struct {
		name      string
		statement string
		family    string
		reported  bool
	}{
		{name: "a column moved", statement: "ALTER TABLE `" + dir + "/t` ALTER COLUMN a SET FAMILY clod", family: "clod", reported: true},
		{name: "a family altered", statement: "ALTER TABLE `" + dir + "/t` ALTER FAMILY warm SET COMPRESSION 'lz4'", family: "warm", reported: true},
		{name: "a column added", statement: "ALTER TABLE `" + dir + "/t` ADD COLUMN b Int32 FAMILY hot", family: "hot", reported: true},
		{name: "a family the table has", statement: "ALTER TABLE `" + dir + "/t` ALTER FAMILY cold SET COMPRESSION 'off'", family: "cold"},
	}
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					c := qt.New(t)
					conn := openYDB(c, line)
					c.Cleanup(func() { dropLintDir(c, conn) })
					for _, step := range setup {
						c.Assert(conn.Writer().ExecuteSQL(c.Context(), step), qt.IsNil, qt.Commentf("setup: %s", step))
					}

					reported := lintAgainst(c, conn, setup, test.statement)
					runErr := conn.Writer().ExecuteSQL(c.Context(), test.statement)
					live, readErr := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{dir})

					c.Assert(slices.Contains(reported, "YD119"), qt.Equals, test.reported)
					c.Assert(runErr, qt.IsNil)
					c.Assert(readErr, qt.IsNil)
					c.Assert(live.Tables, qt.HasLen, 1)
					c.Assert(slices.ContainsFunc(live.Tables[0].YDBColumnFamilies, func(family ast.YDBColumnFamilySpec) bool {
						return family.Name == test.family
					}), qt.IsTrue, qt.Commentf("families: %+v", live.Tables[0].YDBColumnFamilies))
				})
			}
		})
	}
}
