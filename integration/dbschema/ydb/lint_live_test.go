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
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
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

// YDB runs a DROP TABLE a view reads and keeps the view, which then fails on
// every read. YD106 reports the drop, and the server shows what it reports.
func TestYDBLint_DroppedTableLeavesItsViewFailing(t *testing.T) {
	dir := lintDir + "/view"
	setup := []string{
		"CREATE TABLE `" + dir + "/base` (id Uint64 NOT NULL, PRIMARY KEY (id))",
		"CREATE VIEW `" + dir + "/v` WITH (security_invoker = TRUE) AS SELECT id FROM `" + dir + "/base`",
	}
	statement := "DROP TABLE `" + dir + "/base`"
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			c.Cleanup(func() { dropLintDir(c, conn) })
			for _, step := range setup {
				c.Assert(conn.Writer().ExecuteSQL(c.Context(), step), qt.IsNil, qt.Commentf("setup: %s", step))
			}

			reported := lintAgainst(c, conn, setup, statement)
			dropErr := conn.Writer().ExecuteSQL(c.Context(), statement)
			var count int64
			readErr := conn.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM `"+dir+"/v`").Scan(&count)

			c.Assert(reported, qt.Contains, "YD106")
			c.Assert(dropErr, qt.IsNil)
			c.Assert(readErr, qt.ErrorMatches, `(?s).*Cannot find table 'db\.\[/local/`+regexp.QuoteMeta(dir)+`/base\]'.*`)
		})
	}
}

// YD105 reports the ALTER TABLE that resets a table's minimum partition count,
// and the count the server keeps afterwards, read back through the scheme
// service, is what the rule says it is.
func TestYDBLint_PartitioningChangeResetsTheMinimum(t *testing.T) {
	tests := []struct {
		name     string
		set      string
		reported bool
		minimum  uint64
	}{
		{name: "auto partitioning by size turned on", set: "SET (AUTO_PARTITIONING_BY_SIZE = ENABLED)", reported: true, minimum: 1},
		{name: "auto partitioning by load turned on, without parentheses", set: "SET AUTO_PARTITIONING_BY_LOAD ENABLED", reported: true, minimum: 1},
		{name: "turned on with the minimum in the same statement",
			set: "SET (AUTO_PARTITIONING_BY_LOAD = ENABLED), SET (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4)", minimum: 4},
		{name: "a size setting", set: "SET (AUTO_PARTITIONING_PARTITION_SIZE_MB = 100)", minimum: 4},
	}
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			conn := openYDB(qt.New(t), line)
			for i, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					c := qt.New(t)
					c.Cleanup(func() { dropLintDir(c, conn) })
					name := fmt.Sprintf("%s/partitions_%d", lintDir, i)
					create := "CREATE TABLE `" + name + "` (id Uint64 NOT NULL, PRIMARY KEY (id)) WITH (" +
						"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4, AUTO_PARTITIONING_BY_SIZE = DISABLED, AUTO_PARTITIONING_BY_LOAD = DISABLED)"
					c.Assert(conn.Writer().ExecuteSQL(c.Context(), create), qt.IsNil)
					statement := "ALTER TABLE `" + name + "` " + test.set

					reported := lintAgainst(c, conn, []string{create}, statement)
					c.Assert(conn.Writer().ExecuteSQL(c.Context(), statement), qt.IsNil)

					c.Assert(slices.Contains(reported, "YD105"), qt.Equals, test.reported)
					c.Assert(minPartitions(c, line, name), qt.Equals, test.minimum)
				})
			}
		})
	}
}

// minPartitions reads a table's minimum partition count from the scheme
// service, which Ptah's reader does not read yet.
func minPartitions(c *qt.C, line ydbLine, name string) uint64 {
	c.Helper()
	ctx := c.Context()
	driver, err := ydbsdk.Open(ctx, dbtarget.DriverDSN(c, line.engine))
	c.Assert(err, qt.IsNil)
	defer func() { _ = driver.Close(context.Background()) }()
	var minimum uint64
	err = driver.Table().Do(ctx, func(ctx context.Context, session table.Session) error {
		description, describeErr := session.DescribeTable(ctx, path.Join(driver.Name(), name))
		minimum = description.PartitioningSettings.MinPartitionsCount
		return describeErr
	})
	c.Assert(err, qt.IsNil)
	return minimum
}
