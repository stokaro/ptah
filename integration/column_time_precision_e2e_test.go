//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
)

// TestCompatMigrateDiffTimePrecisionE2E_ThePrecisionChangesBothWays applies the file of
// a precision change and reads the type back, then applies its rollback and reads the
// type before it. A comparison that reads no precision writes no file for the change.
func TestCompatMigrateDiffTimePrecisionE2E_ThePrecisionChangesBothWays(t *testing.T) {
	tests := []struct {
		name       string
		engine     columnTypeEngine
		before     string
		after      string
		wantBefore string
		wantAfter  string
	}{
		{
			name: "a timestamp", engine: columnTypePostgres, before: "timestamp(3)", after: "timestamp(5)",
			wantBefore: "timestamp(3) without time zone", wantAfter: "timestamp(5) without time zone",
		},
		{
			name: "a timestamp to the default", engine: columnTypePostgres, before: "timestamp(3)", after: "timestamp",
			wantBefore: "timestamp(3) without time zone", wantAfter: "timestamp without time zone",
		},
		{
			name: "a zoned timestamp", engine: columnTypePostgres, before: "timestamptz", after: "timestamptz(0)",
			wantBefore: "timestamp with time zone", wantAfter: "timestamp(0) with time zone",
		},
		{
			name: "a time", engine: columnTypePostgres, before: "time(3)", after: "time(5)",
			wantBefore: "time(3) without time zone", wantAfter: "time(5) without time zone",
		},
		{
			name: "a zoned time", engine: columnTypePostgres, before: "timetz(3)", after: "timetz",
			wantBefore: "time(3) with time zone", wantAfter: "time with time zone",
		},
		{
			name: "an interval", engine: columnTypePostgres, before: "interval", after: "interval(3)",
			wantBefore: "interval", wantAfter: "interval(3)",
		},
		{
			name: "an array", engine: columnTypePostgres, before: "timestamp(3)[]", after: "timestamp(5)[]",
			wantBefore: "timestamp(3) without time zone[]", wantAfter: "timestamp(5) without time zone[]",
		},
		{
			name: "a timestamp on MySQL", engine: columnTypeMySQL, before: "timestamp(3) NULL", after: "timestamp(6) NULL",
			wantBefore: "timestamp(3)", wantAfter: "timestamp(6)",
		},
		{
			name: "a time on MySQL", engine: columnTypeMySQL, before: "time", after: "time(3)",
			wantBefore: "time", wantAfter: "time(3)",
		},
		{
			name: "a timestamp on MariaDB", engine: columnTypeMariaDB, before: "timestamp(3) NULL", after: "timestamp(6) NULL",
			wantBefore: "timestamp(3)", wantAfter: "timestamp(6)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			pairs := readMigrationPairs(c, diffEngineSteps(c, "", test.engine.target(c), []string{
				columnTypeTable(test.before), columnTypeTable(test.after),
			}))
			target := test.engine.target(c)

			c.Assert(pairs, qt.HasLen, 2)
			applyEngineFile(c, target, test.engine.dialect, pairs[0].Up)
			c.Assert(heldColumns(c, target, test.engine.typeOf), qt.Equals, test.wantBefore)
			applyEngineFile(c, target, test.engine.dialect, pairs[1].Up)
			c.Assert(heldColumns(c, target, test.engine.typeOf), qt.Equals, test.wantAfter)
			applyEngineFile(c, target, test.engine.dialect, pairs[1].Down)
			c.Assert(heldColumns(c, target, test.engine.typeOf), qt.Equals, test.wantBefore)
		})
	}
}

// TestSchemaCompareE2E_APrecisionTheColumnHoldsIsNoChange builds one column of each
// spelling and compares the table with the statement that built it. `timestamp` and
// `timestamp(6)` are one type on PostgreSQL and two on MySQL, so the rows differ.
func TestSchemaCompareE2E_APrecisionTheColumnHoldsIsNoChange(t *testing.T) {
	tests := []struct {
		name   string
		engine columnTypeEngine
		types  []string
	}{
		{
			name: "postgres", engine: columnTypePostgres,
			types: []string{
				"timestamp", "timestamp(6)", "timestamp(3)", "timestamp(0)", "timestamptz", "timestamptz(6)", "timestamptz(3)",
				"timestamp(3) with time zone", "timestamp(3) without time zone", "time", "time(6)", "time(3)", "time(0)", "timetz",
				"timetz(3)", "time(3) with time zone", "interval", "interval(6)", "interval(3)", "timestamp(3)[]", "timestamptz[]",
			},
		},
		{
			name:   "cockroachdb",
			engine: columnTypeEngine{dialect: platform.CockroachDB, target: cockroachColumnTypeDatabase},
			types:  []string{"timestamp", "timestamp(6)", "timestamp(3)", "timestamptz", "timestamptz(3)", "time", "time(3)", "interval"},
		},
		{
			name: "mysql", engine: columnTypeMySQL,
			types: []string{"timestamp NULL", "timestamp(0) NULL", "timestamp(3) NULL", "datetime", "datetime(3)", "time", "time(3)"},
		},
		{
			name: "mariadb", engine: columnTypeMariaDB,
			types: []string{"timestamp NULL", "timestamp(0) NULL", "timestamp(3) NULL", "datetime", "datetime(3)", "time", "time(3)"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := test.engine.target(c)
			table := "CREATE TABLE t (id int PRIMARY KEY"
			for i, columnType := range test.types {
				table += fmt.Sprintf(", c%d %s", i, columnType)
			}
			table += ");\n"
			applyEngineFile(c, target, test.engine.dialect, table)
			schema := filepath.Join(t.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(table), 0o600), qt.IsNil)

			output := runPtahNative(c, "schema", "compare", "--db-url", target, "--schema-file", schema)

			c.Assert(output, qt.Contains, "No schema differences detected.", qt.Commentf("%s", output))
		})
	}
}
