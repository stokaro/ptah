package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/migration/safety"
)

const replaceTableReason = "CREATE OR REPLACE TABLE drops the existing table and all its rows"

// A statement that replaces a table drops it and every row it holds. Measured
// on MariaDB 11.8.9 and 12.3.3 and on ClickHouse 26.9.3: a table holding two
// rows holds none afterwards. Without the rule, each reads as a CREATE TABLE and
// is reported Safe, so an edited plan carrying one keeps destructive=false.
func TestAssessSQL_ReplacingATableIsDestructive(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{name: "MariaDB", statement: "CREATE OR REPLACE TABLE `victim` (`id` INT PRIMARY KEY)"},
		{name: "MariaDB in lower case", statement: "create or replace table victim (id int)"},
		{name: "MariaDB across lines", statement: "CREATE\nOR REPLACE\tTABLE victim (id INT);"},
		{name: "MariaDB qualified", statement: "CREATE OR REPLACE TABLE `app`.`victim` (id INT)"},
		{name: "MariaDB from a query", statement: "CREATE OR REPLACE TABLE victim AS SELECT id FROM source"},
		{name: "MariaDB like another table", statement: "CREATE OR REPLACE TABLE victim LIKE template"},
		{
			name:      "ClickHouse",
			statement: "CREATE OR REPLACE TABLE app.victim (id UInt32) ENGINE = MergeTree ORDER BY id",
		},
		{name: "ClickHouse REPLACE TABLE", statement: "REPLACE TABLE victim (id UInt32) ENGINE = MergeTree ORDER BY id"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := safety.AssessSQL(test.statement)

			c.Assert(got.Severity, qt.Equals, safety.Destructive)
			c.Assert(got.Reason, qt.Equals, replaceTableReason)
			c.Assert(safety.Classify(ast.NewRawSQL(test.statement)), qt.Equals, safety.Destructive)
		})
	}
}

// The words that only look like a replaced table replace nothing that holds
// data: a plain CREATE TABLE, the TEMPORARY form (measured on MariaDB 12.3.3, the
// table it shadows keeps its rows), a replaced view, and MySQL's REPLACE
// statement, which writes rows.
func TestAssessSQL_WordsThatReplaceNoTable(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{name: "CREATE TABLE", statement: "CREATE TABLE victim (id INT)"},
		{name: "a table named replace", statement: "CREATE TABLE `replace` (id INT)"},
		{name: "CREATE OR REPLACE TEMPORARY TABLE", statement: "CREATE OR REPLACE TEMPORARY TABLE scratch (id INT)"},
		{name: "CREATE OR REPLACE VIEW", statement: "CREATE OR REPLACE VIEW v AS SELECT id FROM victim"},
		{name: "REPLACE INTO", statement: "REPLACE INTO victim (id) VALUES (1)"},
		{name: "REPLACE without INTO", statement: "REPLACE victim VALUES (1)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := safety.AssessSQL(test.statement)

			c.Assert(got.Severity, qt.Equals, safety.Safe)
			c.Assert(got.Reason, qt.Equals, safeReason)
		})
	}
}
