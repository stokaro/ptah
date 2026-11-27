//go:build integration

package integration_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// sqlServerDatabaseHolding is a database of collation on the suite's SQL
// Server that holds what each statement creates.
func sqlServerDatabaseHolding(c *qt.C, collation string, statements ...string) string {
	c.Helper()
	url := sqlServerCollatedDatabase(c, collation)
	conn := connectDevDialect(c, url)
	for _, statement := range statements {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	return url
}

// sqlServerObjects reads the objects the database at url holds, each as its
// stored name and its type, ordered by name under a binary collation so that
// names differing only in case stay apart. The table a native apply logs its
// runs in is left out.
func sqlServerObjects(c *qt.C, url string) string {
	c.Helper()
	return readCaseOnlyFact(c, url, `SELECT ISNULL(STRING_AGG(CONCAT(name, N' ', RTRIM(type)) COLLATE Latin1_General_BIN, N',')
			WITHIN GROUP (ORDER BY name COLLATE Latin1_General_BIN), N'')
		FROM sys.objects WHERE is_ms_shipped = 0 AND type IN ('U', 'V', 'P', 'FN', 'SO') AND name NOT LIKE N'ptah[_]%'`)
}

// sqlServerBatches splits a plan written for SQL Server at its GO lines.
func sqlServerBatches(plan string) []string {
	var batches []string
	for batch := range strings.SplitSeq(plan, "\nGO\n") {
		if strings.TrimSpace(batch) != "" {
			batches = append(batches, batch)
		}
	}
	return batches
}

// TestSchemaDiffDropsAProcedureReadFromASchemaFileE2E plans, from two schema
// files, the removal of a procedure and a function, and runs the plan on a
// database that holds the --from file. A procedure read from a file was a
// function on the --from side, and the plan wrote DROP FUNCTION for it, which
// the server refuses (stokaro/ptah#6740).
func TestSchemaDiffDropsAProcedureReadFromASchemaFileE2E(t *testing.T) {
	tests := []struct {
		name   string
		binary clirun.Target
	}{
		{name: "native", binary: clirun.Ptah},
		{name: "compatibility", binary: clirun.Compat},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			const (
				procedure = "CREATE PROCEDURE [MixedCase] AS BEGIN SELECT 1 END"
				function  = "CREATE FUNCTION [f1]() RETURNS int AS BEGIN RETURN 1 END"
			)
			target := sqlServerDatabaseHolding(c, "SQL_Latin1_General_CP1_CI_AS", procedure, function)
			dev := sqlServerCollatedDatabase(c, "SQL_Latin1_General_CP1_CI_AS")
			from := "file://" + schemaFilePath(c, "from.sql", procedure+";\nGO\n"+function+";\nGO\n")
			to := "file://" + schemaFilePath(c, "to.sql", "CREATE TABLE [t9] (id int NOT NULL);\n")

			result := clirun.Run(c, test.binary, clirun.Options{}, "schema", "diff", "--from", from, "--to", to, "--dev-url", dev)
			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
			conn := connectDevDialect(c, target)
			for _, batch := range sqlServerBatches(result.Stdout) {
				_, err := conn.ExecContext(c.Context(), batch)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", batch))
			}

			c.Assert(result.Stdout, qt.Contains, "DROP PROCEDURE [MixedCase];\nGO\n")
			c.Assert(sqlServerObjects(c, target), qt.Equals, "t9 U")
		})
	}
}
