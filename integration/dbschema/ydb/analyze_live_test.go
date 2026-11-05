//go:build integration && ignore

// No build compiles this file. It drives the built-in YDB engine, which the
// host does not carry: stokaro/ptah-ydb-plugin serves YDB. The file is kept as
// the record of the scenarios its rows in the stokaro/ptah#4204 test ledger do
// not mark as ported, and it is deleted once the plugin ports them.

package ydb_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
)

// analyzeSchemas is the directory the ANALYZE test owns.
var analyzeSchemas = []string{"ptah_ydb_analyze"}

// A migration that holds ANALYZE runs it as a query of its own, outside a
// transaction, as every scheme statement, and on a server whose
// EnableColumnStatistics flag is off -- the default on every line, and the
// contour's -- the server refuses it and the statements before it stay
// applied, since YDB has no transaction to roll them back in. lint reports
// the statement as YD122 before it gets this far. The refusal differs by
// line: 25.1 refuses ANALYZE on a row table whatever the flag says.
func TestYDBWriter_AnalyzeStopsAMigration(t *testing.T) {
	refusals := map[string]string{
		"26.2": "(?s).*ANALYZE command is not supported because `EnableColumnStatistics` feature flag is off.*",
		"25.1": "(?s).*analyze is not supported for oltp tables.*",
	}
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, analyzeSchemas)
			c.Cleanup(func() { dropTables(c, conn, analyzeSchemas) })

			err := conn.Writer().ExecuteSQL(context.Background(),
				"CREATE TABLE `ptah_ydb_analyze/t` (id Uint64 NOT NULL, PRIMARY KEY (id));\n"+
					"ANALYZE `ptah_ydb_analyze/t`;\n")

			c.Assert(err, qt.ErrorMatches, refusals[line.name])
			c.Assert(tableNames(readScoped(c, conn, analyzeSchemas)), qt.DeepEquals, []string{"ptah_ydb_analyze|t"})
		})
	}
}
