//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// generatedKindPostgresFile declares a generated column without a kind, which
// PostgreSQL creates STORED.
const generatedKindPostgresFile = "tables:\n" +
	"  t:\n" +
	"    columns:\n" +
	"      id: {type: BIGINT, primary: true}\n" +
	"      a: {type: BIGINT, not_null: true}\n" +
	"      m: {type: BIGINT, generated: a * 2}\n"

// generatedKindClickHouseFile declares generated columns without a kind, as
// STORED and as VIRTUAL, which ClickHouse creates MATERIALIZED, MATERIALIZED
// and ALIAS.
const generatedKindClickHouseFile = "tables:\n" +
	"  t:\n" +
	"    platform:\n" +
	"      clickhouse:\n" +
	"        engine: MergeTree\n" +
	"        order_by: id\n" +
	"    columns:\n" +
	"      id: {type: UInt64, primary: true}\n" +
	"      a: {type: Int64, not_null: true}\n" +
	"      m: {type: Int64, not_null: true, generated: a * 2}\n" +
	"      s: {type: Int64, not_null: true, generated: a * 3, stored: true}\n" +
	"      v: {type: Int64, not_null: true, generated: a * 4, generated_kind: VIRTUAL}\n"

// TestSchemaDiffOfAGeneratedColumnFileWithItselfE2E diffs a YAML schema file
// with itself through `ptah schema diff` on PostgreSQL and ClickHouse. The
// --to side is converted to a catalog that states each kind as declared, and
// the comparison reads both sides as the kind the engine creates, so the file
// is synced with itself. Read on the --from side alone, PostgreSQL plans
// `ALTER COLUMN "m" SET EXPRESSION AS (a * 2)` and ClickHouse a MODIFY COLUMN
// for every generated column.
func TestSchemaDiffOfAGeneratedColumnFileWithItselfE2E(t *testing.T) {
	c := qt.New(t)
	engines := []struct {
		name string
		url  string
		file string
	}{
		{
			name: "PostgreSQL",
			url: createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.PostgreSQL),
				"DROP DATABASE IF EXISTS %s WITH (FORCE)", renameInPath),
			file: generatedKindPostgresFile,
		},
		{
			name: "ClickHouse",
			url:  createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.ClickHouse), "DROP DATABASE IF EXISTS %s SYNC", renameInPath),
			file: generatedKindClickHouseFile,
		},
	}
	for _, engine := range engines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			from, to := writeSchemaFiles(c, "schema.yaml", engine.file, engine.file)

			diff := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
				"schema", "diff", "--from", from, "--to", to, "--dev-url", engine.url)

			c.Assert(diff.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", diff.Stdout, diff.Stderr))
			c.Assert(diff.Stdout, qt.Equals, "Schemas are synced, no changes to be made.\n")
		})
	}
}
