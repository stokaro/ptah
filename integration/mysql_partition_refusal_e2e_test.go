//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// partitionRefusal is the part of the refusal every source shares.
const partitionRefusal = "Ptah does not model MySQL or MariaDB partitioning (stokaro/ptah#6357)"

// partitionedSalesSQL declares a table partitioned by LIST COLUMNS, as MySQL
// and MariaDB write it.
const partitionedSalesSQL = "CREATE TABLE sales (id int NOT NULL, region varchar(10) NOT NULL, PRIMARY KEY (id, region)) " +
	"PARTITION BY LIST COLUMNS(region) (PARTITION p_east VALUES IN ('east'), PARTITION p_west VALUES IN ('west'));\n"

// partitionSources are the sources that can declare a table's partitioning,
// each as a file and the flag that reads it. {platform} is the dialect's
// platform group and {database} the database the HCL schema block names.
var partitionSources = []struct {
	name, file, flag, content string
}{
	{name: "sql", file: "schema.sql", flag: "--schema-file", content: partitionedSalesSQL},
	{name: "sql alter", file: "schema.sql", flag: "--schema-file", content: "CREATE TABLE sales (id int NOT NULL, PRIMARY KEY (id));\n" +
		"ALTER TABLE sales PARTITION BY HASH (id) PARTITIONS 2;\n"},
	{name: "go", file: "sales.go", flag: "--root-dir", content: "package entities\n\n" +
		"//ptah:schema:table name=\"sales\" platform.{platform}.partition_by=\"HASH (id) PARTITIONS 2\"\n" +
		"type Sale struct {\n" +
		"\t//ptah:schema:field name=\"id\" type=\"INT\" primary=\"true\"\n" +
		"\tID int\n" +
		"}\n"},
	{name: "yaml", file: "schema.yaml", flag: "--schema-file", content: "tables:\n  sales:\n    platform:\n      {platform}:\n" +
		"        partition_by: \"HASH (id) PARTITIONS 2\"\n    columns:\n      id:\n        type: INT\n        primary: true\n"},
	{name: "hcl", file: "schema.hcl", flag: "--schema-file", content: "schema \"{database}\" {}\n" +
		"table \"sales\" {\n  schema = schema.{database}\n  column \"id\" {\n    type = int\n  }\n" +
		"  primary_key {\n    columns = [column.id]\n  }\n  partition {\n    type    = HASH\n    columns = [column.id]\n  }\n}\n"},
}

// partitionEngines are the engines whose partitioning Ptah does not model,
// with the platform group a model names each by.
var partitionEngines = []struct {
	name     string
	engine   dbtarget.Engine
	platform string
}{
	{name: "mysql", engine: dbtarget.MySQLAdmin, platform: "mysql"},
	{name: "mariadb", engine: dbtarget.MariaDBAdmin, platform: "mariadb"},
}

// writePartitionSource writes source for the platform group and database
// named, and returns the path its flag takes: the file, or for a Go model its
// directory.
func writePartitionSource(c *qt.C, file, flag, content, platform, database string) string {
	c.Helper()
	dir := c.TempDir()
	content = strings.NewReplacer("{platform}", platform, "{database}", database).Replace(content)
	c.Assert(os.WriteFile(filepath.Join(dir, file), []byte(content), 0o600), qt.IsNil)
	paths := map[string]string{"--root-dir": dir, "--schema-file": filepath.Join(dir, file)}
	return paths[flag]
}

// partitionReadNote is the note a read gives for the table sales, which the
// database holds partitioned.
const partitionReadNote = `table partitioning "sales" in the database is left out: ` + partitionRefusal

// partitionedOutsidePtah creates the table sales, partitioned by HASH into two
// partitions, with nothing of Ptah's in between.
const partitionedOutsidePtah = "CREATE TABLE sales (id int NOT NULL, PRIMARY KEY (id)) PARTITION BY HASH (id) PARTITIONS 2;"

// tablesAndPartitionsIn reads how many tables database holds and how many
// partitions they have, as "tables,partitions".
func (s mysqlScratch) tablesAndPartitionsIn(c *qt.C, database string) string {
	c.Helper()
	var counts string
	c.Assert(s.admin.QueryRowContext(c.Context(), "SELECT CONCAT("+
		"(SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?), ',', "+
		"(SELECT COUNT(*) FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA = ? AND PARTITION_NAME IS NOT NULL))",
		database, database,
	).Scan(&counts), qt.IsNil)
	return counts
}

// TestSchemaApplyRefusesAMySQLPartitionE2E applies a table whose partitioning
// each source declares to an empty MySQL and MariaDB database. Ptah does not
// model their partitioning: written as an option the statement reads
// PARTITION_BY=, which the server refuses, and dropped it creates one
// ordinary table. The apply refuses it by name, and the database gains no
// table.
func TestSchemaApplyRefusesAMySQLPartitionE2E(t *testing.T) {
	for _, engine := range partitionEngines {
		for _, source := range partitionSources {
			t.Run(engine.name+"/"+source.name, func(t *testing.T) {
				c := qt.New(t)
				scratch := newMySQLFamilyScratch(c, engine.engine)
				name, database := scratch.database(c, "partition")
				path := writePartitionSource(c, source.file, source.flag, source.content, engine.platform, name)

				applied := runPtahIn(c, "schema", "apply", "--db-url", database, source.flag, path, "--auto-approve")

				c.Assert(applied.ExitCode, qt.Equals, 2, historyComment(applied))
				c.Assert(applied.Stderr, qt.Contains, partitionRefusal, historyComment(applied))
				c.Assert(scratch.tablesAndPartitionsIn(c, name), qt.Equals, "0,0")
			})
		}
	}
}

// TestSchemaApplyRefusesAPartitionForAnExistingMySQLTableE2E applies a schema
// file whose table declares partitioning to a database that holds the same
// table without partitions. Dropped, the clause leaves nothing to compare, so
// the apply would report the schema synced. It refuses the clause instead,
// and the table stays as it was.
func TestSchemaApplyRefusesAPartitionForAnExistingMySQLTableE2E(t *testing.T) {
	for _, engine := range partitionEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			name, database := scratch.builtFrom(c, "CREATE TABLE sales (id int NOT NULL, region varchar(10) NOT NULL, PRIMARY KEY (id, region));")

			applied := runPtahIn(c, "schema", "apply", "--db-url", database,
				"--schema-file", writeDeclaredSchemaFile(c, "schema.sql", partitionedSalesSQL), "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 2, historyComment(applied))
			c.Assert(applied.Stderr, qt.Contains, partitionRefusal, historyComment(applied))
			c.Assert(scratch.tablesAndPartitionsIn(c, name), qt.Equals, "1,0")
		})
	}
}

// TestCompatSchemaApplyRefusesAMySQLPartitionE2E applies the partitioned table
// from a schema file with ptah-compat. The community binary writes the clause
// as a table option, which the server refuses, and reports a table that exists
// without partitions as synced; ptah-compat refuses the clause by name before
// it sends anything.
func TestCompatSchemaApplyRefusesAMySQLPartitionE2E(t *testing.T) {
	for _, engine := range partitionEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			name, database := scratch.database(c, "partition")
			_, dev := scratch.database(c, "partition_dev")

			applied := clirun.Run(c, clirun.Compat, clirun.Options{}, "schema", "apply", "-u", database,
				"--to", "file://"+writeDeclaredSchemaFile(c, "schema.sql", partitionedSalesSQL), "--dev-url", dev, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 1, historyComment(applied))
			c.Assert(applied.Stderr, qt.Contains, partitionRefusal, historyComment(applied))
			c.Assert(scratch.tablesAndPartitionsIn(c, name), qt.Equals, "0,0")
		})
	}
}

// TestReadsNameAMySQLTablePartitionedOutsidePtahE2E reads, on MySQL and
// MariaDB, a database whose table sales was partitioned outside Ptah, against a
// schema file that declares the same table without partitions. The read has
// no partitions to compare, so every verb reports the schema synced; each one
// names the table and stokaro/ptah#6357 rather than let that read as covering
// the partitions, and the table keeps them.
func TestReadsNameAMySQLTablePartitionedOutsidePtahE2E(t *testing.T) {
	for _, engine := range partitionEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			name, database := scratch.builtFrom(c, partitionedOutsidePtah)
			_, dev := scratch.database(c, "partition_dev")
			schema := writeDeclaredSchemaFile(c, "schema.sql", "CREATE TABLE sales (id int NOT NULL, PRIMARY KEY (id));\n")

			read := runPtahIn(c, "db", "read", "--db-url", database)
			planned := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", schema, "--dry-run")
			compared := runPtahIn(c, "schema", "compare", "--db-url", database, "--schema-file", schema)
			diffed := clirun.Run(c, clirun.Compat, clirun.Options{}, "schema", "diff", "--from", database,
				"--to", "file://"+schema, "--dev-url", dev)

			c.Assert(read.ExitCode, qt.Equals, 0, historyComment(read))
			c.Assert(read.Stderr, qt.Contains, partitionReadNote, historyComment(read))
			c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
			c.Assert(planned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n", historyComment(planned))
			c.Assert(planned.Stderr, qt.Contains, partitionReadNote, historyComment(planned))
			c.Assert(compared.ExitCode, qt.Equals, 0, historyComment(compared))
			c.Assert(compared.Stdout, qt.Contains, "No schema differences detected, but 1 table partitioning was not compared:\n"+
				`  table partitioning "sales": `+partitionRefusal, historyComment(compared))
			c.Assert(diffed.ExitCode, qt.Equals, 0, historyComment(diffed))
			c.Assert(diffed.Stdout, qt.Equals, "Schemas are synced, no changes to be made.\n", historyComment(diffed))
			c.Assert(diffed.Stderr, qt.Contains, `table partitioning "sales" in --from is left out: `+partitionRefusal, historyComment(diffed))
			c.Assert(scratch.tablesAndPartitionsIn(c, name), qt.Equals, "1,2")
		})
	}
}

// TestSchemaApplyNotesOnlyTheUnmodeledObjectsItsSelectionKeepsE2E plans, on
// MySQL and MariaDB, against a database holding the partitioned table sales,
// the table other and the event tick, with a selector on the run. A table the
// selection leaves out takes its partitioning out of the note, and an event a
// selector names leaves it too; what the selection keeps is still named.
func TestSchemaApplyNotesOnlyTheUnmodeledObjectsItsSelectionKeepsE2E(t *testing.T) {
	const (
		both      = "CREATE TABLE sales (id int NOT NULL, PRIMARY KEY (id));\nCREATE TABLE other (id int NOT NULL, PRIMARY KEY (id));\n"
		otherOnly = "CREATE TABLE other (id int NOT NULL, PRIMARY KEY (id));\n"
		eventNote = `event "tick" in the database is left out: Ptah does not model events, so nothing compares or plans this one`
	)
	tests := []struct {
		name     string
		schema   string
		selector []string
		wantNote string
		leftOut  string
	}{
		{name: "an excluded table", schema: otherOnly, selector: []string{"--exclude", "sales"}, wantNote: eventNote,
			leftOut: `table partitioning "sales"`},
		{name: "a table an include leaves out", schema: otherOnly, selector: []string{"--include", "other", "--include", "tick"},
			wantNote: eventNote, leftOut: `table partitioning "sales"`},
		{name: "an excluded event", schema: both, selector: []string{"--exclude", "tick"}, wantNote: partitionReadNote,
			leftOut: `event "tick"`},
	}

	for _, engine := range partitionEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				scratch := newMySQLFamilyScratch(c, engine.engine)
				name, database := scratch.builtFrom(c, partitionedOutsidePtah+
					"CREATE TABLE other (id int NOT NULL, PRIMARY KEY (id));"+
					"CREATE EVENT tick ON SCHEDULE EVERY 1 DAY DO DELETE FROM other WHERE id < 0;")
				schema := writeDeclaredSchemaFile(c, "schema.sql", test.schema)

				planned := runPtahIn(c, append([]string{"schema", "apply", "--db-url", database, "--schema-file", schema,
					"--dry-run"}, test.selector...)...)

				c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
				c.Assert(planned.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n", historyComment(planned))
				c.Assert(planned.Stderr, qt.Contains, test.wantNote, historyComment(planned))
				c.Assert(planned.Stderr, qt.Not(qt.Contains), test.leftOut, historyComment(planned))
				c.Assert(scratch.salesIn(c, name), qt.Equals, "id;2")
			})
		}
	}
}

// TestCompatNotesOnlyTheUnmodeledObjectsItsSelectionKeepsE2E runs ptah-compat
// schema diff and schema inspect with --exclude sales against the same
// database. Each names the event and leaves the partitioning of the excluded
// table out of its note, since each reports from the scoped description.
func TestCompatNotesOnlyTheUnmodeledObjectsItsSelectionKeepsE2E(t *testing.T) {
	const eventReason = `event "tick" in %s is left out: Ptah does not model events, so nothing compares or plans this one`
	for _, engine := range partitionEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			name, database := scratch.builtFrom(c, partitionedOutsidePtah+
				"CREATE TABLE other (id int NOT NULL, PRIMARY KEY (id));"+
				"CREATE EVENT tick ON SCHEDULE EVERY 1 DAY DO DELETE FROM other WHERE id < 0;")
			_, dev := scratch.database(c, "partition_dev")
			schema := writeDeclaredSchemaFile(c, "schema.sql", "CREATE TABLE other (id int NOT NULL, PRIMARY KEY (id));\n")

			diffed := clirun.Run(c, clirun.Compat, clirun.Options{}, "schema", "diff", "--from", database,
				"--to", "file://"+schema, "--dev-url", dev, "--exclude", "sales")
			inspected := clirun.Run(c, clirun.Compat, clirun.Options{}, "schema", "inspect", "-u", database,
				"--exclude", "sales")

			c.Assert(diffed.ExitCode, qt.Equals, 0, historyComment(diffed))
			c.Assert(diffed.Stdout, qt.Equals, "Schemas are synced, no changes to be made.\n", historyComment(diffed))
			c.Assert(diffed.Stderr, qt.Contains, fmt.Sprintf(eventReason, "--from"), historyComment(diffed))
			c.Assert(diffed.Stderr, qt.Not(qt.Contains), `table partitioning "sales"`, historyComment(diffed))
			c.Assert(inspected.ExitCode, qt.Equals, 0, historyComment(inspected))
			c.Assert(inspected.Stderr, qt.Contains, fmt.Sprintf(eventReason, "the database"), historyComment(inspected))
			c.Assert(inspected.Stderr, qt.Not(qt.Contains), `table partitioning "sales"`, historyComment(inspected))
			c.Assert(scratch.salesIn(c, name), qt.Equals, "id;2")
		})
	}
}

// TestSchemaApplyChangesAMySQLTablePartitionedOutsidePtahE2E applies a column
// to a table partitioned outside Ptah. The read names the partitioning and
// does not refuse the table: the server keeps a table's partitions through ADD
// COLUMN, so the change applies, and the table holds the new column and both
// partitions.
func TestSchemaApplyChangesAMySQLTablePartitionedOutsidePtahE2E(t *testing.T) {
	for _, engine := range partitionEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			name, database := scratch.builtFrom(c, partitionedOutsidePtah)
			schema := writeDeclaredSchemaFile(c, "schema.sql", "CREATE TABLE sales (id int NOT NULL, name varchar(10), PRIMARY KEY (id));\n")

			applied := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", schema, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(applied.Stdout, qt.Contains, "ALTER TABLE `sales` ADD COLUMN `name` varchar(10);", historyComment(applied))
			c.Assert(applied.Stderr, qt.Contains, partitionReadNote, historyComment(applied))
			c.Assert(scratch.salesIn(c, name), qt.Equals, "id,name;2")
		})
	}
}

// salesIn reads the columns of table sales in database, in order, and how
// many partitions it has, as "columns;partitions".
func (s mysqlScratch) salesIn(c *qt.C, database string) string {
	c.Helper()
	var shape string
	c.Assert(s.admin.QueryRowContext(c.Context(), "SELECT CONCAT("+
		"(SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION) FROM information_schema.COLUMNS "+
		"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'sales'), ';', "+
		"(SELECT COUNT(*) FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'sales' AND PARTITION_NAME IS NOT NULL))",
		database, database,
	).Scan(&shape), qt.IsNil)
	return shape
}
