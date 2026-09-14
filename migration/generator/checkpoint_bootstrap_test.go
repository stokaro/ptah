package generator_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/generator"
	"ptah.run/migration/migrator"
)

// TestCheckpointBootstrapEquivalence is the proof the checkpoint mechanism owes
// a new database: the two paths to version C agree on the schema and on the
// rows the migrations after C read.
//
// Path A replays the whole history. Path B applies the checkpoint alone. They
// then run the same post-checkpoint migration, which updates a row only the
// bootstrap could have put there, and are compared again.
func TestCheckpointBootstrapEquivalence(t *testing.T) {
	c := qt.New(t)
	history := writeHistoryWithReferenceData(c)

	upSQL, downSQL := generateSQLiteCheckpoint(c, history, "regions")

	c.Assert(upSQL, qt.Contains, generator.BootstrapDataMarker+" regions rows=2")
	c.Assert(upSQL, qt.Contains, `INSERT INTO "regions"`)

	checkpointDir := c.TempDir()
	writeFile(c, filepath.Join(checkpointDir, "0000000003_snapshot.checkpoint.up.sql"), upSQL)
	writeFile(c, filepath.Join(checkpointDir, "0000000003_snapshot.checkpoint.down.sql"), downSQL)

	fullHistory := applyDirectory(c, history, "full-history.db")
	fromCheckpoint := applyDirectory(c, checkpointDir, "from-checkpoint.db")

	c.Assert(fromCheckpoint.regions, qt.DeepEquals, fullHistory.regions)
	c.Assert(fromCheckpoint.columns, qt.DeepEquals, fullHistory.columns)

	// The migration after the checkpoint reads a row the bootstrap carried. On
	// a database that started from a schema-only checkpoint it would update
	// nothing, and the two paths would part here rather than at the schema.
	writeFile(c, filepath.Join(history, "0000000004_promote.up.sql"),
		"UPDATE regions SET tier = 'gold' WHERE code = 'NO';\n")
	writeFile(c, filepath.Join(history, "0000000004_promote.down.sql"),
		"UPDATE regions SET tier = 'standard' WHERE code = 'NO';\n")
	writeFile(c, filepath.Join(checkpointDir, "0000000004_promote.up.sql"),
		"UPDATE regions SET tier = 'gold' WHERE code = 'NO';\n")
	writeFile(c, filepath.Join(checkpointDir, "0000000004_promote.down.sql"),
		"UPDATE regions SET tier = 'standard' WHERE code = 'NO';\n")

	fullHistoryAfter := applyDirectory(c, history, "full-history-after.db")
	fromCheckpointAfter := applyDirectory(c, checkpointDir, "from-checkpoint-after.db")

	c.Assert(fromCheckpointAfter.regions, qt.DeepEquals, fullHistoryAfter.regions)
	// Norway, not the first row by code: the update names one row, and a
	// bootstrap that had carried no rows would have updated nothing at all.
	c.Assert(regionByCode(c, fromCheckpointAfter.regions, "NO")["tier"], qt.Equals, "gold")
	c.Assert(regionByCode(c, fromCheckpointAfter.regions, "CZ")["tier"], qt.Equals, "standard")
}

// TestCheckpointWithoutDataTablesCarriesNoRows keeps the existing contract: a
// checkpoint that was not asked for data is still schema-only.
func TestCheckpointWithoutDataTablesCarriesNoRows(t *testing.T) {
	c := qt.New(t)
	history := writeHistoryWithReferenceData(c)

	upSQL, _ := generateSQLiteCheckpoint(c, history)

	c.Assert(upSQL, qt.Not(qt.Contains), generator.BootstrapDataMarker)
	c.Assert(upSQL, qt.Not(qt.Contains), "INSERT INTO")
}

func TestCheckpointBootstrapRefusesAnUnusableTable(t *testing.T) {
	tests := []struct {
		name    string
		table   string
		message string
	}{
		{
			name:    "absent table",
			table:   "missing",
			message: `.*bootstrap table missing does not exist at this version`,
		},
		{
			name:    "table without a primary key",
			table:   "events",
			message: `.*bootstrap table events has no primary key.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			history := writeHistoryWithReferenceData(c)

			_, _, err := generator.GenerateCheckpointFromShadow(context.Background(), generator.CheckpointFromShadowOptions{
				ShadowDatabaseURL: "sqlite://" + filepath.Join(c.TempDir(), "shadow.db"),
				MigrationsDir:     history,
				Dialect:           "sqlite",
				DataTables:        []string{test.table},
			})

			c.Assert(err, qt.ErrorMatches, test.message)
		})
	}
}

type appliedDatabase struct {
	regions []map[string]any
	columns []string
}

// writeHistoryWithReferenceData writes a history whose reference rows are
// created by one migration and changed by a later one, so a bootstrap that read
// the latest declarations instead of the replayed database would be visibly
// wrong.
func writeHistoryWithReferenceData(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	writeFile(c, filepath.Join(dir, "0000000001_regions.up.sql"),
		"CREATE TABLE regions (code TEXT PRIMARY KEY, name TEXT NOT NULL);\n"+
			"INSERT INTO regions (code, name) VALUES ('NO', 'Norway');\n"+
			"INSERT INTO regions (code, name) VALUES ('CZ', 'Czechia');\n"+
			"CREATE TABLE events (recorded_at TEXT NOT NULL);\n")
	writeFile(c, filepath.Join(dir, "0000000001_regions.down.sql"),
		"DROP TABLE events;\nDROP TABLE regions;\n")
	writeFile(c, filepath.Join(dir, "0000000002_tier.up.sql"),
		"ALTER TABLE regions ADD COLUMN tier TEXT;\n"+
			"UPDATE regions SET tier = 'standard';\n")
	writeFile(c, filepath.Join(dir, "0000000002_tier.down.sql"),
		"ALTER TABLE regions DROP COLUMN tier;\n")
	return dir
}

func generateSQLiteCheckpoint(c *qt.C, historyDir string, dataTables ...string) (upSQL, downSQL string) {
	c.Helper()
	up, down, err := generator.GenerateCheckpointFromShadow(context.Background(), generator.CheckpointFromShadowOptions{
		ShadowDatabaseURL: "sqlite://" + filepath.Join(c.TempDir(), "shadow.db"),
		MigrationsDir:     historyDir,
		Dialect:           "sqlite",
		DataTables:        dataTables,
	})
	c.Assert(err, qt.IsNil)
	return up, down
}

func applyDirectory(c *qt.C, dir, name string) appliedDatabase {
	c.Helper()
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(c.TempDir(), name))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	mig, err := migrator.NewFSMigrator(conn, os.DirFS(dir))
	c.Assert(err, qt.IsNil)
	c.Assert(mig.MigrateUp(ctx), qt.IsNil)

	schema, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, nil)
	c.Assert(err, qt.IsNil)
	columns := make([]string, 0)
	for _, table := range schema.Tables {
		if table.Name != "regions" {
			continue
		}
		for _, column := range table.Columns {
			columns = append(columns, column.Name)
		}
	}
	rows, err := dbschema.ReadTableRows(ctx, conn, "", "regions", []string{"code", "name", "tier"})
	c.Assert(err, qt.IsNil)
	return appliedDatabase{regions: sortedByCode(rows), columns: columns}
}

func sortedByCode(rows []map[string]any) []map[string]any {
	sorted := make([]map[string]any, len(rows))
	copy(sorted, rows)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && stringValue(sorted[j-1]["code"]) > stringValue(sorted[j]["code"]); j-- {
			sorted[j-1], sorted[j] = sorted[j], sorted[j-1]
		}
	}
	return sorted
}

func regionByCode(c *qt.C, rows []map[string]any, code string) map[string]any {
	c.Helper()
	for _, row := range rows {
		if stringValue(row["code"]) == code {
			return row
		}
	}
	c.Fatalf("no region %q in %v", code, rows)
	return nil
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}

func writeFile(c *qt.C, path, contents string) {
	c.Helper()
	c.Assert(os.WriteFile(path, []byte(contents), 0o600), qt.IsNil)
}
