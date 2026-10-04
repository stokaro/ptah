//go:build integration

package ydb_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasretry"
	"ptah.run/migration/migrator"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// rebuildSchema is the directory the rebuild tests own.
const rebuildSchema = "ptah_ydb_rebuild"

var rebuildSchemas = []string{rebuildSchema}

// rebuildItems is the table the rebuild tests change: a key, a NOT NULL
// column, a nullable integer, a nullable text column and an index on it. Each
// option changes the declaration the way one test asks.
func rebuildItems(options ...func(*schemamodel.Database)) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Item", Name: "items", Schema: rebuildSchema}},
		Fields: []schemamodel.Field{
			{StructName: "Item", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Item", Name: "k", Type: "INTEGER"},
			{StructName: "Item", Name: "n", Type: "INTEGER", Nullable: true},
			{StructName: "Item", Name: "label", Type: "TEXT", Nullable: true},
		},
		Indexes: []schemamodel.Index{{StructName: "Item", Name: "items_label", Fields: []string{"label"}}},
	}
	for _, option := range options {
		option(db)
	}
	schemamodel.Finalize(db)
	return db
}

// withField changes the declared column of that name.
func withField(name string, change func(*schemamodel.Field)) func(*schemamodel.Database) {
	return func(db *schemamodel.Database) {
		for i := range db.Fields {
			if db.Fields[i].Name == name {
				change(&db.Fields[i])
			}
		}
	}
}

// keyedByIDAndK declares the key (id, k) in place of (id).
func keyedByIDAndK(db *schemamodel.Database) {
	db.Fields[0].Primary = false
	db.Tables[0].PrimaryKey = []string{"id", "k"}
}

// rebuildPlan plans the migration that takes the directories a test owns to
// declared, with table rebuilds allowed, as the text of a migration file.
func rebuildPlan(c *qt.C, conn *dbschema.DatabaseConnection, declared *schemamodel.Database, allow bool) (string, error) {
	c.Helper()
	info := conn.Info()
	diff, err := schemadiff.CompareWithDatabaseInfo(declared, readScoped(c, conn, rebuildSchemas), info, nil)
	c.Assert(err, qt.IsNil)
	return planner.GenerateSchemaDiffSQLWithOptions(diff, info.Dialect, planner.Options{
		Capabilities: info.Capabilities, AllowTableRebuild: allow,
	})
}

// seedRebuildItems creates the table as rebuildItems declares it and writes
// two rows into it.
func seedRebuildItems(c *qt.C, conn *dbschema.DatabaseConnection, before *schemamodel.Database, rows string) {
	c.Helper()
	apply(c, conn, planAgainst(c, conn, before, rebuildSchemas))
	c.Assert(conn.Writer().ExecuteSQL(c.Context(),
		"UPSERT INTO `"+rebuildSchema+"/items` (`id`, `k`, `n`, `label`) VALUES "+rows), qt.IsNil)
}

// rebuildMigrator runs file as migration 1, keeping its revision in the
// rebuild directory.
func rebuildMigrator(c *qt.C, conn *dbschema.DatabaseConnection, file string, observer migrator.StatementObserver) *migrator.Migrator {
	c.Helper()
	var options []migrator.FSProviderOption
	if observer != nil {
		options = append(options, migrator.WithStatementObserver(observer))
	}
	m, err := migrator.NewFSMigrator(conn, migrationFiles(map[string]string{
		"0000000001_rebuild.up.sql":   file,
		"0000000001_rebuild.down.sql": "SELECT 1;\n",
	}), options...)
	c.Assert(err, qt.IsNil)
	return m.WithRevisionTableFormat(migrator.RevisionTableFormatPtah).WithMigrationsTable(rebuildSchema, "")
}

// cleanRebuild drops what a rebuild test leaves, the scratch tables included.
func cleanRebuild(c *qt.C, conn *dbschema.DatabaseConnection) {
	c.Helper()
	dropDirectory(c, conn, rebuildSchema, "items", "__ptah_rebuild_items", "__ptah_replaced_items")
}

const twoRows = "(1l, 10, 100, 'a'u), (2l, 20, NULL, 'b'u)"

// A change YDB cannot make in place -- the key, a column type, a column made
// NOT NULL -- is planned as a rebuild when asked for, and runs through the
// migrator one query at a time. The table reads back as declared, with
// nothing left to plan, its index, and both rows as they were; no scratch
// table is left behind.
func TestYDBRebuild_PreservesTheRows(t *testing.T) {
	tests := []struct {
		name  string
		after *schemamodel.Database
		rows  []string
	}{
		{name: "a key change", after: rebuildItems(keyedByIDAndK),
			rows: []string{"`id` = 1l AND `k` = 10 AND `n` = 100 AND `label` = 'a'u",
				"`id` = 2l AND `k` = 20 AND `n` IS NULL AND `label` = 'b'u"}},
		{name: "a type change", after: rebuildItems(withField("n", func(f *schemamodel.Field) { f.Type = "BIGINT" })),
			rows: []string{"`id` = 1l AND `k` = 10 AND `n` = 100l AND `label` = 'a'u",
				"`id` = 2l AND `k` = 20 AND `n` IS NULL AND `label` = 'b'u"}},
		{name: "SET NOT NULL", after: rebuildItems(withField("label", func(f *schemamodel.Field) { f.Nullable = false })),
			rows: []string{"`id` = 1l AND `k` = 10 AND `n` = 100 AND `label` = 'a'u",
				"`id` = 2l AND `k` = 20 AND `n` IS NULL AND `label` = 'b'u"}},
	}
	for _, line := range ydbLines {
		for _, test := range tests {
			t.Run(line.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				conn := openYDB(c, line)
				cleanRebuild(c, conn)
				c.Cleanup(func() { cleanRebuild(c, conn) })
				seedRebuildItems(c, conn, rebuildItems(), twoRows)

				file, err := rebuildPlan(c, conn, test.after, true)
				c.Assert(err, qt.IsNil)
				c.Assert(file, qt.Contains, "Rows written to "+rebuildSchema+"/items between the copy and the swap are lost")

				m := rebuildMigrator(c, conn, file, nil)
				c.Assert(m.MigrateUp(c.Context()), qt.IsNil)

				c.Assert(planAgainst(c, conn, test.after, rebuildSchemas), qt.HasLen, 0)
				live := readScoped(c, conn, rebuildSchemas)
				c.Assert(tableNames(live), qt.DeepEquals, []string{rebuildSchema + "|items"})
				c.Assert(indexNamesOf(live), qt.DeepEquals, []string{"items_label"})
				c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+rebuildSchema+"/items`"), qt.Equals, int64(2))
				for _, row := range test.rows {
					c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+rebuildSchema+"/items` WHERE "+row), qt.Equals, int64(1),
						qt.Commentf("row %s", row))
				}
			})
		}
	}
}

// errInjected is the failure a test's observer raises.
var errInjected = errors.New("injected after the copy")

// failingAfter is an observer that fails the run right after the statement
// holding fragment ran and its progress was recorded.
func failingAfter(fragment string) migrator.StatementObserver {
	return migrator.StatementObserverFunc(func(_ context.Context, event migrator.StatementEvent) error {
		if strings.Contains(event.Statement, fragment) {
			return errInjected
		}
		return nil
	})
}

// A run that stops after the copy committed -- here an observer fails it --
// leaves both tables and a revision that records the CREATE and the copy. A
// rerun with --allow-dirty resumes at the first rename and completes the
// swap, and the rows are copied once.
func TestYDBRebuild_ResumesAfterAFailureAfterTheCopy(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			cleanRebuild(c, conn)
			c.Cleanup(func() { cleanRebuild(c, conn) })
			seedRebuildItems(c, conn, rebuildItems(), twoRows)
			after := rebuildItems(withField("n", func(f *schemamodel.Field) { f.Type = "BIGINT" }))
			file, err := rebuildPlan(c, conn, after, true)
			c.Assert(err, qt.IsNil)
			failed := rebuildMigrator(c, conn, file, failingAfter("INSERT INTO")).MigrateUp(c.Context())

			c.Assert(failed, qt.ErrorIs, errInjected)
			c.Assert(revisionProgress(c, rebuildMigrator(c, conn, file, nil)), qt.DeepEquals,
				[]progress{{Version: 1, State: "failed", Applied: 2, Total: 5}})
			c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+rebuildSchema+"/items`"), qt.Equals, int64(2))
			c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+rebuildSchema+"/__ptah_rebuild_items`"), qt.Equals, int64(2))

			resumed := rebuildMigrator(c, conn, file, nil)
			c.Assert(resumed.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true}), qt.IsNil)

			c.Assert(revisionProgress(c, resumed), qt.DeepEquals, []progress{{Version: 1, State: "applied", Applied: 5, Total: 5}})
			c.Assert(planAgainst(c, conn, after, rebuildSchemas), qt.HasLen, 0)
			c.Assert(tableNames(readScoped(c, conn, rebuildSchemas)), qt.DeepEquals, []string{rebuildSchema + "|items"})
			c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+rebuildSchema+"/items`"), qt.Equals, int64(2))
		})
	}
}

// A value the new type cannot hold fails the copy, which is one data query:
// nothing is copied, the old table keeps its type and its rows, and the run
// says which column. Once the row is fixed, a rerun with --allow-dirty
// resumes at the copy and completes the rebuild.
func TestYDBRebuild_AConversionFailureLeavesTheOldTable(t *testing.T) {
	asText := withField("n", func(f *schemamodel.Field) { f.Type = "TEXT" })
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			cleanRebuild(c, conn)
			c.Cleanup(func() { cleanRebuild(c, conn) })
			seedRebuildItems(c, conn, rebuildItems(asText), "(1l, 10, 'x'u, 'a'u), (2l, 20, '5'u, 'b'u)")
			after := rebuildItems(withField("n", func(f *schemamodel.Field) { f.Type = "BIGINT" }))
			file, err := rebuildPlan(c, conn, after, true)
			c.Assert(err, qt.IsNil)

			failed := rebuildMigrator(c, conn, file, nil).MigrateUp(c.Context())

			c.Assert(failed, qt.ErrorMatches,
				`(?s).*rebuilding table ptah_ydb_rebuild\.items: column n holds a value that does not convert to its new type.*`)
			c.Assert(revisionProgress(c, rebuildMigrator(c, conn, file, nil)), qt.DeepEquals,
				[]progress{{Version: 1, State: "failed", Applied: 1, Total: 5}})
			items := tableNamed(c, readScoped(c, conn, rebuildSchemas), rebuildSchema, "items")
			c.Assert(columnNamed(c, items, "n").DataType, qt.Equals, "Utf8")
			c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+rebuildSchema+"/items` WHERE `n` = 'x'u"), qt.Equals, int64(1))
			c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+rebuildSchema+"/__ptah_rebuild_items`"), qt.Equals, int64(0))

			c.Assert(conn.Writer().ExecuteSQL(c.Context(),
				"UPDATE `"+rebuildSchema+"/items` SET `n` = '7'u WHERE `n` = 'x'u"), qt.IsNil)
			resumed := rebuildMigrator(c, conn, file, nil)
			c.Assert(resumed.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true}), qt.IsNil)
			c.Assert(planAgainst(c, conn, after, rebuildSchemas), qt.HasLen, 0)
			c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+rebuildSchema+"/items` WHERE `n` = 7l"), qt.Equals, int64(1))
		})
	}
}

// A rebuild that would damage the table is refused while planning, even when
// asked for: one whose key is a Serial column, whose new sequence would start
// below the copied rows, and one carrying a partitioning setting Ptah does not
// model, which recreating would drop. Without the request, the change is
// refused with the flag that asks for it.
func TestYDBRebuild_Refusals(t *testing.T) {
	typeChange := withField("n", func(f *schemamodel.Field) { f.Type = "BIGINT" })
	serialKey := withField("id", func(f *schemamodel.Field) { f.AutoInc = true })
	tests := []struct {
		name     string
		before   *schemamodel.Database
		settings []string
		after    *schemamodel.Database
		allow    bool
		wantErr  string
	}{
		{name: "a Serial key", before: rebuildItems(serialKey), after: rebuildItems(serialKey, typeChange), allow: true,
			wantErr: `(?s).*rebuilding table "ptah_ydb_rebuild\.items": column "id" takes its values from a sequence\..*`},
		{name: "a partitioning setting", before: rebuildItems(),
			settings: []string{"ALTER TABLE `" + rebuildSchema + "/items` SET (AUTO_PARTITIONING_BY_LOAD = ENABLED)"},
			after:    rebuildItems(typeChange), allow: true,
			wantErr: `(?s).*rebuilding table "ptah_ydb_rebuild\.items": the table carries partitioning, read replica or ` +
				`key bloom filter options, which Ptah does not model.*`},
		{name: "not asked for", before: rebuildItems(), after: rebuildItems(typeChange), allow: false,
			wantErr: `(?s).*changing the type of column "n" of table "ptah_ydb_rebuild\.items" .*; YDB makes it by ` +
				`rebuilding the table, which Ptah plans when asked with --allow-table-rebuild.*`},
	}
	for _, line := range ydbLines {
		for _, test := range tests {
			t.Run(line.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				conn := openYDB(c, line)
				cleanRebuild(c, conn)
				c.Cleanup(func() { cleanRebuild(c, conn) })
				apply(c, conn, planAgainst(c, conn, test.before, rebuildSchemas))
				apply(c, conn, test.settings)

				file, err := rebuildPlan(c, conn, test.after, test.allow)

				c.Assert(err, qt.ErrorMatches, test.wantErr)
				c.Assert(file, qt.Equals, "")
			})
		}
	}
}

// copyLimitAnswers is how each line refuses a copy larger than one query may
// carry: 26.2 runs out of its 64 MiB write buffer, and 25.1 exceeds the 48 MiB
// datashard program size and answers ABORTED, the status a lock conflict
// carries too.
var copyLimitAnswers = map[string]string{
	"26.2": `(?s).*Out of buffer memory\. Used \d+ bytes of 67108864 bytes.*`,
	"25.1": `(?s).*Datashard program size limit exceeded \(\d+ > 50331648\).*`,
}

// A rebuild copies the table in one query, and a table too large for one is
// refused whole: nothing is copied. The refusal is not a conflict, so the
// migrator's transaction retry does not run it again -- on 25.1 it answers
// ABORTED, which a lock conflict answers too. 600000 rows of about 80 bytes
// are over the limit on both lines; 400000 copy.
func TestYDBRebuild_ACopyTooLargeForOneQueryIsNotRetried(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropDirectory(c, conn, rebuildSchema, "big", "big_copy")
			c.Cleanup(func() { dropDirectory(c, conn, rebuildSchema, "big", "big_copy") })
			fillRows(c, conn, rebuildSchema+"/big", 600000)
			c.Assert(conn.Writer().ExecuteSQL(c.Context(), "CREATE TABLE `"+rebuildSchema+
				"/big_copy` (id Int64 NOT NULL, v Utf8, w Utf8, PRIMARY KEY (id))"), qt.IsNil)

			_, err := conn.ExecContext(c.Context(), "INSERT INTO `"+rebuildSchema+"/big_copy` (id, v, w) "+
				"SELECT id, v, w FROM `"+rebuildSchema+"/big`")

			c.Assert(err, qt.ErrorMatches, copyLimitAnswers[line.name])
			c.Assert(atlasretry.IsRetryable(err), qt.IsFalse)
			c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+rebuildSchema+"/big_copy`"), qt.Equals, int64(0))
		})
	}
}
