//go:build integration

package ydb_test

import (
	"context"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/seeder"
)

// seedSchema is the directory the seed tests write their rows into. The
// tracker table schema_seeds sits at the database root, where an unqualified
// name lands, so each test drops it before and after itself.
const seedSchema = "ptah_ydb_data/seed"

// ownSeeds gives a test the seed directory and an empty tracker. The cleanup
// runs after the test's context is canceled, so it runs under its own.
func ownSeeds(c *qt.C, conn *dbschema.DatabaseConnection) {
	c.Helper()
	ownDirectory(c, conn, seedSchema)
	drop := func() {
		c.Assert(conn.Writer().ExecuteSQL(context.Background(), "DROP TABLE IF EXISTS schema_seeds"), qt.IsNil)
	}
	drop()
	c.Cleanup(drop)
	apply(c, conn, []string{
		"CREATE TABLE `" + seedSchema + "/regions` (code Utf8 NOT NULL, name Utf8, population Int64, PRIMARY KEY (code))",
	})
}

// seedFiles is a seed directory holding one file per name.
func seedFiles(files map[string]string) fstest.MapFS {
	fsys := fstest.MapFS{}
	for name, body := range files {
		fsys[name] = &fstest.MapFile{Data: []byte(body)}
	}
	return fsys
}

func appliedPaths(result *seeder.Result) (applied, skipped []string) {
	for _, seed := range result.Applied {
		applied = append(applied, seed.Path)
	}
	for _, seed := range result.Skipped {
		skipped = append(skipped, seed.Path)
	}
	return applied, skipped
}

// regionsSeed defines a named expression and reads it in a second statement,
// which runs only when both statements run as one query.
const regionsSeed = "$cz = 'CZ'u;\n" +
	"INSERT INTO `" + seedSchema + "/regions` (code, name, population) VALUES ($cz, 'Czechia'u, 10900000l);\n" +
	"UPSERT INTO `" + seedSchema + "/regions` (code, name) VALUES ('SK'u, 'Slovakia'u);\n"

// TestYDBSeeder_AppliesReadsBackAndSkips applies a seed, reads its rows and its
// tracker row back, and applies the directory again with nothing left to run.
func TestYDBSeeder_AppliesReadsBackAndSkips(t *testing.T) {
	c := qt.New(t)
	conn := openYDB(c)
	ownSeeds(c, conn)
	fsys := seedFiles(map[string]string{"001_regions.all.sql": regionsSeed})

	first, err := seeder.Apply(c.Context(), conn, fsys, seeder.Options{Env: "dev"})
	c.Assert(err, qt.IsNil)
	firstApplied, firstSkipped := appliedPaths(first)
	rows := readSorted(c, conn, seedSchema, "regions", "code", "name", "population")
	tracked := readSorted(c, conn, "", "schema_seeds", "seed_path", "env", "checksum")
	second, err := seeder.Apply(c.Context(), conn, fsys, seeder.Options{Env: "dev"})
	c.Assert(err, qt.IsNil)
	secondApplied, secondSkipped := appliedPaths(second)

	c.Assert(firstApplied, qt.DeepEquals, []string{"001_regions.all.sql"})
	c.Assert(firstSkipped, qt.IsNil)
	c.Assert(rows, qt.DeepEquals, []map[string]any{
		{"code": "CZ", "name": "Czechia", "population": int64(10900000)},
		{"code": "SK", "name": "Slovakia", "population": nil},
	})
	c.Assert(tracked, qt.DeepEquals, []map[string]any{
		{"seed_path": "001_regions.all.sql", "env": "dev", "checksum": first.Applied[0].Checksum},
	})
	c.Assert(secondApplied, qt.IsNil)
	c.Assert(secondSkipped, qt.DeepEquals, []string{"001_regions.all.sql"})
	c.Assert(readSorted(c, conn, seedSchema, "regions", "code", "name", "population"), qt.DeepEquals, rows)
}

// conflictingSeed inserts a new row and then one whose key the table already
// holds, so the file fails at its second statement.
const conflictingSeed = "INSERT INTO `" + seedSchema + "/regions` (code, name) VALUES ('AT'u, 'Austria'u);\n" +
	"INSERT INTO `" + seedSchema + "/regions` (code, name) VALUES ('CZ'u, 'Again'u);\n"

// TestYDBSeeder_IdempotentRecordsASeedThatConflicts runs a file whose second
// statement hits an existing key. YDB has no savepoint, and the conflict ends
// the transaction, so --idempotent rolls the whole file back and records the
// seed on its own: the first statement's row is not there, and the seed reads
// as applied. Without --idempotent the same file fails and is not recorded.
func TestYDBSeeder_IdempotentRecordsASeedThatConflicts(t *testing.T) {
	c := qt.New(t)
	conn := openYDB(c)
	ownSeeds(c, conn)
	apply(c, conn, []string{"INSERT INTO `" + seedSchema + "/regions` (code, name) VALUES ('CZ'u, 'Czechia'u)"})
	fsys := seedFiles(map[string]string{"002_conflict.all.sql": conflictingSeed})

	_, strictErr := seeder.Apply(c.Context(), conn, fsys, seeder.Options{Env: "dev"})
	trackedAfterStrict := readSorted(c, conn, "", "schema_seeds", "seed_path")
	result, err := seeder.Apply(c.Context(), conn, fsys, seeder.Options{Env: "dev", Idempotent: true})
	c.Assert(err, qt.IsNil)
	applied, _ := appliedPaths(result)

	c.Assert(seeder.IsConflictError(strictErr), qt.IsTrue, qt.Commentf("error: %v", strictErr))
	c.Assert(strictErr, qt.ErrorMatches, `(?s)apply seed 002_conflict.all.sql: .*Conflict with existing key.*`)
	c.Assert(trackedAfterStrict, qt.HasLen, 0)
	c.Assert(applied, qt.DeepEquals, []string{"002_conflict.all.sql"})
	c.Assert(readSorted(c, conn, seedSchema, "regions", "code", "name"), qt.DeepEquals, []map[string]any{
		{"code": "CZ", "name": "Czechia"},
	})
	c.Assert(readSorted(c, conn, "", "schema_seeds", "seed_path"), qt.DeepEquals, []map[string]any{
		{"seed_path": "002_conflict.all.sql"},
	})
}

// TestYDBSeeder_RefusesAStatementATransactionCannotHold refuses a seed holding
// a scheme statement before anything of it runs: YDB runs one only outside a
// transaction, and a seed runs in one.
func TestYDBSeeder_RefusesAStatementATransactionCannotHold(t *testing.T) {
	c := qt.New(t)
	conn := openYDB(c)
	ownSeeds(c, conn)
	fsys := seedFiles(map[string]string{"003_ddl.all.sql": "INSERT INTO `" + seedSchema +
		"/regions` (code) VALUES ('PL'u);\nCREATE TABLE `" + seedSchema + "/extra` (id Int64 NOT NULL, PRIMARY KEY (id));\n"})

	result, err := seeder.Apply(c.Context(), conn, fsys, seeder.Options{Env: "dev"})

	c.Assert(err, qt.ErrorMatches, `(?s)apply seed 003_ddl.all.sql: YDB runs a scheme query only outside a `+
		`transaction, and a seed runs in one; move this statement to a migration: CREATE TABLE .*`)
	c.Assert(result.Applied, qt.HasLen, 0)
	c.Assert(readSorted(c, conn, seedSchema, "regions", "code"), qt.HasLen, 0)
	c.Assert(tableNames(readScoped(c, conn, []string{seedSchema})), qt.DeepEquals, []string{seedSchema + "|regions"})
}

// TestYDBSeeder_RefusesATargetHoldingAProtectedTable lists the tables of the
// directory the connection reads as its schema, and refuses the run when one
// of them is fenced.
func TestYDBSeeder_RefusesATargetHoldingAProtectedTable(t *testing.T) {
	c := qt.New(t)
	conn := openYDB(c)
	ownSeeds(c, conn)
	const fenced = "ptah_ydb_seed_fenced"
	drop := func() {
		c.Assert(conn.Writer().ExecuteSQL(context.Background(), "DROP TABLE IF EXISTS `"+fenced+"`"), qt.IsNil)
	}
	drop()
	c.Cleanup(drop)
	apply(c, conn, []string{"CREATE TABLE `" + fenced + "` (id Int64 NOT NULL, PRIMARY KEY (id))"})
	fsys := seedFiles(map[string]string{"001_regions.all.sql": regionsSeed})

	fencedResult, fencedErr := seeder.Apply(c.Context(), conn, fsys,
		seeder.Options{Env: "dev", ProtectedTables: []string{"PTAH_YDB_SEED_FENCED"}})
	openResult, openErr := seeder.Apply(c.Context(), conn, fsys,
		seeder.Options{Env: "dev", ProtectedTables: []string{"ptah_ydb_seed_elsewhere"}})

	c.Assert(fencedErr, qt.ErrorMatches, `refusing to seed target database because protected tables exist: `+
		`PTAH_YDB_SEED_FENCED; pass --allow-prod to override`)
	c.Assert(fencedResult, qt.IsNil)
	c.Assert(openErr, qt.IsNil)
	c.Assert(openResult.Applied, qt.HasLen, 1)
}
