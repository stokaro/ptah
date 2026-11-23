package shadow_test

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlschema"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/migrator"
	"ptah.run/migration/shadow"
)

// The drift check, the rollback rehearsal, the derived rollback, the baseline
// verification and the verification of a planned migration each replay a
// history, and each
// replays a file in a transaction of its own unless a run in tx-mode file
// refuses the file there. SQLite refuses VACUUM inside a transaction, so
// migration 1 of the history ran without one, and its replay runs without one
// too, with no mode stated. A file's directive wins over the rule.

// replayHistory is a history whose migration 1, which header leads, holds a
// VACUUM beside a statement a transaction accepts.
func replayHistory(header string) fstest.MapFS {
	return fstest.MapFS{
		"0000000001_items.up.sql":   {Data: []byte(header + "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL);\nVACUUM;\n")},
		"0000000001_items.down.sql": {Data: []byte("DROP TABLE items;\n")},
		"0000000002_note.up.sql":    {Data: []byte("ALTER TABLE items ADD COLUMN note TEXT;\n")},
		"0000000002_note.down.sql":  {Data: []byte("ALTER TABLE items DROP COLUMN note;\n")},
	}
}

// replayHistoryHeaders lead migration 1 in the rows that replay. A run in
// tx-mode file refuses the first, and a directive runs the others without a
// transaction.
var replayHistoryHeaders = []struct {
	name   string
	header string
}{
	{name: "a file without a directive"},
	{name: "a file whose directive names none", header: "-- atlas:txmode none\n"},
	{name: "a file whose Ptah directive names none", header: "-- +ptah no_transaction\n"},
}

// replayDirectiveFile leads migration 1 in the rows that are refused: the
// directive names file, which wins over the rule, so the replay runs the file
// in a transaction and the transaction refuses it.
const replayDirectiveFile = "-- atlas:txmode file\n"

// replayDirectiveRefusal is the refusal of migration 1 led by
// replayDirectiveFile.
const replayDirectiveRefusal = `(?s).*0000000001_items\.up\.sql cannot run inside a transaction: line 3 VACUUM is refused inside a transaction block.*`

// replayTarget opens a SQLite target holding the schema replayHistory makes at
// version 2, and returns it with the URL of a shadow database beside it.
func replayTarget(c *qt.C) (*dbschema.DatabaseConnection, string) {
	c.Helper()
	dir := c.TempDir()
	target, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(filepath.Join(dir, "target.db")))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(target) })
	_, err = target.ExecContext(c.Context(),
		"CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL); ALTER TABLE items ADD COLUMN note TEXT;")
	c.Assert(err, qt.IsNil)
	return target, atlasurl.SQLiteURLFromPath(filepath.Join(dir, "shadow.db"))
}

// replayAppliedOptions check target against the history as its revision table
// would record it.
func replayAppliedOptions(target *dbschema.DatabaseConnection, shadowURL, header string) shadow.AppliedVerifyOptions {
	return shadow.AppliedVerifyOptions{
		Runtime:           builtintest.Runtime(),
		ShadowDatabaseURL: shadowURL,
		TargetConn:        target,
		Migrations:        must.Must(migrator.NewFSMigrationProvider(replayHistory(header))).Migrations(),
		Dialect:           target.Info().Dialect,
	}
}

func replayRollbackOptions(target *dbschema.DatabaseConnection, shadowURL, header string) shadow.RollbackVerifyOptions {
	return shadow.RollbackVerifyOptions{
		TargetConnection:  target,
		ShadowDatabaseURL: shadowURL,
		FS:                replayHistory(header),
		CurrentVersion:    2,
		TargetVersion:     1,
	}
}

func replayDynamicRollbackOptions(target *dbschema.DatabaseConnection, devURL, header string) shadow.DynamicRollbackOptions {
	return shadow.DynamicRollbackOptions{
		Runtime:          builtintest.Runtime(),
		TargetConnection: target,
		DevDatabaseURL:   devURL,
		FS:               replayHistory(header),
		TargetVersion:    1,
	}
}

func replayBaselineOptions(target *dbschema.DatabaseConnection, shadowURL, header string) shadow.BaselineVerifyOptions {
	return shadow.BaselineVerifyOptions{
		Runtime:           builtintest.Runtime(),
		ShadowDatabaseURL: shadowURL,
		TargetConn:        target,
		MigrationsFS:      replayHistory(header),
		Version:           2,
		Dialect:           target.Info().Dialect,
	}
}

// replayDeclaration is the schema the history and replayCandidate make.
func replayDeclaration(c *qt.C) *schemamodel.Database {
	c.Helper()
	return must.Must(yamlschema.Parse(c.Context(), builtintest.Runtime().YAML(), []byte(`
tables:
  items:
    columns:
      id:
        type: INTEGER
        primary: true
      name:
        type: TEXT
        not_null: true
      note:
        type: TEXT
      tag:
        type: TEXT
`)))
}

// replayCandidate is the planned migration the verification applies on top of
// the history. It has not run anywhere, so it runs as a pending file does.
var replayCandidate = shadow.Candidate{
	Version: 3,
	Name:    "add_tag",
	UpSQL:   "ALTER TABLE items ADD COLUMN tag TEXT;",
	DownSQL: "ALTER TABLE items DROP COLUMN tag;",
}

func replayMigrationOptions(c *qt.C, header string) shadow.MigrationVerifyOptions {
	c.Helper()
	target, shadowURL := replayTarget(c)
	return shadow.MigrationVerifyOptions{
		Runtime:           builtintest.Runtime(),
		ShadowDatabaseURL: shadowURL,
		TargetConnection:  target,
		MigrationsFS:      replayHistory(header),
		Dialect:           "sqlite",
		Candidates:        []shadow.Candidate{replayCandidate},
		Generated:         replayDeclaration(c),
	}
}

// TestVerifyApplied_HappyPathReplaysEachFileAsItRan checks a target at version
// 2 against its history.
func TestVerifyApplied_HappyPathReplaysEachFileAsItRan(t *testing.T) {
	for _, test := range replayHistoryHeaders {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, shadowURL := replayTarget(c)

			result, err := shadow.VerifyApplied(c.Context(), replayAppliedOptions(target, shadowURL, test.header))

			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.DeepEquals, shadow.AppliedResult{})
		})
	}
}

// TestVerifyApplied_FailurePathKeepsTheDirective checks a history whose
// revision table a baseline wrote without running it, so its migration 1
// carries a directive no run could have applied it under.
func TestVerifyApplied_FailurePathKeepsTheDirective(t *testing.T) {
	c := qt.New(t)
	target, shadowURL := replayTarget(c)

	result, err := shadow.VerifyApplied(c.Context(), replayAppliedOptions(target, shadowURL, replayDirectiveFile))

	assertCannotCompare(c, result, err, "replay", "replay_error")
	c.Assert(err, qt.ErrorMatches, replayDirectiveRefusal)
}

// TestVerifyRollback_HappyPathReplaysEachFileAsItRan rehearses the rollback of
// migration 2.
func TestVerifyRollback_HappyPathReplaysEachFileAsItRan(t *testing.T) {
	for _, test := range replayHistoryHeaders {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, shadowURL := replayTarget(c)

			err := shadow.VerifyRollback(c.Context(), replayRollbackOptions(target, shadowURL, test.header))

			c.Assert(err, qt.IsNil)
		})
	}
}

func TestVerifyRollback_FailurePathKeepsTheDirective(t *testing.T) {
	c := qt.New(t)
	target, shadowURL := replayTarget(c)

	err := shadow.VerifyRollback(c.Context(), replayRollbackOptions(target, shadowURL, replayDirectiveFile))

	c.Assert(err, qt.ErrorMatches, `rollback verification failed: replay migrations: `+replayDirectiveRefusal)
}

// TestPlanDynamicRollback_HappyPathReplaysEachFileAsItRan derives the rollback
// to version 1.
func TestPlanDynamicRollback_HappyPathReplaysEachFileAsItRan(t *testing.T) {
	for _, test := range replayHistoryHeaders {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, devURL := replayTarget(c)

			statements, err := shadow.PlanDynamicRollback(c.Context(), replayDynamicRollbackOptions(target, devURL, test.header))

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{
				"PRAGMA foreign_keys = off",
				`CREATE TABLE "new_items" ("id" INTEGER PRIMARY KEY, "name" TEXT NOT NULL)`,
				`INSERT INTO "new_items" ("id", "name") SELECT "id", "name" FROM "items"`,
				`DROP TABLE "items"`,
				`ALTER TABLE "new_items" RENAME TO "items"`,
				"PRAGMA foreign_keys = on",
			})
		})
	}
}

func TestPlanDynamicRollback_FailurePathKeepsTheDirective(t *testing.T) {
	c := qt.New(t)
	target, devURL := replayTarget(c)

	statements, err := shadow.PlanDynamicRollback(c.Context(), replayDynamicRollbackOptions(target, devURL, replayDirectiveFile))

	c.Assert(err, qt.ErrorMatches, `dynamic rollback planning failed: replay migrations: `+replayDirectiveRefusal)
	c.Assert(statements, qt.IsNil)
}

// TestVerifyBaseline_HappyPathReplaysEachFileAsItRan verifies a baseline at
// version 2.
func TestVerifyBaseline_HappyPathReplaysEachFileAsItRan(t *testing.T) {
	for _, test := range replayHistoryHeaders {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, shadowURL := replayTarget(c)

			err := shadow.VerifyBaseline(c.Context(), replayBaselineOptions(target, shadowURL, test.header))

			c.Assert(err, qt.IsNil)
		})
	}
}

func TestVerifyBaseline_FailurePathKeepsTheDirective(t *testing.T) {
	c := qt.New(t)
	target, shadowURL := replayTarget(c)

	err := shadow.VerifyBaseline(c.Context(), replayBaselineOptions(target, shadowURL, replayDirectiveFile))

	c.Assert(err, qt.ErrorMatches, replayDirectiveRefusal)
}

// TestVerifyMigration_HappyPathReplaysEachPriorFileAsItRan verifies a planned
// migration on top of the history.
func TestVerifyMigration_HappyPathReplaysEachPriorFileAsItRan(t *testing.T) {
	for _, test := range replayHistoryHeaders {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := shadow.VerifyMigration(c.Context(), replayMigrationOptions(c, test.header))

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestVerifyMigration_FailurePathRunsACandidateAsAPendingFile refuses the
// prior file whose directive names file, and a candidate that holds a VACUUM:
// a candidate has run nowhere, so it runs in a transaction, as `migrations up`
// runs a pending file, and SQLite refuses the VACUUM there.
func TestVerifyMigration_FailurePathRunsACandidateAsAPendingFile(t *testing.T) {
	tests := []struct {
		name      string
		header    string
		candidate string
		wantErr   string
	}{
		{
			name:      "a prior file whose directive names file",
			header:    replayDirectiveFile,
			candidate: replayCandidate.UpSQL,
			wantErr:   replayDirectiveRefusal,
		},
		{
			name:      "a candidate a transaction refuses",
			candidate: replayCandidate.UpSQL + "\nVACUUM;",
			wantErr:   `(?s).*failed to apply migration 3: .*cannot VACUUM from within a transaction.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			opts := replayMigrationOptions(c, test.header)
			opts.Candidates = []shadow.Candidate{{
				Version: replayCandidate.Version,
				Name:    replayCandidate.Name,
				UpSQL:   test.candidate,
				DownSQL: replayCandidate.DownSQL,
			}}

			err := shadow.VerifyMigration(c.Context(), opts)

			var verificationErr *shadow.VerificationError
			c.Assert(err, qt.ErrorAs, &verificationErr)
			c.Assert(verificationErr.Result.Stage, qt.Equals, "replay")
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
