package atlasmigrate

// White-box testing required: filesystem publication races and post-replay
// fault injection cannot be staged deterministically through the exported
// GenerateDiff API.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/migrationreplay"
	"go.5x5.cz/ptah/internal/migrationsnapshot"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/internal/schemascope"
	"go.5x5.cz/ptah/migration/migrator"
)

const undecidedSequenceDiagnostic = "Warning: sequence \"order_seq\" is declared by --to but no change was planned for it:" +
	" the replayed migration directory records `ptah:not-described sequence`," +
	" so this comparison cannot tell it apart from one that already exists," +
	" and the creation Ptah renders for it cannot safely converge from an unknown current state.\n"

// TestCompareReplayedState_CarriesTheDropPolicyIntoTheVirtualTableGuard is
// `migrate diff`'s half of the plumbing internal/atlasschema and
// migration/generator pin.
//
// A migration directory can create an FTS5 index, and no --to document can
// declare one, so the replayed state holds a virtual table the desired side
// never names -- the shape the SQLite virtual-table guard refuses because it
// plans DROP TABLE. That refusal happens inside the comparison, while
// atlasschema.ApplyDiffPolicy deletes the drop afterwards, so a project
// carrying `diff { skip { drop_table = true } }` was refused for a statement it
// had configured away.
//
// The zero-policy row is the control: without it this would pass just as well
// against a guard that had been switched off.
func TestCompareReplayedState_CarriesTheDropPolicyIntoTheVirtualTableGuard(t *testing.T) {
	tests := []struct {
		name         string
		policy       atlasschema.DiffPolicy
		wantErr      bool
		wantContains string
	}{
		{
			name:         "a plan that would drop the virtual table is refused",
			policy:       atlasschema.DiffPolicy{},
			wantErr:      true,
			wantContains: `virtual table "docs" (module fts5)`,
		},
		{
			name:    "a plan whose table drops the policy removes is not refused",
			policy:  atlasschema.DiffPolicy{SkipDropTable: true},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			conn := connectDiffComparisonSQLite(c)
			replayed := &dbschematypes.DBSchema{
				Tables: []dbschematypes.DBTable{
					{Name: "docs", Type: "TABLE", VirtualModule: "fts5", VirtualArguments: "title, body"},
				},
			}
			runtime := diffRuntime{
				readDevSchema: func(
					*dbschema.DatabaseConnection,
					[]string,
					string,
				) (*dbschematypes.DBSchema, error) {
					return replayed, nil
				},
			}

			_, _, err := compareReplayedState(
				c.Context(), conn, runtime, nil, conn.Info().Schema,
				&goschema.Database{}, nil, nil, tt.policy,
			)

			c.Assert(err != nil, qt.Equals, tt.wantErr)
			c.Assert(errorTextOf(err), qt.Contains, tt.wantContains)
		})
	}
}

// errorTextOf renders an error for a Contains assertion that also has to accept
// the nil case, where the expected fragment is empty.
func errorTextOf(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func TestCompareReplayedState_PreservesDesiredCoverage(t *testing.T) {
	c := qt.New(t)
	conn := connectDiffComparisonSQLite(c)
	current := &dbschematypes.DBSchema{
		Extensions: []dbschematypes.DBExtension{{Name: "pgcrypto"}},
	}
	desired := &goschema.Database{
		NotDescribed: coverage.Set{}.WithKind(coverage.Extension),
	}
	runtime := diffRuntime{
		readDevSchema: func(
			*dbschema.DatabaseConnection,
			[]string,
			string,
		) (*dbschematypes.DBSchema, error) {
			return current, nil
		},
	}

	replayed, diff, err := compareReplayedState(
		c.Context(), conn, runtime, nil, conn.Info().Schema, desired, nil, nil,
		atlasschema.DiffPolicy{},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(replayed, qt.Equals, current)
	c.Assert(diff.ExtensionsRemoved, qt.HasLen, 0)
}

func TestCompareReplayedState_PreservesExplicitRemoval(t *testing.T) {
	c := qt.New(t)
	conn := connectDiffComparisonSQLite(c)
	current := &dbschematypes.DBSchema{
		Extensions: []dbschematypes.DBExtension{{Name: "pgcrypto"}},
	}
	runtime := diffRuntime{
		readDevSchema: func(
			*dbschema.DatabaseConnection,
			[]string,
			string,
		) (*dbschematypes.DBSchema, error) {
			return current, nil
		},
	}

	_, diff, err := compareReplayedState(
		c.Context(), conn, runtime, nil, conn.Info().Schema,
		&goschema.Database{}, nil, nil, atlasschema.DiffPolicy{},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{"pgcrypto"})
}

func TestCompareReplayedState_SchemaScopeKeepsDatabaseWideExtensionSynced(t *testing.T) {
	c := qt.New(t)
	conn := connectDiffComparisonSQLite(c)
	schemas := []string{"app"}
	current := &dbschematypes.DBSchema{
		Extensions: []dbschematypes.DBExtension{
			{Name: "pgcrypto", Schema: "public"},
			{Name: "citext", Schema: "extensions"},
			{Name: "unrelated", Schema: "other"},
		},
	}
	desired := schemascope.FilterGeneratedWithDefaultSchema(&goschema.Database{
		Extensions: []goschema.Extension{
			{Name: "pgcrypto", Schema: "public"},
			{Name: "citext", Schema: "extensions"},
			{Name: "unrelated", Schema: "other"},
		},
	}, schemas, "public")
	runtime := diffRuntime{
		readDevSchema: func(
			*dbschema.DatabaseConnection,
			[]string,
			string,
		) (*dbschematypes.DBSchema, error) {
			return schemascope.FilterDatabaseWithDefaultSchema(current, schemas, "public"), nil
		},
	}

	replayed, diff, err := compareReplayedState(
		c.Context(), conn, runtime, schemas, "public", desired, nil, nil,
		atlasschema.DiffPolicy{},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(replayed.Extensions, qt.DeepEquals, current.Extensions)
	c.Assert(diff.HasChanges(), qt.IsFalse)
}

func TestCompareReplayedState_SchemaScopePreservesExplicitExtensionRemoval(t *testing.T) {
	c := qt.New(t)
	conn := connectDiffComparisonSQLite(c)
	schemas := []string{"app"}
	current := &dbschematypes.DBSchema{
		Extensions: []dbschematypes.DBExtension{{Name: "citext", Schema: "extensions"}},
	}
	runtime := diffRuntime{
		readDevSchema: func(
			*dbschema.DatabaseConnection,
			[]string,
			string,
		) (*dbschematypes.DBSchema, error) {
			return schemascope.FilterDatabaseWithDefaultSchema(current, schemas, "public"), nil
		},
	}

	replayed, diff, err := compareReplayedState(
		c.Context(), conn, runtime, schemas, "public",
		schemascope.FilterGeneratedWithDefaultSchema(&goschema.Database{}, schemas, "public"), nil, nil,
		atlasschema.DiffPolicy{},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(replayed.Extensions, qt.DeepEquals, current.Extensions)
	c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{"citext"})
}

func TestCompareReplayedState_ReportsUndecidedAddition(t *testing.T) {
	c := qt.New(t)
	conn := connectDiffComparisonSQLite(c)
	current := &dbschematypes.DBSchema{
		NotDescribed: coverage.Set{}.WithKind(coverage.Sequence),
	}
	runtime := diffRuntime{
		readDevSchema: func(
			*dbschema.DatabaseConnection,
			[]string,
			string,
		) (*dbschematypes.DBSchema, error) {
			return current, nil
		},
	}
	diagnostics := &bytes.Buffer{}

	_, diff, err := compareReplayedState(
		c.Context(), conn, runtime, nil, conn.Info().Schema,
		&goschema.Database{Sequences: []goschema.Sequence{{Name: "order_seq"}}},
		diagnostics, nil, atlasschema.DiffPolicy{},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
	c.Assert(diagnostics.String(), qt.Equals, undecidedSequenceDiagnostic)
}

func connectDiffComparisonSQLite(c *qt.C) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(
		c.Context(),
		atlasurl.SQLiteURLFromPath(c.TempDir()+"/catalog.db"),
	)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

func TestGenerateDiff_RoutesUndecidedAdditionDiagnostics(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	desiredPath := filepath.Join(dir, "schema.hcl")
	c.Assert(os.WriteFile(
		desiredPath,
		[]byte("sequence \"order_seq\" {}\n"),
		0o600,
	), qt.IsNil)
	desired, err := atlassource.ClassifySet(
		"--to",
		[]string{"file://" + desiredPath},
		atlassource.ProjectEnv{},
	)
	c.Assert(err, qt.IsNil)
	conn := connectDiffComparisonSQLite(c)
	diagnostics := &bytes.Buffer{}

	result, err := generateDiff(c.Context(), conn, DiffOptions{
		Dir:         migrationsDir,
		Desired:     desired,
		Name:        "undecided_sequence",
		Diagnostics: diagnostics,
	}, diffRuntime{
		readDevSchema: func(
			*dbschema.DatabaseConnection,
			[]string,
			string,
		) (*dbschematypes.DBSchema, error) {
			return &dbschematypes.DBSchema{
				NotDescribed: coverage.Set{}.WithKind(coverage.Sequence),
			}, nil
		},
		withReplayedSnapshot: migrationreplay.WithReplayedSnapshotLocked,
	})
	artifacts, readErr := os.ReadDir(migrationsDir)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Synced, qt.IsTrue)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	c.Assert(result.SumPath, qt.Equals, "")
	c.Assert(diagnostics.String(), qt.Equals, undecidedSequenceDiagnostic)
	c.Assert(readErr, qt.IsNil)
	c.Assert(artifacts, qt.HasLen, 0)
}

func TestWriteMigrationFilesAt_CollisionRejectsStalePlan(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	// A non-cooperating writer claimed version 2 after this run allocated 1..2.
	// Publishing the stale plan must fail instead of silently moving it above
	// a migration that was never replayed.
	c.Assert(os.WriteFile(filepath.Join(dir, "2_add_email_concurrent_indexes.sql"), []byte("taken"), 0o600), qt.IsNil)
	contents := []MigrationFileContent{
		{NameSuffix: "_transactional", SQL: "SELECT 1;"},
		{NameSuffix: "_concurrent_indexes", SQL: "SELECT 2;", NoTransaction: true},
	}
	writer := openTestWriter(c, dir)

	batch, err := stageMigrationBatchAt(writer, "add_email", 1, contents)
	c.Assert(err, qt.IsNil)
	published, err := publishMigrationBatch(writer, batch)

	c.Assert(err, qt.ErrorMatches, `migration directory changed during publication: .*2_add_email_concurrent_indexes\.sql already exists`)
	c.Assert(published, qt.Equals, 1)
	c.Assert(rollBackUnjournaledBatch(writer, batch, published), qt.IsNil)
	// The colliding file kept its content and no version-1 leftover remains
	// from the aborted attempt.
	taken, readErr := os.ReadFile(filepath.Join(dir, "2_add_email_concurrent_indexes.sql"))
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(taken), qt.Equals, "taken")
	_, leftoverErr := os.Stat(filepath.Join(dir, "1_add_email_transactional.sql"))
	c.Assert(leftoverErr, qt.ErrorIs, os.ErrNotExist)
}

func TestWriteMigrationFiles_FailedWriteRollsBackEarlierFiles(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	// The second file's name exceeds the filesystem's name limit, so its
	// create fails with a non-collision error after the first file was
	// already written; the batch rolls back completely.
	oversizedSuffix := "_" + strings.Repeat("x", 512)
	writer := openTestWriter(c, dir)

	batch, err := stageMigrationBatch(writer, "add_email", []MigrationFileContent{
		{NameSuffix: "_transactional", SQL: "SELECT 1;"},
		{NameSuffix: oversizedSuffix, SQL: "SELECT 2;", NoTransaction: true},
	})
	c.Assert(err, qt.IsNil)
	published, err := publishMigrationBatch(writer, batch)

	c.Assert(err, qt.ErrorMatches, `publish migration file: .*`)
	c.Assert(published, qt.Equals, 1)
	c.Assert(rollBackUnjournaledBatch(writer, batch, published), qt.IsNil)
	_, leftoverErr := os.Stat(filepath.Join(dir, "1_add_email_transactional.sql"))
	c.Assert(leftoverErr, qt.ErrorIs, os.ErrNotExist)
}

func TestPublishMigrationBatch_ExclusiveCopyMode(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writer := openTestWriter(c, dir)
	batch, err := stageMigrationBatchAt(
		writer,
		"copy",
		1,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
	)
	c.Assert(err, qt.IsNil)
	batch.mode = publicationModeCopy

	published, err := publishMigrationBatch(writer, batch)

	c.Assert(err, qt.IsNil)
	c.Assert(published, qt.Equals, 1)
	stagedInfo, err := os.Stat(stagedPath(writer, batch, 0))
	c.Assert(err, qt.IsNil)
	finalInfo, err := os.Stat(batch.paths[0])
	c.Assert(err, qt.IsNil)
	c.Assert(os.SameFile(stagedInfo, finalInfo), qt.IsFalse)
	stagedContents, err := os.ReadFile(stagedPath(writer, batch, 0))
	c.Assert(err, qt.IsNil)
	finalContents, err := os.ReadFile(batch.paths[0])
	c.Assert(err, qt.IsNil)
	c.Assert(finalContents, qt.DeepEquals, stagedContents)
	c.Assert(rollBackUnjournaledBatch(writer, batch, published), qt.IsNil)
}

func TestWritePublicationJournal_FallsBackWithoutHardLinks(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writer := openTestWriter(c, dir)
	journal := publicationJournal{
		Version:    publicationJournalVersion,
		CommitMode: publicationCommitModeAtlasSum,
		Entries: []publicationEntry{{
			Staged: ".ptah-migrate-diff-staged.tmp",
			Final:  "1_copy.sql",
			Mode:   string(publicationModeCopy),
			Digest: contentDigest([]byte("sum")),
		}},
		Sum: []byte("sum"),
	}

	err := writePublicationJournalWithLink(
		writer,
		journal,
		func(string, string) error {
			return syscall.ENOTSUP
		},
	)

	c.Assert(err, qt.IsNil)
	journalPath := testJournalPath(writer)
	got, err := readPublicationJournal(writer)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, journal)
	backups, err := filepath.Glob(journalPath + ".*.tmp")
	c.Assert(err, qt.IsNil)
	c.Assert(backups, qt.HasLen, 1)
	c.Assert(os.WriteFile(journalPath, []byte("{"), 0o600), qt.IsNil)
	got, err = readPublicationJournal(writer)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, journal)
	c.Assert(removePublicationJournal(writer), qt.IsNil)
}

func TestMigrationWriterPublishArtifacts_PublishesCompleteBatch(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	migrationWriter, err := OpenMigrationWriter(nil, dir)
	c.Assert(err, qt.IsNil)
	defer func() { _ = migrationWriter.Close() }()

	paths, err := migrationWriter.PublishArtifacts(
		t.Context(),
		[]PublicationArtifact{
			{Name: "1_change.up.sql", Contents: []byte("SELECT 1;\n")},
			{Name: "1_change.down.sql", Contents: []byte("SELECT 2;\n")},
			{Name: "1_change.safety.json", Contents: []byte("{}\n")},
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(paths, qt.HasLen, 3)
	up, err := os.ReadFile(paths[0])
	c.Assert(err, qt.IsNil)
	c.Assert(string(up), qt.Equals, "SELECT 1;\n")
	down, err := os.ReadFile(paths[1])
	c.Assert(err, qt.IsNil)
	c.Assert(string(down), qt.Equals, "SELECT 2;\n")
	report, err := os.ReadFile(paths[2])
	c.Assert(err, qt.IsNil)
	c.Assert(string(report), qt.Equals, "{}\n")
	writer := openTestWriter(c, dir)
	journalPath := testJournalPath(writer)
	_, err = os.Stat(journalPath)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	_, err = os.Stat(journalPath + publicationCommitMarkerSuffix)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

func TestMigrationWriterPublishArtifacts_CollisionRollsBackWholeBatch(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	existingPath := filepath.Join(dir, "1_change.down.sql")
	c.Assert(os.WriteFile(existingPath, []byte("existing\n"), 0o600), qt.IsNil)
	migrationWriter, err := OpenMigrationWriter(nil, dir)
	c.Assert(err, qt.IsNil)
	defer func() { _ = migrationWriter.Close() }()

	paths, err := migrationWriter.PublishArtifacts(
		t.Context(),
		[]PublicationArtifact{
			{Name: "1_change.up.sql", Contents: []byte("SELECT 1;\n")},
			{Name: "1_change.down.sql", Contents: []byte("SELECT 2;\n")},
		},
	)

	c.Assert(err, qt.ErrorMatches, `migration directory changed during publication: .* already exists`)
	c.Assert(paths, qt.IsNil)
	_, err = os.Stat(filepath.Join(dir, "1_change.up.sql"))
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	existing, err := os.ReadFile(existingPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(existing), qt.Equals, "existing\n")
	staged, err := filepath.Glob(filepath.Join(dir, stagedMigrationPattern))
	c.Assert(err, qt.IsNil)
	c.Assert(staged, qt.HasLen, 0)
	writer := openTestWriter(c, dir)
	_, err = os.Stat(testJournalPath(writer))
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

func TestStageMigrationBatch_FallsBackToExclusiveCopy(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writer := openTestWriter(c, dir)

	batch, err := stageMigrationBatchAtWithModeDetector(
		writer,
		"copy",
		1,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
		atlasmigrateimport.FormatAtlas,
		func(d *pathguard.OpenedDirectory, stagedName string) (publicationMode, error) {
			return detectPublicationModeWithLink(
				d,
				stagedName,
				func(string, string) error {
					return syscall.ENOTSUP
				},
			)
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(batch.mode, qt.Equals, publicationModeCopy)
	published, err := publishMigrationBatch(writer, batch)
	c.Assert(err, qt.IsNil)
	c.Assert(published, qt.Equals, 1)
	stagedInfo, err := os.Stat(stagedPath(writer, batch, 0))
	c.Assert(err, qt.IsNil)
	finalInfo, err := os.Stat(batch.paths[0])
	c.Assert(err, qt.IsNil)
	c.Assert(os.SameFile(stagedInfo, finalInfo), qt.IsFalse)
	c.Assert(rollBackUnjournaledBatch(writer, batch, published), qt.IsNil)
}

func TestWriteDiffArtifacts_SumPublishFailureRollsBackMigrations(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(os.Mkdir(filepath.Join(dir, "atlas.sum"), 0o700), qt.IsNil)
	baseSnapshot, err := migrationsnapshot.CaptureStable(os.DirFS(dir))
	c.Assert(err, qt.IsNil)
	writer := openTestWriter(c, dir)

	result, err := writeDiffArtifacts(
		t.Context(),
		writer,
		"add_email",
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
		baseSnapshot,
		nil,
		diffWriteLayout{},
	)

	c.Assert(err, qt.ErrorMatches, `write atlas\.sum: .*`)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	entries, readErr := os.ReadDir(dir)
	c.Assert(readErr, qt.IsNil)
	c.Assert(entries, qt.HasLen, 1)
	c.Assert(entries[0].Name(), qt.Equals, "atlas.sum")
	c.Assert(entries[0].IsDir(), qt.IsTrue)
}

func TestWriteDiffArtifacts_ReverseNoTransactionRefusesUnrepresentedForeignLayoutsBeforePublication(t *testing.T) {
	formats := []atlasmigrateimport.Format{
		atlasmigrateimport.FormatGolangMigrate,
	}

	for _, format := range formats {
		t.Run(string(format), func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			baseSnapshot, err := migrationsnapshot.CaptureStable(os.DirFS(dir))
			c.Assert(err, qt.IsNil)
			writer := openTestWriter(c, dir)

			result, err := writeDiffArtifacts(
				t.Context(),
				writer,
				"concurrent_index",
				[]MigrationFileContent{{
					SQL:                  "CREATE INDEX CONCURRENTLY idx_widgets_id ON widgets (id);",
					DownSQL:              "DROP INDEX CONCURRENTLY idx_widgets_id;",
					ReverseNoTransaction: true,
				}},
				baseSnapshot,
				nil,
				diffWriteLayout{format: format},
			)

			c.Assert(result, qt.DeepEquals, DiffResult{})
			c.Assert(err, qt.ErrorMatches,
				`migration directory format "`+string(format)+`" cannot safely express a rollback that requires no-transaction execution; no migration files or atlas\.sum were written`)
			entries, readErr := os.ReadDir(dir)
			c.Assert(readErr, qt.IsNil)
			c.Assert(entries, qt.HasLen, 0,
				qt.Commentf("a refused %s rollback published artifacts", format))
		})
	}
}

func TestWriteDiffArtifacts_GooseNoTransactionPublishesWholeFileDirective(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	baseSnapshot, err := migrationsnapshot.CaptureStable(os.DirFS(dir))
	c.Assert(err, qt.IsNil)
	writer := openTestWriter(c, dir)

	result, err := writeDiffArtifacts(
		t.Context(),
		writer,
		"concurrent_index",
		[]MigrationFileContent{{
			SQL:                  "CREATE INDEX CONCURRENTLY idx_widgets_id ON widgets (id);",
			DownSQL:              "DROP INDEX CONCURRENTLY idx_widgets_id;",
			NoTransaction:        true,
			ReverseNoTransaction: true,
			Statements:           []string{"CREATE INDEX CONCURRENTLY idx_widgets_id ON widgets (id)"},
			ReverseStatements:    []string{"DROP INDEX CONCURRENTLY idx_widgets_id"},
		}},
		baseSnapshot,
		nil,
		diffWriteLayout{format: atlasmigrateimport.FormatGoose},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(result.MigrationPaths, qt.HasLen, 1)
	contents, readErr := os.ReadFile(result.MigrationPaths[0])
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(contents), qt.Equals,
		"-- +goose NO TRANSACTION\n-- +goose Up\n"+
			"CREATE INDEX CONCURRENTLY idx_widgets_id ON widgets (id);\n\n"+
			"-- +goose Down\nDROP INDEX CONCURRENTLY idx_widgets_id;\n")
	_, statErr := os.Stat(filepath.Join(dir, "atlas.sum"))
	c.Assert(statErr, qt.IsNil)
}

func TestWriteDiffArtifacts_EnumAddRefusesUnrepresentedForeignLayoutsBeforePublication(t *testing.T) {
	c := qt.New(t)
	contents, err := BuildMigrationFileContents(
		platform.Postgres,
		capability.Postgres17(),
		"",
		[]ast.Node{
			ast.NewAlterType("status").AddOperation(ast.NewAddEnumValueOperation("archived")),
		},
	)
	c.Assert(err, qt.IsNil)
	c.Assert(contents, qt.HasLen, 1)
	c.Assert(contents[0].NoTransaction, qt.IsTrue)

	formats := []atlasmigrateimport.Format{
		atlasmigrateimport.FormatGolangMigrate,
	}

	for _, format := range formats {
		t.Run(string(format), func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			baseSnapshot, err := migrationsnapshot.CaptureStable(os.DirFS(dir))
			c.Assert(err, qt.IsNil)
			writer := openTestWriter(c, dir)

			result, err := writeDiffArtifacts(
				t.Context(),
				writer,
				"enum_add",
				contents,
				baseSnapshot,
				nil,
				diffWriteLayout{format: format},
			)

			c.Assert(result, qt.DeepEquals, DiffResult{})
			c.Assert(err, qt.ErrorMatches,
				`migration directory format "`+string(format)+`" cannot safely express a forward migration that requires no-transaction execution; no migration files or atlas\.sum were written`)
			entries, readErr := os.ReadDir(dir)
			c.Assert(readErr, qt.IsNil)
			c.Assert(entries, qt.HasLen, 0,
				qt.Commentf("a refused %s forward migration published artifacts", format))
		})
	}
}

func TestRecoverPendingPublication_RollsBackInterruptedBatch(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	initialPath := filepath.Join(dir, "1_initial.sql")
	c.Assert(os.WriteFile(initialPath, []byte("SELECT 1;"), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	writer := openTestWriter(c, dir)
	batch, err := stageMigrationBatchAt(
		writer,
		"interrupted",
		2,
		[]MigrationFileContent{{SQL: "SELECT 2;"}},
	)
	c.Assert(err, qt.IsNil)
	_, _ = beginTestPublication(
		c,
		writer,
		batch,
		[]MigrationFileContent{{SQL: "SELECT 2;"}},
	)
	published, err := publishMigrationBatch(writer, batch)
	c.Assert(err, qt.IsNil)
	c.Assert(published, qt.Equals, 1)

	c.Assert(recoverPendingPublication(writer), qt.IsNil)

	_, err = os.Stat(batch.paths[0])
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	_, err = os.Stat(stagedPath(writer, batch, 0))
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	_, err = os.Stat(testJournalPath(writer))
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	result, err := migratesum.VerifyWithFormat(os.DirFS(dir), migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(result.OK(), qt.IsTrue)
}

func TestRecoverPendingPublication_RollsBackInterruptedCopyBatch(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	initialPath := filepath.Join(dir, "1_initial.sql")
	c.Assert(os.WriteFile(initialPath, []byte("SELECT 1;"), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	writer := openTestWriter(c, dir)
	batch, err := stageMigrationBatchAt(
		writer,
		"interrupted_copy",
		2,
		[]MigrationFileContent{{SQL: "SELECT 2;"}},
	)
	c.Assert(err, qt.IsNil)
	batch.mode = publicationModeCopy
	_, _ = beginTestPublication(
		c,
		writer,
		batch,
		[]MigrationFileContent{{SQL: "SELECT 2;"}},
	)
	published, err := publishMigrationBatch(writer, batch)
	c.Assert(err, qt.IsNil)
	c.Assert(published, qt.Equals, 1)

	c.Assert(recoverPendingPublication(writer), qt.IsNil)

	_, err = os.Stat(batch.paths[0])
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	_, err = os.Stat(stagedPath(writer, batch, 0))
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	result, err := migratesum.VerifyWithFormat(
		os.DirFS(dir),
		migrator.MigrationDirFormatAtlas,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(result.OK(), qt.IsTrue)
}

func TestRecoverPendingPublication_RollsBackInterruptedMoveBatch(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writer := openTestWriter(c, dir)
	batch, err := stageMigrationBatchAt(
		writer,
		"interrupted_move",
		1,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
	)
	c.Assert(err, qt.IsNil)
	batch.mode = publicationModeWriteThroughMove
	_, _ = beginTestPublication(
		c,
		writer,
		batch,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
	)
	published, err := publishMigrationBatch(writer, batch)
	c.Assert(err, qt.IsNil)
	c.Assert(published, qt.Equals, 1)
	_, err = os.Stat(stagedPath(writer, batch, 0))
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)

	c.Assert(recoverPendingPublication(writer), qt.IsNil)

	_, err = os.Stat(batch.paths[0])
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	_, err = os.Stat(testJournalPath(writer))
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

func TestRecoverPendingPublication_CleansQuarantineOnlyStateForEveryMode(t *testing.T) {
	tests := []struct {
		name string
		mode publicationMode
	}{
		{name: "hard link", mode: publicationModeHardLink},
		{name: "copy", mode: publicationModeCopy},
		{name: "write-through move", mode: publicationModeWriteThroughMove},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			writer := openTestWriter(c, dir)
			batch, err := stageMigrationBatchAt(
				writer,
				"interrupted",
				1,
				[]MigrationFileContent{{SQL: "SELECT 1;"}},
			)
			c.Assert(err, qt.IsNil)
			batch.mode = test.mode
			_, _ = beginTestPublication(
				c,
				writer,
				batch,
				[]MigrationFileContent{{SQL: "SELECT 1;"}},
			)
			published, err := publishMigrationBatch(writer, batch)
			c.Assert(err, qt.IsNil)
			c.Assert(published, qt.Equals, 1)
			quarantinePath := stagedPath(writer, batch, 0) + publicationRollbackSuffix
			c.Assert(os.Rename(batch.paths[0], quarantinePath), qt.IsNil)
			c.Assert(removeRootedFiles(writer.dir, batch.stagedNames), qt.IsNil)

			c.Assert(recoverPendingPublication(writer), qt.IsNil)

			_, err = os.Stat(batch.paths[0])
			c.Assert(err, qt.ErrorIs, os.ErrNotExist)
			_, err = os.Stat(stagedPath(writer, batch, 0))
			c.Assert(err, qt.ErrorIs, os.ErrNotExist)
			_, err = os.Stat(quarantinePath)
			c.Assert(err, qt.ErrorIs, os.ErrNotExist)
			_, err = os.Stat(testJournalPath(writer))
			c.Assert(err, qt.ErrorIs, os.ErrNotExist)
		})
	}
}

func TestRemovePublicationJournal_RetireFailurePreservesCommitMarker(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writer := openTestWriter(c, dir)
	journalPath := testJournalPath(writer)
	markerPath := journalPath + publicationCommitMarkerSuffix
	c.Assert(os.WriteFile(journalPath, []byte("{}"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(markerPath, []byte("committed"), 0o600), qt.IsNil)
	retireErr := errors.New("injected journal retirement failure")

	err := removePublicationJournalWithRetirer(
		writer,
		func(*pathguard.OpenedDirectory, string, string) error {
			return retireErr
		},
	)

	c.Assert(err, qt.ErrorIs, retireErr)
	_, err = os.Stat(journalPath)
	c.Assert(err, qt.IsNil)
	_, err = os.Stat(markerPath)
	c.Assert(err, qt.IsNil)
}

func TestRecoverPendingPublication_FinalizesMarkerCommittedBatch(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writer := openTestWriter(c, dir)
	batch, err := stageArtifactBatch(
		writer,
		[]PublicationArtifact{
			{Name: "1_change.up.sql", Contents: []byte("SELECT 1;\n")},
			{Name: "1_change.safety.json", Contents: []byte("{}\n")},
		},
	)
	c.Assert(err, qt.IsNil)
	journal, err := beginMarkerPublication(writer, batch)
	c.Assert(err, qt.IsNil)
	published, err := publishMigrationBatch(writer, batch)
	c.Assert(err, qt.IsNil)
	c.Assert(published, qt.Equals, 2)
	c.Assert(writePublicationCommitMarker(writer, journal), qt.IsNil)

	c.Assert(recoverPendingPublication(writer), qt.IsNil)

	up, err := os.ReadFile(batch.paths[0])
	c.Assert(err, qt.IsNil)
	c.Assert(string(up), qt.Equals, "SELECT 1;\n")
	report, err := os.ReadFile(batch.paths[1])
	c.Assert(err, qt.IsNil)
	c.Assert(string(report), qt.Equals, "{}\n")
	journalPath := testJournalPath(writer)
	_, err = os.Stat(journalPath)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	_, err = os.Stat(journalPath + publicationCommitMarkerSuffix)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	_, err = os.Stat(journalPath + publicationCleanupSuffix)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

func TestRecoverPendingPublication_FinalizesCommittedBatch(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writer := openTestWriter(c, dir)
	batch, err := stageMigrationBatchAt(
		writer,
		"committed",
		1,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
	)
	c.Assert(err, qt.IsNil)
	journal, sum := beginTestPublication(
		c,
		writer,
		batch,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
	)
	published, err := publishMigrationBatch(writer, batch)
	c.Assert(err, qt.IsNil)
	c.Assert(published, qt.Equals, 1)
	_, err = publishDirSum(writer, sum)
	c.Assert(err, qt.IsNil)
	committed, err := publicationCommitted(writer, journal)
	c.Assert(err, qt.IsNil)
	c.Assert(committed, qt.IsTrue)

	c.Assert(recoverPendingPublication(writer), qt.IsNil)

	contents, err := os.ReadFile(batch.paths[0])
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "SELECT 1;")
	_, err = os.Stat(stagedPath(writer, batch, 0))
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	_, err = os.Stat(testJournalPath(writer))
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	result, err := migratesum.VerifyWithFormat(os.DirFS(dir), migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(result.OK(), qt.IsTrue)
}

func TestRecoverPendingPublication_RejectsForeignCollision(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writer := openTestWriter(c, dir)
	batch, err := stageMigrationBatchAt(
		writer,
		"collision",
		1,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
	)
	c.Assert(err, qt.IsNil)
	_, _ = beginTestPublication(
		c,
		writer,
		batch,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
	)
	c.Assert(os.WriteFile(batch.paths[0], []byte("foreign"), 0o600), qt.IsNil)

	err = recoverPendingPublication(writer)

	c.Assert(err, qt.ErrorMatches, `cannot safely recover migration publication: .* content changed; preserved at .*`)
	_, err = os.Stat(batch.paths[0])
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	contents, err := os.ReadFile(stagedPath(writer, batch, 0) + publicationRollbackSuffix)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "foreign")
	_, err = os.Stat(stagedPath(writer, batch, 0))
	c.Assert(err, qt.IsNil)
	_, err = os.Stat(testJournalPath(writer))
	c.Assert(err, qt.IsNil)
}

// TestAbortPendingPublication_RejectsForeignReplacement runs every publication
// mode, because the platform picks one and the refusal has to hold under all
// three.
//
// It used to let platformPublicationMode choose, which made it a test of the
// host: Unix detects hard links and publishes by adding a second name, so the
// staging entry survives, while Windows publishes by an atomic rename that
// consumes it. Only the last assertion depended on that, and it was the one
// that reddened -- the refusal, the preserved foreign bytes and the retained
// journal are identical in all three modes.
//
// Pinning one mode instead would have been the smaller change and the worse
// one: it would have retired the mode Windows actually uses from the test that
// covers this path.
// retainedStagingEntries reports how many staging entries a published batch
// leaves behind, from the mode the journal recorded for it.
//
// Publishing by rename consumes the staging entry; publishing by hard link or
// by copy adds a second name and leaves it. The journal is the only place a
// test can learn which happened when the batch was staged inside the code
// under test.
func retainedStagingEntries(c *qt.C, writer *migrationWriterDir) int {
	c.Helper()
	journal, err := readPublicationJournal(writer)
	c.Assert(err, qt.IsNil)
	c.Assert(journal.Entries, qt.Not(qt.HasLen), 0)
	retained := 0
	for _, entry := range journal.Entries {
		if publicationMode(entry.Mode) != publicationModeWriteThroughMove {
			retained++
		}
	}
	return retained
}

func TestAbortPendingPublication_RejectsForeignReplacement(t *testing.T) {
	tests := []struct {
		name string
		mode publicationMode
		// stagedSurvives records whether publication leaves the staging entry
		// behind. A rename consumes it; a link or a copy does not.
		stagedSurvives bool
	}{
		{name: "hard link", mode: publicationModeHardLink, stagedSurvives: true},
		{name: "copy", mode: publicationModeCopy, stagedSurvives: true},
		{name: "write-through move", mode: publicationModeWriteThroughMove, stagedSurvives: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			writer := openTestWriter(c, dir)
			batch, err := stageMigrationBatchAt(
				writer,
				"collision",
				1,
				[]MigrationFileContent{{SQL: "SELECT 1;"}},
			)
			c.Assert(err, qt.IsNil)
			batch.mode = test.mode
			_, _ = beginTestPublication(
				c,
				writer,
				batch,
				[]MigrationFileContent{{SQL: "SELECT 1;"}},
			)
			published, err := publishMigrationBatch(writer, batch)
			c.Assert(err, qt.IsNil)
			c.Assert(published, qt.Equals, 1)
			c.Assert(os.Remove(batch.paths[0]), qt.IsNil)
			c.Assert(os.WriteFile(batch.paths[0], []byte("foreign"), 0o600), qt.IsNil)

			err = abortPendingPublication(writer, batch, published)

			c.Assert(err, qt.ErrorMatches, `roll back published migration files: cannot safely recover migration publication: .* content changed; preserved at .*`)
			_, err = os.Stat(batch.paths[0])
			c.Assert(err, qt.ErrorIs, os.ErrNotExist)
			contents, err := os.ReadFile(stagedPath(writer, batch, 0) + publicationRollbackSuffix)
			c.Assert(err, qt.IsNil)
			c.Assert(string(contents), qt.Equals, "foreign")
			_, err = os.Stat(stagedPath(writer, batch, 0))
			c.Assert(err == nil, qt.Equals, test.stagedSurvives)
			_, err = os.Stat(testJournalPath(writer))
			c.Assert(err, qt.IsNil)
		})
	}
}

func TestAbortPendingPublication_RejectsInPlaceHardLinkMutation(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writer := openTestWriter(c, dir)
	batch, err := stageMigrationBatchAt(
		writer,
		"mutated",
		1,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
	)
	c.Assert(err, qt.IsNil)
	batch.mode = publicationModeHardLink
	_, _ = beginTestPublication(
		c,
		writer,
		batch,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
	)
	published, err := publishMigrationBatch(writer, batch)
	c.Assert(err, qt.IsNil)
	c.Assert(published, qt.Equals, 1)
	c.Assert(os.WriteFile(batch.paths[0], []byte("foreign"), 0o600), qt.IsNil)

	err = abortPendingPublication(writer, batch, published)

	c.Assert(err, qt.ErrorMatches, `roll back published migration files: cannot safely recover migration publication: staging file content changed; preserved .*`)
	_, err = os.Stat(batch.paths[0])
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	contents, err := os.ReadFile(stagedPath(writer, batch, 0) + publicationRollbackSuffix)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "foreign")
	stagedContents, err := os.ReadFile(stagedPath(writer, batch, 0))
	c.Assert(err, qt.IsNil)
	c.Assert(string(stagedContents), qt.Equals, "foreign")
	_, err = os.Stat(testJournalPath(writer))
	c.Assert(err, qt.IsNil)
}

func TestWriteDiffArtifacts_CommitUncertainRetainsRecoverableBatch(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	baseSnapshot, err := migrationsnapshot.CaptureStable(os.DirFS(dir))
	c.Assert(err, qt.IsNil)
	writer := openTestWriter(c, dir)

	result, err := writeDiffArtifactsWithSumWriter(
		t.Context(),
		writer,
		"uncertain",
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
		baseSnapshot,
		nil,
		diffWriteLayout{},
		func(w *migrationWriterDir, sum *migratesum.SumFile) (string, error) {
			path, writeErr := publishDirSum(w, sum)
			c.Assert(writeErr, qt.IsNil)
			return path, &migratesum.CommitUncertainError{
				Err: errors.New("injected directory sync failure"),
			}
		},
	)

	c.Assert(err, qt.ErrorMatches, `write atlas\.sum; migration publication journal retained for recovery: injected directory sync failure`)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	sqlFiles, err := filepath.Glob(filepath.Join(dir, "*_uncertain.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(sqlFiles, qt.HasLen, 1)
	stagedFiles, err := filepath.Glob(filepath.Join(dir, stagedMigrationPattern))
	c.Assert(err, qt.IsNil)
	// How many staging entries survive publication is the publication mode's
	// answer, not this test's: a rename consumes the entry, a link or a copy
	// leaves it. The count is read from the journal rather than assumed,
	// because the platform chooses the mode -- Unix detects hard links, Windows
	// always moves -- and a fixed 1 here asserted the host.
	c.Assert(stagedFiles, qt.HasLen, retainedStagingEntries(c, writer))
	journalPath := testJournalPath(writer)
	_, err = os.Stat(journalPath)
	c.Assert(err, qt.IsNil)

	c.Assert(recoverPendingPublication(writer), qt.IsNil)
	sqlFiles, err = filepath.Glob(filepath.Join(dir, "*_uncertain.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(sqlFiles, qt.HasLen, 1)
	stagedFiles, err = filepath.Glob(filepath.Join(dir, stagedMigrationPattern))
	c.Assert(err, qt.IsNil)
	c.Assert(stagedFiles, qt.HasLen, 0)
	_, err = os.Stat(journalPath)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	verifyResult, err := migratesum.VerifyWithFormat(
		os.DirFS(dir),
		migrator.MigrationDirFormatAtlas,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(verifyResult.OK(), qt.IsTrue)
}

func TestWriteDiffArtifacts_RejectsUnreplayedConcurrentMigration(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	baseSnapshot, err := migrationsnapshot.CaptureStable(os.DirFS(dir))
	c.Assert(err, qt.IsNil)
	foreignPath := filepath.Join(dir, "99_foreign.sql")
	c.Assert(os.WriteFile(foreignPath, []byte("SELECT 99;"), 0o600), qt.IsNil)
	writer := openTestWriter(c, dir)

	result, err := writeDiffArtifacts(
		t.Context(),
		writer,
		"planned",
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
		baseSnapshot,
		nil,
		diffWriteLayout{},
	)

	c.Assert(err, qt.ErrorMatches, `migration directory changed during migrate diff planning`)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	contents, err := os.ReadFile(foreignPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "SELECT 99;")
	plannedFiles, err := filepath.Glob(filepath.Join(dir, "*_planned.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(plannedFiles, qt.HasLen, 0)
	_, err = os.Stat(testJournalPath(writer))
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

func beginTestPublication(
	c *qt.C,
	writer *migrationWriterDir,
	batch migrationBatch,
	contents []MigrationFileContent,
) (publicationJournal, *migratesum.SumFile) {
	c.Helper()
	fsys, err := writer.FS()
	c.Assert(err, qt.IsNil)
	baseSnapshot, err := migrationsnapshot.CaptureStable(fsys)
	c.Assert(err, qt.IsNil)
	_, sum, err := preparePublicationSnapshot(baseSnapshot, batch, contents, atlasmigrateimport.FormatAtlas)
	c.Assert(err, qt.IsNil)
	journal, err := beginPublication(writer, batch, sum.Bytes())
	c.Assert(err, qt.IsNil)
	return journal, sum
}

func TestRecoverPendingPublication_RemovesOrphanTemps(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writer := openTestWriter(c, dir)
	stagedName, err := stageRootedFile(
		writer.dir,
		stagedMigrationPattern,
		[]byte("SELECT 1;"),
		publishedFileMode,
	)
	c.Assert(err, qt.IsNil)
	journalPath := testJournalPath(writer)
	journalTemp, err := os.CreateTemp(
		filepath.Dir(journalPath),
		filepath.Base(journalPath)+".*.tmp",
	)
	c.Assert(err, qt.IsNil)
	journalTempPath := journalTemp.Name()
	c.Assert(journalTemp.Close(), qt.IsNil)

	c.Assert(recoverPendingPublication(writer), qt.IsNil)

	_, err = os.Stat(filepath.Join(dir, stagedName))
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	_, err = os.Stat(journalTempPath)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

func TestWriteMigrationFiles_EmptySQLIsRejected(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writer := openTestWriter(c, dir)

	batch, err := stageMigrationBatch(writer, "noop", []MigrationFileContent{{SQL: "   \n"}})

	c.Assert(err, qt.ErrorMatches, `migration SQL is empty`)
	c.Assert(batch.paths, qt.HasLen, 0)
}

// openTestWriter binds dir the way the writer does in production: one rooted
// handle for the migration directory and one for its parent.
func openTestWriter(c *qt.C, dir string) *migrationWriterDir {
	c.Helper()
	writer, err := createMigrationWriterDir(nil, dir)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(writer.Close(), qt.IsNil)
	})
	return writer
}

// testJournalPath spells the journal's pathname for assertions only. The writer
// itself never resolves it; it names the journal as a direct child of the
// opened parent handle.
func testJournalPath(writer *migrationWriterDir) string {
	return filepath.Join(writer.parent.Path(), publicationJournalName(writer))
}

func stagedPath(writer *migrationWriterDir, batch migrationBatch, index int) string {
	return filepath.Join(writer.path, batch.stagedNames[index])
}

func publishMigrationBatch(writer *migrationWriterDir, batch migrationBatch) (int, error) {
	return publishMigrationBatchContext(context.Background(), writer, batch)
}

func rollBackUnjournaledBatch(
	writer *migrationWriterDir,
	batch migrationBatch,
	published int,
) error {
	if err := removeRootedFiles(writer.dir, batch.names[:published]); err != nil {
		return fmt.Errorf("roll back published migration files: %w", err)
	}
	if err := removeRootedFiles(writer.dir, batch.stagedNames); err != nil {
		return fmt.Errorf("remove rolled back migration staging files: %w", err)
	}
	if err := writer.dir.Sync(); err != nil {
		return fmt.Errorf("sync rolled back migration directory: %w", err)
	}
	return nil
}

func TestGenerateDiff_PostReplayReadFailureCleansAndReleasesLock(t *testing.T) {
	c := qt.New(t)
	conn, opts := prepareGenerateDiffFaultTest(c)
	readErr := errors.New("injected schema read failure")

	result, err := generateDiff(t.Context(), conn, opts, diffRuntime{
		readDevSchema: func(
			*dbschema.DatabaseConnection,
			[]string,
			string,
		) (*dbschematypes.DBSchema, error) {
			return nil, readErr
		},
		withReplayedSnapshot: migrationreplay.WithReplayedSnapshotLocked,
	})

	c.Assert(err, qt.ErrorIs, readErr)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	assertSQLiteCleanupObjectCount(c, conn, 0)
	assertDiffDirectoryReleased(c, opts.Dir)
}

func TestGenerateDiff_FinalCleanupFailureIsNotRetried(t *testing.T) {
	c := qt.New(t)
	conn, opts := prepareGenerateDiffFaultTest(c)
	cleanupErr := errors.New("injected final cleanup failure")
	cleanupCalls := 0

	result, err := generateDiff(t.Context(), conn, opts, diffRuntime{
		readDevSchema: readScopedDevSchema,
		withReplayedSnapshot: func(
			ctx context.Context,
			conn *dbschema.DatabaseConnection,
			snapshot fs.FS,
			format migrator.MigrationDirFormat,
			consume func(*dbschema.DatabaseConnection) error,
		) error {
			cleanupCalls++
			return errors.Join(
				migrationreplay.WithReplayedSnapshotLocked(ctx, conn, snapshot, format, consume),
				cleanupErr,
			)
		},
	})

	c.Assert(err, qt.ErrorIs, cleanupErr)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	c.Assert(cleanupCalls, qt.Equals, 1)
	assertSQLiteCleanupObjectCount(c, conn, 0)
	assertDiffDirectoryReleased(c, opts.Dir)
}

func TestGenerateDiff_JoinsPostReplayFailureAndCleanupFailure(t *testing.T) {
	c := qt.New(t)
	conn, opts := prepareGenerateDiffFaultTest(c)
	readErr := errors.New("injected schema read failure")
	cleanupErr := errors.New("injected cleanup failure")
	cleanupCalls := 0

	result, err := generateDiff(t.Context(), conn, opts, diffRuntime{
		readDevSchema: func(
			*dbschema.DatabaseConnection,
			[]string,
			string,
		) (*dbschematypes.DBSchema, error) {
			return nil, readErr
		},
		withReplayedSnapshot: func(
			ctx context.Context,
			conn *dbschema.DatabaseConnection,
			snapshot fs.FS,
			format migrator.MigrationDirFormat,
			consume func(*dbschema.DatabaseConnection) error,
		) error {
			cleanupCalls++
			return errors.Join(
				migrationreplay.WithReplayedSnapshotLocked(ctx, conn, snapshot, format, consume),
				cleanupErr,
			)
		},
	})

	c.Assert(err, qt.ErrorIs, readErr)
	c.Assert(err, qt.ErrorIs, cleanupErr)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	c.Assert(cleanupCalls, qt.Equals, 1)
	assertSQLiteCleanupObjectCount(c, conn, 0)
	assertDiffDirectoryReleased(c, opts.Dir)
}

func TestGenerateDiff_CancellationDuringCleanupPreventsArtifacts(t *testing.T) {
	c := qt.New(t)
	conn, opts := prepareGenerateDiffFaultTest(c)
	ctx, cancel := context.WithCancel(t.Context())
	cleanupCalls := 0

	result, err := generateDiff(ctx, conn, opts, diffRuntime{
		readDevSchema: readScopedDevSchema,
		withReplayedSnapshot: func(
			replayCtx context.Context,
			replayConn *dbschema.DatabaseConnection,
			snapshot fs.FS,
			format migrator.MigrationDirFormat,
			consume func(*dbschema.DatabaseConnection) error,
		) error {
			cleanupCalls++
			return migrationreplay.WithReplayedSnapshotLocked(
				replayCtx,
				replayConn,
				snapshot,
				format,
				func(conn *dbschema.DatabaseConnection) error {
					consumeErr := consume(conn)
					cancel()
					return errors.Join(consumeErr, replayCtx.Err())
				},
			)
		},
	})

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	c.Assert(cleanupCalls, qt.Equals, 1)
	assertSQLiteCleanupObjectCount(c, conn, 0)
	assertDiffDirectoryReleased(c, opts.Dir)
}

func TestGenerateDiff_PreparePublicationFailurePreservesExistingArtifacts(t *testing.T) {
	c := qt.New(t)
	conn, opts := prepareGenerateDiffFaultTest(c)
	previousSum, err := os.ReadFile(
		filepath.Join(opts.Dir, migratesum.AtlasFileName),
	)
	c.Assert(err, qt.IsNil)
	prepareErr := errors.New("injected preparation failure")
	opts.PreparePublication = func(stagedPaths []string) error {
		writeErr := os.WriteFile(
			stagedPaths[0],
			[]byte("SELECT 99;\n"),
			0o600,
		)
		return errors.Join(writeErr, prepareErr)
	}

	result, err := generateDiff(t.Context(), conn, opts, diffRuntime{
		readDevSchema:        readScopedDevSchema,
		withReplayedSnapshot: migrationreplay.WithReplayedSnapshotLocked,
	})

	c.Assert(err, qt.ErrorIs, prepareErr)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	sqlFiles, err := filepath.Glob(filepath.Join(opts.Dir, "*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(sqlFiles, qt.HasLen, 1)
	restoredSum, err := os.ReadFile(
		filepath.Join(opts.Dir, migratesum.AtlasFileName),
	)
	c.Assert(err, qt.IsNil)
	c.Assert(restoredSum, qt.DeepEquals, previousSum)
	verification, err := migratesum.VerifyWithFormat(
		os.DirFS(opts.Dir),
		migrator.MigrationDirFormatAtlas,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(verification.OK(), qt.IsTrue)
	assertSQLiteCleanupObjectCount(c, conn, 0)
	assertDiffDirectoryLockReleased(c, opts.Dir)
}

func TestGenerateDiff_PreparedContentsArePublishedWithMatchingChecksum(t *testing.T) {
	c := qt.New(t)
	conn, opts := prepareGenerateDiffFaultTest(c)
	opts.PreparePublication = func(stagedPaths []string) error {
		return os.WriteFile(
			stagedPaths[0],
			[]byte("SELECT 99;\n"),
			0o600,
		)
	}

	result, err := generateDiff(t.Context(), conn, opts, diffRuntime{
		readDevSchema:        readScopedDevSchema,
		withReplayedSnapshot: migrationreplay.WithReplayedSnapshotLocked,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.MigrationPaths, qt.HasLen, 1)
	sqlFiles, err := filepath.Glob(filepath.Join(opts.Dir, "*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(sqlFiles, qt.HasLen, 2)
	contents, err := os.ReadFile(result.MigrationPaths[0])
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "SELECT 99;\n")
	verification, err := migratesum.VerifyWithFormat(
		os.DirFS(opts.Dir),
		migrator.MigrationDirFormatAtlas,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(verification.OK(), qt.IsTrue)
	assertSQLiteCleanupObjectCount(c, conn, 0)
	assertDiffDirectoryLockReleased(c, opts.Dir)
}

func TestGenerateDiff_PreparePublicationRunsUnderDirectoryLock(t *testing.T) {
	c := qt.New(t)
	conn, opts := prepareGenerateDiffFaultTest(c)
	var competingLock *dirLock
	var competingErr error
	opts.LockTimeout = time.Millisecond
	opts.PreparePublication = func([]string) error {
		competingLock, competingErr = acquireDirLock(
			t.Context(),
			opts.Dir,
			opts.LockTimeout,
		)
		return competingErr
	}

	result, err := generateDiff(t.Context(), conn, opts, diffRuntime{
		readDevSchema:        readScopedDevSchema,
		withReplayedSnapshot: migrationreplay.WithReplayedSnapshotLocked,
	})

	c.Assert(err, qt.ErrorMatches, `.*migration directory lock timeout after 1ms: .*`)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	c.Assert(competingLock, qt.IsNil)
	c.Assert(competingErr, qt.IsNotNil)
	assertSQLiteCleanupObjectCount(c, conn, 0)
	assertDiffDirectoryReleased(c, opts.Dir)
}

func TestCanonicalMigrationDirResolvesSymlinkAlias(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	realDir := filepath.Join(root, "real", "migrations")
	c.Assert(os.MkdirAll(realDir, 0o755), qt.IsNil)
	aliasDir := filepath.Join(root, "alias")
	c.Assert(os.Symlink(realDir, aliasDir), qt.IsNil)

	canonicalReal, err := canonicalMigrationDir(realDir)
	c.Assert(err, qt.IsNil)
	canonicalAlias, err := canonicalMigrationDir(aliasDir)
	c.Assert(err, qt.IsNil)

	c.Assert(canonicalAlias, qt.Equals, canonicalReal)
	c.Assert(migrationDirLockPath(canonicalAlias), qt.Equals, migrationDirLockPath(canonicalReal))
}

func TestTryAcquireDirLock_ReclaimsStaleFile(t *testing.T) {
	c := qt.New(t)
	lockPath := filepath.Join(c.TempDir(), ".migrations"+lockFileName)
	c.Assert(os.WriteFile(lockPath, []byte("stale"), 0o600), qt.IsNil)

	lock, err := tryAcquireDirLock(lockPath)
	c.Assert(err, qt.IsNil)
	c.Assert(lock.release(), qt.IsNil)

	info, err := os.Stat(lockPath)
	c.Assert(err, qt.IsNil)
	c.Assert(info.Mode().IsRegular(), qt.IsTrue)
	lock, err = tryAcquireDirLock(lockPath)
	c.Assert(err, qt.IsNil)
	c.Assert(lock.release(), qt.IsNil)
}

func TestGenerateDiff_SerializesSQLiteDevDatabaseAcrossDirectories(t *testing.T) {
	c := qt.New(t)
	conn, opts := prepareGenerateDiffFaultTest(c)
	opts.Dir = filepath.Join(c.TempDir(), "other-migrations")
	opts.LockTimeout = time.Millisecond
	devLock, err := acquireDevDatabaseLock(t.Context(), conn, 0)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(devLock.release(), qt.IsNil)
	})

	result, err := generateDiff(t.Context(), conn, opts, diffRuntime{
		readDevSchema:        readScopedDevSchema,
		withReplayedSnapshot: migrationreplay.WithReplayedSnapshotLocked,
	})

	c.Assert(err, qt.ErrorMatches, `acquire migrate diff dev database lock: acquire sqlite dev database realm lock: lock timeout after 1ms`)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	_, err = os.Stat(opts.Dir)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

func prepareGenerateDiffFaultTest(c *qt.C) (*dbschema.DatabaseConnection, DiffOptions) {
	c.Helper()
	dir := c.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "1_init.sql"),
		[]byte(`
CREATE TABLE replayed_users (id INTEGER PRIMARY KEY);
CREATE VIEW replayed_user_ids AS SELECT id FROM replayed_users;
`),
		0o600,
	), qt.IsNil)
	_, err := migratesum.WriteWithFormat(migrationsDir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(
		schemaPath,
		[]byte("CREATE TABLE desired_users (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	desired, err := atlassource.ClassifySet(
		"--to",
		[]string{"file://" + schemaPath},
		atlassource.ProjectEnv{},
	)
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(
		c.Context(),
		atlasurl.SQLiteURLFromPath(filepath.Join(dir, "dev.db")),
	)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		dbschema.CloseAndWarn(conn)
	})
	return conn, DiffOptions{
		Dir:     migrationsDir,
		Desired: desired,
		Name:    "fault_injection",
	}
}

func assertSQLiteCleanupObjectCount(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	want int,
) {
	c.Helper()
	var count int
	err := conn.QueryRowContext(c.Context(), `
		SELECT COUNT(*)
		FROM pragma_table_list
		WHERE schema = ?
		  AND type IN ('table', 'view', 'virtual')
		  AND name NOT LIKE 'sqlite_%'
		  AND name <> 'schema_migrations'
	`, "main").Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, want)
}

func assertDiffDirectoryReleased(c *qt.C, dir string) {
	c.Helper()
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 2)
	assertDiffDirectoryLockReleased(c, dir)
}

func assertDiffDirectoryLockReleased(c *qt.C, dir string) {
	c.Helper()
	lock, err := tryAcquireDirLock(migrationDirLockPath(dir))
	c.Assert(err, qt.IsNil)
	c.Assert(lock.release(), qt.IsNil)
}

// TestDiffOptionsVerifyDirRoutesThroughTheCallersPredicate pins the seam
// stokaro/ptah#1086 added.
//
// `migrate diff` re-checks the directory's integrity file once the migration
// directory lock is held, and until #1086 that recheck was a private verifier
// with rules of its own: it accepted a directory carrying no atlas.sum at all,
// and reported a stale one in wording no other verb uses. The compatibility
// surface now supplies the same predicate its preflight refused with, so a
// directory edited between the two is refused with the community binary's bytes
// instead of a second verifier's.
//
// Reverting the routing -- calling verifyDirSum unconditionally -- prints
// `got nil error but want non-nil` on the "caller's predicate refuses" row,
// because the default accepts exactly what the caller rejects. The nil rows are
// the control: they must keep answering nil, so the seam cannot be "always
// refuse".
func TestDiffOptionsVerifyDirRoutesThroughTheCallersPredicate(t *testing.T) {
	errCallerRefused := errors.New("caller refused")

	tests := []struct {
		name string
		// opts carries the hook under test.
		opts DiffOptions
		// wantErr is what the returned error must be in the chain, nil for the
		// rows the seam has to keep accepting.
		wantErr error
	}{
		{
			name: "nil hook keeps the permissive default",
			opts: DiffOptions{},
		},
		{
			name:    "caller's predicate refuses",
			opts:    DiffOptions{VerifyDir: func(fs.FS) error { return errCallerRefused }},
			wantErr: errCallerRefused,
		},
		{
			name: "caller's predicate accepts",
			opts: DiffOptions{VerifyDir: func(fs.FS) error { return nil }},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			// A directory holding one migration and no atlas.sum: the state the
			// default accepts and the compatibility gate refuses.
			dir := c.TempDir()
			c.Assert(os.WriteFile(
				filepath.Join(dir, "1_init.sql"),
				[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
				0o600,
			), qt.IsNil)
			snapshot, captureErr := migrationsnapshot.CaptureStable(os.DirFS(dir))
			c.Assert(captureErr, qt.IsNil)

			c.Assert(tt.opts.verifyDir(snapshot), qt.ErrorIs, tt.wantErr)
		})
	}
}

func TestCaptureVerifiedMigrationDirSeparatesPublicationAndReplaySources(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	rawMigration := []byte("CREATE TABLE {{ .table }} (id INTEGER PRIMARY KEY);\n")
	renderedMigration := []byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n")
	c.Assert(os.WriteFile(filepath.Join(dir, "1_init.sql"), rawMigration, 0o600), qt.IsNil)
	rendered := fstest.MapFS{
		"1_init.sql": &fstest.MapFile{Data: renderedMigration, Mode: 0o444},
		"atlas.sum":  &fstest.MapFile{Data: []byte("rendered sum\n"), Mode: 0o444},
	}
	verified := false
	writer := openTestWriter(c, dir)
	defer func() {
		c.Assert(writer.Close(), qt.IsNil)
	}()

	snapshots, err := captureVerifiedMigrationDir(writer, DiffOptions{
		ReplaySource: rendered,
		VerifyDir: func(fsys fs.FS) error {
			verified = true
			contents, readErr := fs.ReadFile(fsys, "1_init.sql")
			c.Assert(readErr, qt.IsNil)
			c.Assert(contents, qt.DeepEquals, renderedMigration)
			return nil
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(verified, qt.IsTrue)
	publicationContents, err := fs.ReadFile(snapshots.publication, "1_init.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(publicationContents, qt.DeepEquals, rawMigration)
	replayContents, err := fs.ReadFile(snapshots.replay, "1_init.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(replayContents, qt.DeepEquals, renderedMigration)
	_, err = fs.Stat(snapshots.publication, "atlas.sum")
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
	_, err = fs.Stat(snapshots.replay, "atlas.sum")
	c.Assert(err, qt.IsNil)
}
