package migrate_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/migrate"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/migratesum"
)

func runGenerate(args ...string) (string, error) {
	cmd := migrate.NewMigrateGenerateCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// writeReplayFixture writes a ptah-format migration directory plus a desired
// schema file that adds one table on top of the replayed history.
func writeReplayFixture(t *testing.T) (migrationsDir, schemaPath string) {
	t.Helper()
	c := qt.New(t)
	migrationsDir = t.TempDir()
	files := map[string]string{
		"0000000001_users.up.sql":   "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		"0000000001_users.down.sql": "DROP TABLE users;\n",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(migrationsDir, name), []byte(content), 0o600), qt.IsNil)
	}
	schemaPath = filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n",
	), 0o600), qt.IsNil)
	return migrationsDir, schemaPath
}

func writeInterruptedReplayPublication(c *qt.C, migrationsDir string) []string {
	c.Helper()
	contents := []byte("CREATE TABLE interrupted_items (id INTEGER);\n")
	finalName := "9999999999_interrupted.up.sql"
	stagedName := ".ptah-migrate-diff-recovery-test.tmp"
	finalPath := filepath.Join(migrationsDir, finalName)
	stagedPath := filepath.Join(migrationsDir, stagedName)
	c.Assert(os.WriteFile(finalPath, contents, 0o600), qt.IsNil)
	c.Assert(os.WriteFile(stagedPath, contents, 0o600), qt.IsNil)
	digest := sha256.Sum256(contents)
	journal := struct {
		Version    int    `json:"version"`
		CommitMode string `json:"commit_mode"`
		Entries    []struct {
			Staged string `json:"staged"`
			Final  string `json:"final"`
			Mode   string `json:"mode"`
			Digest string `json:"digest"`
		} `json:"entries"`
	}{Version: 5, CommitMode: "journal-marker"}
	journal.Entries = append(journal.Entries, struct {
		Staged string `json:"staged"`
		Final  string `json:"final"`
		Mode   string `json:"mode"`
		Digest string `json:"digest"`
	}{
		Staged: stagedName,
		Final:  finalName,
		Mode:   "exclusive-copy",
		Digest: hex.EncodeToString(digest[:]),
	})
	journalContents, err := json.Marshal(journal)
	c.Assert(err, qt.IsNil)
	journalPath := filepath.Join(
		filepath.Dir(migrationsDir),
		"."+filepath.Base(migrationsDir)+".ptah-migrate-diff.pending",
	)
	c.Assert(os.WriteFile(journalPath, journalContents, 0o600), qt.IsNil)
	return []string{finalPath, stagedPath, journalPath}
}

func TestMigrateGenerateReplayDerivesCurrentStateFromDirectory(t *testing.T) {
	c := qt.New(t)
	migrationsDir, schemaPath := writeReplayFixture(t)
	devPath := filepath.Join(t.TempDir(), "dev.db")

	out, err := runGenerate(
		"--replay",
		"--dev-url", "sqlite://"+devPath,
		"--migrations-dir", migrationsDir,
		"--schema-file", schemaPath,
		"--name", "add_orders",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "UP:")
	c.Assert(out, qt.Contains, "DOWN:")

	matches, err := filepath.Glob(filepath.Join(migrationsDir, "*_add_orders.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(matches, qt.HasLen, 1)
	upSQL, err := os.ReadFile(matches[0])
	c.Assert(err, qt.IsNil)
	// The replayed state already contains users, so only orders is created.
	c.Assert(string(upSQL), qt.Contains, `CREATE TABLE "orders"`)
	c.Assert(string(upSQL), qt.Not(qt.Contains), `CREATE TABLE "users"`)
	assertGenerateReplayDevEmpty(c, devPath)
}

func TestMigrateGenerateReplayRefusesDriftBeforeConnectingToDevDatabase(t *testing.T) {
	c := qt.New(t)
	migrationsDir, schemaPath := writeReplayFixture(t)
	_, err := migratesum.WriteWithFormat(migrationsDir, "ptah")
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "0000000001_users.up.sql"),
		[]byte("CREATE TABLE tampered (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)

	out, err := runGenerate(
		"--replay",
		"--dev-url", "postgres://invalid.invalid/db",
		"--migrations-dir", migrationsDir,
		"--schema-file", schemaPath,
	)

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
	c.Assert(err.Error(), qt.Contains, "migration sum verification failed")
	c.Assert(err.Error(), qt.Contains, "0000000001_users.up.sql")
	c.Assert(err.Error(), qt.Not(qt.Contains), "invalid.invalid")
}

func TestMigrateGenerateReplayRecoversPendingPublicationBeforeIntegrityGate(t *testing.T) {
	c := qt.New(t)
	migrationsDir, schemaPath := writeReplayFixture(t)
	_, err := migratesum.WriteWithFormat(migrationsDir, "ptah")
	c.Assert(err, qt.IsNil)
	pendingPaths := writeInterruptedReplayPublication(c, migrationsDir)
	devPath := filepath.Join(t.TempDir(), "dev.db")

	out, err := runGenerate(
		"--replay",
		"--dev-url", "sqlite://"+devPath,
		"--migrations-dir", migrationsDir,
		"--schema-file", schemaPath,
		"--name", "add_orders",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	for _, path := range pendingPaths {
		_, statErr := os.Stat(path)
		c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
	}
}

func TestMigrateGenerateReplayConnectTimeoutStartsAfterDirectoryLock(t *testing.T) {
	c := qt.New(t)
	migrationsDir, schemaPath := writeReplayFixture(t)
	devPath := filepath.Join(t.TempDir(), "dev.db")
	lockHeld := make(chan struct{})
	releaseLock := make(chan struct{})
	lockResult := make(chan error, 1)
	go func() {
		lockResult <- atlasmigrate.WithMigrationDirectoryLock(
			t.Context(),
			migrationsDir,
			0,
			func(context.Context) error {
				close(lockHeld)
				<-releaseLock
				return nil
			},
		)
	}()
	<-lockHeld
	time.AfterFunc(250*time.Millisecond, func() { close(releaseLock) })

	out, err := runGenerate(
		"--replay",
		"--connect-timeout", "100ms",
		"--dev-url", "sqlite://"+devPath,
		"--migrations-dir", migrationsDir,
		"--schema-file", schemaPath,
		"--name", "add_orders",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(<-lockResult, qt.IsNil)
	c.Assert(out, qt.Contains, "UP:")
	assertGenerateReplayDevEmpty(c, devPath)
}

func TestMigrateGenerateShadowRefusesDriftBeforeConnectingToTarget(t *testing.T) {
	c := qt.New(t)
	migrationsDir, schemaPath := writeReplayFixture(t)
	_, err := migratesum.WriteWithFormat(migrationsDir, "ptah")
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "0000000001_users.down.sql"),
		[]byte("DROP TABLE tampered;\n"),
		0o600,
	), qt.IsNil)

	out, err := runGenerate(
		"--db-url", "postgres://invalid.invalid/target",
		"--shadow-db", "postgres://invalid.invalid/shadow",
		"--migrations-dir", migrationsDir,
		"--schema-file", schemaPath,
	)

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
	c.Assert(err.Error(), qt.Contains, "migration sum verification failed")
	c.Assert(err.Error(), qt.Contains, "0000000001_users.down.sql")
	c.Assert(err.Error(), qt.Not(qt.Contains), "invalid.invalid")
}

// TestMigrateGenerateShadowRecoversPendingPublicationBeforeIntegrityGate is the
// shadow-only half of the recovery ordering the replay path already has.
//
// An interrupted publication leaves migration artifacts the recorded checksum
// does not cover, so a gate that runs before the recovery journal is processed
// refuses on a directory the recovery would have settled — and refuses it on
// every retry, because nothing else ever runs the journal. The target URL is
// unreachable on purpose: the assertion is that the run gets far enough to fail
// on the CONNECTION, with the pending artifacts already resolved, rather than
// stopping at a checksum the recovery had not yet been given a chance to fix.
func TestMigrateGenerateShadowRecoversPendingPublicationBeforeIntegrityGate(t *testing.T) {
	c := qt.New(t)
	migrationsDir, schemaPath := writeReplayFixture(t)
	_, err := migratesum.WriteWithFormat(migrationsDir, "ptah")
	c.Assert(err, qt.IsNil)
	pendingPaths := writeInterruptedReplayPublication(c, migrationsDir)

	out, err := runGenerate(
		"--db-url", "postgres://invalid.invalid/target",
		"--shadow-db", "postgres://invalid.invalid/shadow",
		"--migrations-dir", migrationsDir,
		"--schema-file", schemaPath,
	)

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
	c.Assert(err.Error(), qt.Not(qt.Contains), "migration sum verification failed")
	c.Assert(err.Error(), qt.Contains, "invalid.invalid")
	for _, path := range pendingPaths {
		_, statErr := os.Stat(path)
		c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
	}
}

func assertGenerateReplayDevEmpty(c *qt.C, path string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+path)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	err = conn.QueryRowContext(c.Context(), `
		SELECT COUNT(*)
		FROM main.sqlite_schema
		WHERE name NOT LIKE 'sqlite_%'
	`).Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 0)
}

func TestMigrateGenerateReplayRequiresDevURL(t *testing.T) {
	c := qt.New(t)
	migrationsDir, schemaPath := writeReplayFixture(t)

	out, err := runGenerate(
		"--replay",
		"--migrations-dir", migrationsDir,
		"--schema-file", schemaPath,
	)

	c.Assert(err, qt.ErrorMatches, "--dev-url is required with --replay", qt.Commentf("%s", out))
}

func TestMigrateGenerateReplayRejectsDBURL(t *testing.T) {
	c := qt.New(t)
	migrationsDir, schemaPath := writeReplayFixture(t)

	out, err := runGenerate(
		"--replay",
		"--db-url", "sqlite://"+filepath.Join(t.TempDir(), "target.db"),
		"--dev-url", "sqlite://"+filepath.Join(t.TempDir(), "dev.db"),
		"--migrations-dir", migrationsDir,
		"--schema-file", schemaPath,
	)

	c.Assert(err, qt.ErrorMatches, "--db-url cannot be combined with --replay: .*", qt.Commentf("%s", out))
}

func TestMigrateGenerateDevURLRequiresReplay(t *testing.T) {
	c := qt.New(t)

	out, err := runGenerate(
		"--dev-url", "sqlite://"+filepath.Join(t.TempDir(), "dev.db"),
		"--db-url", "sqlite://"+filepath.Join(t.TempDir(), "target.db"),
		"--migrations-dir", t.TempDir(),
	)

	c.Assert(err, qt.ErrorMatches, "--dev-url requires --replay", qt.Commentf("%s", out))
}

func TestMigrateGenerateQualifierRejectsUnsupportedDialect(t *testing.T) {
	c := qt.New(t)
	migrationsDir, schemaPath := writeReplayFixture(t)
	devPath := filepath.Join(t.TempDir(), "dev.db")

	out, err := runGenerate(
		"--replay",
		"--dev-url", "sqlite://"+devPath,
		"--migrations-dir", migrationsDir,
		"--schema-file", schemaPath,
		"--qualifier", "tenant",
	)

	c.Assert(err, qt.ErrorMatches, `--qualifier is not supported for dialect "sqlite"`, qt.Commentf("%s", out))
}

func TestMigrateGenerateQualifierRejectsInvalidValue(t *testing.T) {
	c := qt.New(t)

	out, err := runGenerate(
		"--db-url", "sqlite://"+filepath.Join(t.TempDir(), "target.db"),
		"--migrations-dir", t.TempDir(),
		"--qualifier", "a.b",
	)

	c.Assert(err, qt.ErrorMatches, `invalid --qualifier "a\.b": .*`, qt.Commentf("%s", out))
}
