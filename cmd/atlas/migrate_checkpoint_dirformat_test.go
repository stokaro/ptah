package atlas_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/atlas"
	"github.com/stokaro/ptah/dbschema"
)

// writeCheckpointPtahFixture fills migrationsDir with a ptah-format migration
// pair.
func writeCheckpointPtahFixture(c *qt.C, migrationsDir string) {
	c.Helper()
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "0000000001_init.up.sql"),
		[]byte("CREATE TABLE ckpt_users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "0000000001_init.down.sql"),
		[]byte("DROP TABLE ckpt_users;\n"), 0o600), qt.IsNil)
}

// writeCheckpointAtlasFixture fills migrationsDir with an Atlas-format history:
// single up-only files carrying timestamp versions, the shape `atlas migrate
// diff` produces and the shape a Pro checkpoint pipeline actually runs against.
func writeCheckpointAtlasFixture(c *qt.C, migrationsDir string) {
	c.Helper()
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "20250801000001_init.sql"),
		[]byte("CREATE TABLE ckpt_users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "20250801000002_email.sql"),
		[]byte("ALTER TABLE ckpt_users ADD COLUMN email TEXT;\n"), 0o600), qt.IsNil)
}

func runCompatCheckpoint(c *qt.C, args ...string) (string, error) {
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	// Execute first and bind the result: `return out.String(), cmd.Execute()`
	// evaluates out.String() before Execute runs and always yields "".
	err := cmd.Execute()
	return out.String(), err
}

// assertAtlasCheckpointWritten asserts the Atlas artifact: exactly one new
// single file whose FIRST line is the directive, covered by atlas.sum, with no
// ptah-side output. It returns the checkpoint's base name.
func assertAtlasCheckpointWritten(c *qt.C, migrationsDir, stem string) string {
	c.Helper()
	written, err := filepath.Glob(filepath.Join(migrationsDir, "*_"+stem+".sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 1)

	body, err := os.ReadFile(written[0])
	c.Assert(err, qt.IsNil)
	firstLine, _, _ := strings.Cut(string(body), "\n")
	c.Assert(firstLine, qt.Equals, "-- atlas:checkpoint")

	name := filepath.Base(written[0])
	sum, err := os.ReadFile(filepath.Join(migrationsDir, "atlas.sum"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(sum), qt.Contains, name)

	// The ptah convention must not leak into an Atlas directory.
	_, err = os.Stat(filepath.Join(migrationsDir, "ptah.sum"))
	c.Assert(os.IsNotExist(err), qt.IsTrue)
	pairs, err := filepath.Glob(filepath.Join(migrationsDir, "*.checkpoint.*"))
	c.Assert(err, qt.IsNil)
	c.Assert(pairs, qt.HasLen, 0)
	return name
}

func TestCompatCommand_MigrateCheckpointDirFormatAtlasWrites(t *testing.T) {
	c := qt.New(t)
	migrationsDir := filepath.Join(t.TempDir(), "migrations")
	writeCheckpointAtlasFixture(c, migrationsDir)

	out, err := runCompatCheckpoint(c,
		"migrate", "checkpoint",
		"--dir", migrationsDir,
		"--dev-url", "sqlite://"+filepath.Join(t.TempDir(), "shadow.db"),
		"--dir-format", "atlas",
		"snapshot",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	name := assertAtlasCheckpointWritten(c, migrationsDir, "snapshot")
	// The positional [name] became the file-name stem, as Atlas's [tag] does.
	c.Assert(strings.HasSuffix(name, "_snapshot.sql"), qt.IsTrue, qt.Commentf("name=%s", name))

	body, err := os.ReadFile(filepath.Join(migrationsDir, name))
	c.Assert(err, qt.IsNil)
	// The body is the cumulative schema, not the last migration: the column the
	// second migration added must be part of the CREATE.
	c.Assert(string(body), qt.Contains, "email")
}

func TestCompatCommand_MigrateCheckpointDefaultsToAtlasFormat(t *testing.T) {
	c := qt.New(t)
	migrationsDir := filepath.Join(t.TempDir(), "migrations")
	writeCheckpointAtlasFixture(c, migrationsDir)

	// No --dir-format at all: an unflagged Atlas pipeline must get the Atlas
	// convention back, matching the default the Atlas Pro trial registers on
	// this verb. This is the case that separates the compat default from the
	// native `ptah migrations checkpoint` default, which stays ptah.
	out, err := runCompatCheckpoint(c,
		"migrate", "checkpoint",
		"--dir", migrationsDir,
		"--dev-url", "sqlite://"+filepath.Join(t.TempDir(), "shadow.db"),
	)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	assertAtlasCheckpointWritten(c, migrationsDir, "checkpoint")
}

func TestCompatCommand_MigrateCheckpointEmptyDirFormatIsAtlas(t *testing.T) {
	c := qt.New(t)
	migrationsDir := filepath.Join(t.TempDir(), "migrations")
	writeCheckpointAtlasFixture(c, migrationsDir)

	// An explicitly empty --dir-format is a different path from omitting the
	// flag: the registered default never fires, so the mapper sees "". Measured
	// CE takes an empty value as the atlas default (it proceeds to a checksum
	// error rather than rejecting the format), so checkpoint does too. This is
	// deliberately unlike the other compat migrate verbs, which reject an empty
	// value — that rejection is itself a recorded divergence from CE.
	out, err := runCompatCheckpoint(c,
		"migrate", "checkpoint",
		"--dir", migrationsDir,
		"--dev-url", "sqlite://"+filepath.Join(t.TempDir(), "shadow.db"),
		"--dir-format", "",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	assertAtlasCheckpointWritten(c, migrationsDir, "checkpoint")
}

func TestCompatCommand_MigrateCheckpointAtlasVersionIsATimestamp(t *testing.T) {
	c := qt.New(t)
	migrationsDir := filepath.Join(t.TempDir(), "migrations")
	writeCheckpointAtlasFixture(c, migrationsDir) // newest version 20250801000002

	out, err := runCompatCheckpoint(c,
		"migrate", "checkpoint",
		"--dir", migrationsDir,
		"--dev-url", "sqlite://"+filepath.Join(t.TempDir(), "shadow.db"),
		"--dir-format", "atlas",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	name := assertAtlasCheckpointWritten(c, migrationsDir, "checkpoint")
	version, err := strconv.ParseInt(strings.TrimSuffix(name, "_checkpoint.sql"), 10, 64)
	c.Assert(err, qt.IsNil)

	// The ptah counter would have produced 20250801000003 here — one above the
	// newest migration, and a perfectly valid Atlas name, which is exactly why
	// it would otherwise go unnoticed. Atlas timestamps instead, so the version
	// must be a current UTC timestamp, far above the seeded history.
	c.Assert(version > 20260101000000, qt.IsTrue, qt.Commentf("version=%d", version))
}

func TestCompatCommand_MigrateCheckpointDirFormatPtahWrites(t *testing.T) {
	c := qt.New(t)
	migrationsDir := filepath.Join(t.TempDir(), "migrations")
	writeCheckpointPtahFixture(c, migrationsDir)

	out, err := runCompatCheckpoint(c,
		"migrate", "checkpoint",
		"--dir", migrationsDir,
		"--dev-url", "sqlite://"+filepath.Join(t.TempDir(), "shadow.db"),
		"--dir-format", "ptah",
		"snapshot",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	// The reversible pair and ptah.sum, and specifically NOT the Atlas artifact:
	// this is the fixture that separates the two conventions.
	_, err = os.Stat(filepath.Join(migrationsDir, "0000000002_snapshot.checkpoint.up.sql"))
	c.Assert(err, qt.IsNil)
	_, err = os.Stat(filepath.Join(migrationsDir, "0000000002_snapshot.checkpoint.down.sql"))
	c.Assert(err, qt.IsNil)
	_, err = os.Stat(filepath.Join(migrationsDir, "ptah.sum"))
	c.Assert(err, qt.IsNil)
	_, err = os.Stat(filepath.Join(migrationsDir, "atlas.sum"))
	c.Assert(os.IsNotExist(err), qt.IsTrue)
}

// TestCompatCommand_MigrateCheckpointAtlasRoundTrip closes the write/read loop:
// the checkpoint this verb writes must satisfy the same measured Atlas
// semantics that the hand-written fixtures in migrate_apply_integrity_test.go
// pin — bootstrap on a fresh database, silent skip on a pre-checkpoint one.
// Writing a file that merely looks right is not enough; the apply path is what
// stokaro/ptah#954 was about.
func TestCompatCommand_MigrateCheckpointAtlasRoundTrip(t *testing.T) {
	c := qt.New(t)
	dir := filepath.Join(c.TempDir(), "m")
	writeCheckpointAtlasFixture(c, dir)
	hashOut, _, err := runCompat("migrate", "hash", "--dir", "file://"+dir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", hashOut))

	// A database already carrying the pre-checkpoint history, seeded before the
	// checkpoint is cut.
	preDB := filepath.Join(c.TempDir(), "pre.db")
	applyOut, _, err := compatApply(dir, preDB)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", applyOut))

	out, err := runCompatCheckpoint(c,
		"migrate", "checkpoint",
		"--dir", dir,
		"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "shadow.db"),
		"--dir-format", "atlas",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	name := assertAtlasCheckpointWritten(c, dir, "checkpoint")
	version := strings.TrimSuffix(name, "_checkpoint.sql")

	// Fresh database: only the checkpoint runs, recorded as a single type=2
	// revision, with nothing recorded for the squashed pre-checkpoint files.
	freshDB := filepath.Join(c.TempDir(), "fresh.db")
	freshOut, _, err := compatApply(dir, freshDB)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", freshOut))
	c.Assert(freshOut, qt.Contains, "Migrating to version "+version+" from 1 pending migrations.")
	c.Assert(compatRevisionRows(c, freshDB), qt.DeepEquals, []compatRevisionRow{
		{Version: version, Description: "checkpoint", Type: 2, Applied: 1, Total: 1},
	})
	// The checkpoint body is cumulative, so the column the second migration
	// added exists even though that migration never ran here.
	c.Assert(sqliteTableCount(c, freshDB, "ckpt_users"), qt.Equals, 1)

	// Pre-checkpoint database: the checkpoint is skipped, not replayed. Assert
	// the revision rows, not just the message — replaying it is exactly the
	// double-apply failure #954 reported, and it would show up here as a third
	// row (or an error), never in the wording.
	skipOut, _, err := compatApply(dir, preDB)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", skipOut))
	c.Assert(skipOut, qt.Contains, "No migration files to execute")
	rows := compatRevisionRows(c, preDB)
	c.Assert(rows, qt.HasLen, 2)
	c.Assert(rows[0].Version, qt.Equals, "20250801000001")
	c.Assert(rows[1].Version, qt.Equals, "20250801000002")
}

// TestCompatCommand_MigrateCheckpointAtlasSupersedesEarlierCheckpoint pins the
// layering case: checkpointing a directory that already holds a checkpoint. The
// new one must sort above the whole history — including a future-dated
// migration, which is the input where a plain timestamp would land too low —
// and a fresh apply must bootstrap from the newest checkpoint, not the first.
func TestCompatCommand_MigrateCheckpointAtlasSupersedesEarlierCheckpoint(t *testing.T) {
	c := qt.New(t)
	dir := filepath.Join(c.TempDir(), "m")
	writeCheckpointAtlasFixture(c, dir)

	devURL := "sqlite://" + filepath.Join(c.TempDir(), "shadow1.db")
	out, err := runCompatCheckpoint(c, "migrate", "checkpoint", "--dir", dir, "--dev-url", devURL, "--dir-format", "atlas")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	first := assertAtlasCheckpointWritten(c, dir, "checkpoint")

	// A migration dated far past any real timestamp, added after the first
	// checkpoint.
	c.Assert(os.WriteFile(filepath.Join(dir, "29990101000000_later.sql"),
		[]byte("ALTER TABLE ckpt_users ADD COLUMN nickname TEXT;\n"), 0o600), qt.IsNil)
	hashOut, _, err := runCompat("migrate", "hash", "--dir", "file://"+dir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", hashOut))

	devURL2 := "sqlite://" + filepath.Join(c.TempDir(), "shadow2.db")
	out, err = runCompatCheckpoint(c, "migrate", "checkpoint", "--dir", dir, "--dev-url", devURL2, "--dir-format", "atlas")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	// Both checkpoints coexist; the new one is above the future-dated migration.
	c.Assert(out, qt.Contains, "29990101000001")
	_, err = os.Stat(filepath.Join(dir, first))
	c.Assert(err, qt.IsNil)

	freshDB := filepath.Join(c.TempDir(), "fresh.db")
	applyOut, _, err := compatApply(dir, freshDB)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", applyOut))
	c.Assert(compatRevisionRows(c, freshDB), qt.DeepEquals, []compatRevisionRow{
		{Version: "29990101000001", Description: "checkpoint", Type: 2, Applied: 1, Total: 1},
	})
	// The newest checkpoint is cumulative through the later migration, so the
	// column that migration adds exists without it having run.
	c.Assert(sqliteCheckpointColumnCount(c, freshDB, "ckpt_users", "nickname"), qt.Equals, 1)
}

// sqliteCheckpointColumnCount reports whether table carries column, so a
// cumulative checkpoint body can be asserted by its effect on the database
// rather than by grepping the SQL it was rendered from.
func sqliteCheckpointColumnCount(c *qt.C, dbPath, table, column string) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.Query("SELECT name FROM pragma_table_info('" + table + "')")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	count := 0
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		if name == column {
			count++
		}
	}
	c.Assert(rows.Err(), qt.IsNil)
	return count
}

func TestCompatCommand_MigrateCheckpointDirFormatRejections(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{
			name:  "external_format_rejected",
			value: "goose",
			want:  `atlas migrate checkpoint --dir-format: Atlas accepts --dir-format=goose, but Ptah does not implement that directory format yet`,
		},
		{
			name:  "unknown_format_rejected",
			value: "sprocket",
			want:  `atlas migrate checkpoint --dir-format: unknown Atlas migration directory format "sprocket": expected atlas, golang-migrate, goose, flyway, liquibase, or dbmate`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			migrationsDir := filepath.Join(t.TempDir(), "migrations")
			writeCheckpointAtlasFixture(c, migrationsDir)

			_, err := runCompatCheckpoint(c,
				"migrate", "checkpoint",
				"--dir", migrationsDir,
				"--dev-url", "sqlite://"+filepath.Join(t.TempDir(), "shadow.db"),
				"--dir-format", tt.value,
			)

			c.Assert(err, qt.ErrorMatches, tt.want)
			// Assert the protected state: the rejection happens before the engine
			// runs, so no checkpoint file and no integrity file may appear.
			matches, globErr := filepath.Glob(filepath.Join(migrationsDir, "*checkpoint*"))
			c.Assert(globErr, qt.IsNil)
			c.Assert(matches, qt.HasLen, 0)
			_, statErr := os.Stat(filepath.Join(migrationsDir, "atlas.sum"))
			c.Assert(os.IsNotExist(statErr), qt.IsTrue)
			_, statErr = os.Stat(filepath.Join(migrationsDir, "ptah.sum"))
			c.Assert(os.IsNotExist(statErr), qt.IsTrue)
		})
	}
}
