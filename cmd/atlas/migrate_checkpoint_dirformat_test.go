package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/atlas"
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
	return out.String(), cmd.Execute()
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
