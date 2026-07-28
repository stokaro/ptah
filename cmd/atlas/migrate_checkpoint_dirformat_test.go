package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/atlas"
)

// writeCheckpointPtahFixture fills migrationsDir with a ptah-format migration
// pair, the only directory format the checkpoint writer supports.
func writeCheckpointPtahFixture(c *qt.C, migrationsDir string) {
	c.Helper()
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "0000000001_init.up.sql"),
		[]byte("CREATE TABLE ckpt_users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "0000000001_init.down.sql"),
		[]byte("DROP TABLE ckpt_users;\n"), 0o600), qt.IsNil)
}

func TestNewAtlasCommand_MigrateCheckpointDirFormatPtahWrites(t *testing.T) {
	c := qt.New(t)
	migrationsDir := filepath.Join(t.TempDir(), "migrations")
	writeCheckpointPtahFixture(c, migrationsDir)

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "checkpoint",
		"--dir", migrationsDir,
		"--dev-url", "sqlite://" + filepath.Join(t.TempDir(), "shadow.db"),
		"--dir-format", "ptah",
		"snapshot",
	})

	err := cmd.Execute()

	// --dir-format=ptah is the native default spelled explicitly, so the
	// checkpoint pair is written as usual.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	_, err = os.Stat(filepath.Join(migrationsDir, "0000000002_snapshot.checkpoint.up.sql"))
	c.Assert(err, qt.IsNil)
	_, err = os.Stat(filepath.Join(migrationsDir, "0000000002_snapshot.checkpoint.down.sql"))
	c.Assert(err, qt.IsNil)
}

func TestNewAtlasCommand_MigrateCheckpointDirFormatWaiversAndRejections(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{
			// Atlas-format checkpoint output is a recorded waiver: the engine
			// marks checkpoints via the ptah file-name convention plus
			// ptah.sum, and the Atlas-format reader has no `-- atlas:checkpoint`
			// directive support, so an atlas-format checkpoint file would
			// replay as an ordinary migration.
			name:  "atlas_waived",
			value: "atlas",
			want:  `atlas migrate checkpoint --dir-format: Atlas accepts --dir-format=atlas, but Ptah writes checkpoint files only in the ptah two-file convention \(NNNNNNNNNN_name\.checkpoint\.up\.sql/\.down\.sql plus ptah\.sum\); Atlas-format checkpoint output is a recorded waiver, not pending work`,
		},
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
			writeCheckpointPtahFixture(c, migrationsDir)

			cmd := atlas.NewAtlasCommand()
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs([]string{
				"migrate", "checkpoint",
				"--dir", migrationsDir,
				"--dev-url", "sqlite://" + filepath.Join(t.TempDir(), "shadow.db"),
				"--dir-format", tt.value,
			})

			err := cmd.Execute()

			c.Assert(err, qt.ErrorMatches, tt.want)
			// Nothing was written: the rejection happens before the engine runs.
			matches, globErr := filepath.Glob(filepath.Join(migrationsDir, "*.checkpoint.*"))
			c.Assert(globErr, qt.IsNil)
			c.Assert(matches, qt.HasLen, 0)
		})
	}
}
