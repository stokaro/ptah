package schematests_test

import (
	"bytes"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas"
)

// TestSchemaExportRefusesWhatItCannotSelect pins the one UI-bound refusal left.
//
// --web and --export were both registered refusals. --export was implemented in
// stokaro/ptah#1620 and --web in stokaro/ptah#3011, so what remains here is the
// case where --export has nothing to select, which still has to refuse for the
// reason the whole flag once did: emitting the ordinary report would let an
// operator believe their exporter ran.
func TestSchemaExportRefusesWhatItCannotSelect(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(c.TempDir(), "ui-flags.db")
	createSQLiteSchemaCleanTable(c, dbPath, "users")

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--from", "sqlite://" + dbPath,
		"--to", "sqlite://" + dbPath,
		"--export",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNotNil)
	c.Assert(out.String(), qt.Contains, "an exporter is declared by an atlas.hcl `exporter` block")
}

// TestSchemaWebWritesAnArtifactAndSaysWhereItWent pins what replaced the
// refusal, on both verbs and on both spellings of the flag.
//
// CI is set for the whole test, so the run is one that cannot open a browser
// and the assertion is about the artifact rather than about this machine's
// desktop. That the suppression is honored on a machine that COULD open is
// pinned in internal/fileopen, where the environment is a parameter.
func TestSchemaWebWritesAnArtifactAndSaysWhereItWent(t *testing.T) {
	tests := []struct {
		name string
		args func(dbPath string) []string
	}{
		{
			name: "schema inspect --web",
			args: func(dbPath string) []string {
				return []string{"schema", "inspect", "--url", "sqlite://" + dbPath, "--web"}
			},
		},
		{
			name: "schema inspect -w",
			args: func(dbPath string) []string {
				return []string{"schema", "inspect", "--url", "sqlite://" + dbPath, "-w"}
			},
		},
		{
			name: "schema diff --web",
			args: func(dbPath string) []string {
				return []string{
					"schema", "diff",
					"--from", "sqlite://" + dbPath,
					"--to", "sqlite://" + dbPath,
					"--web",
				}
			},
		},
		{
			name: "schema diff -w",
			args: func(dbPath string) []string {
				return []string{
					"schema", "diff",
					"--from", "sqlite://" + dbPath,
					"--to", "sqlite://" + dbPath,
					"-w",
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv("CI", "true")
			dbPath := filepath.Join(c.TempDir(), "ui-flags.db")
			createSQLiteSchemaCleanTable(c, dbPath, "users")

			cmd := atlas.NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(test.args(dbPath))

			err := cmd.Execute()

			c.Assert(err, qt.IsNil)
			c.Assert(out.String(), qt.Contains, "Schema document written to ")
			c.Assert(out.String(), qt.Contains, "not opened: CI is set")
		})
	}
}

// TestSchemaUIFlagsUnpassedDoNotInterfere pins that registering the refusals
// leaves the ordinary paths alone.
func TestSchemaUIFlagsUnpassedDoNotInterfere(t *testing.T) {
	tests := []struct {
		name string
		args func(dbPath string) []string
		want string
	}{
		{
			name: "schema inspect without --web",
			args: func(dbPath string) []string {
				return []string{"schema", "inspect", "--url", "sqlite://" + dbPath}
			},
			want: `table "users"`,
		},
		{
			name: "schema diff without --export",
			args: func(dbPath string) []string {
				return []string{"schema", "diff", "--from", "sqlite://" + dbPath, "--to", "sqlite://" + dbPath}
			},
			want: "Schemas are synced",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbPath := filepath.Join(c.TempDir(), "ui-flags-clear.db")
			createSQLiteSchemaCleanTable(c, dbPath, "users")

			cmd := atlas.NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(test.args(dbPath))

			err := cmd.Execute()

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
			c.Assert(out.String(), qt.Contains, test.want)
		})
	}
}
