package atlas_test

import (
	"bytes"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

// TestSchemaUIFlagsAreRegisteredRefusals pins the decision taken on --web: it
// parses, and it refuses with a named reason.
//
// --export was here too until stokaro/ptah#1620 implemented it. What is left of
// it in this table is the case where there is nothing to select, which still
// has to refuse for the reason the whole flag once did.
//
// Reverted, both rows fail with `unknown flag`, which is the state that made a
// script passing the spelling unable to learn anything. Turned into a silent
// accept, both rows fail on the exit status instead — which is the outcome this
// pins against, because an accepted-and-ignored --export would report an export
// that never happened.
func TestSchemaUIFlagsAreRegisteredRefusals(t *testing.T) {
	tests := []struct {
		name string
		args func(dbPath string) []string
		want string
	}{
		{
			name: "schema inspect --web",
			args: func(dbPath string) []string {
				return []string{"schema", "inspect", "--url", "sqlite://" + dbPath, "--web"}
			},
			want: "atlas schema inspect accepts --web, but Ptah does not implement its behavior",
		},
		{
			name: "schema inspect -w",
			args: func(dbPath string) []string {
				return []string{"schema", "inspect", "--url", "sqlite://" + dbPath, "-w"}
			},
			want: "render it with --format '{{ mermaid . }}'",
		},
		// --export left this table in stokaro/ptah#1620: it is implemented, so
		// there is no refusal to word. Its own refusals -- the ones that keep
		// an export nobody declared from silently emitting the ordinary
		// report -- are covered by TestSchemaExportRefusesWhatItCannotResolve.
		{
			name: "schema diff --export with no project config",
			args: func(dbPath string) []string {
				return []string{
					"schema", "diff",
					"--from", "sqlite://" + dbPath,
					"--to", "sqlite://" + dbPath,
					"--export",
				}
			},
			want: "an exporter is declared by an atlas.hcl `exporter` block",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbPath := filepath.Join(c.TempDir(), "ui-flags.db")
			createSQLiteSchemaCleanTable(c, dbPath, "users")

			cmd := atlas.NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(test.args(dbPath))

			err := cmd.Execute()

			c.Assert(err, qt.IsNotNil)
			c.Assert(out.String(), qt.Contains, test.want)
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
