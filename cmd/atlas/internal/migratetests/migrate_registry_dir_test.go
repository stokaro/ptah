//go:build !windows

package migratetests_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/cmd/atlas"
)

// TestMigrateVerbs_RegistryDirNeedsANamespace pins what `--dir atlas://…`
// answers with nothing to resolve against, on every verb that takes the flag.
//
// The refusal has to name PTAH_ATLAS_REGISTRY rather than the scheme: the
// reference is supported now, and what is missing is the namespace it stands
// for. A verb left out of the flag's resolution would answer "only local
// file:// migration directories are supported" instead, which is how this row
// tells the two apart (stokaro/ptah#1210).
func TestMigrateVerbs_RegistryDirNeedsANamespace(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "status", args: []string{"migrate", "status", "--dir", "atlas://app", "--url", "sqlite://x.db"}},
		{name: "apply", args: []string{"migrate", "apply", "--dir", "atlas://app", "--url", "sqlite://x.db"}},
		{name: "validate", args: []string{"migrate", "validate", "--dir", "atlas://app"}},
		{
			name: "lint",
			args: []string{
				"migrate", "lint", "--dir", "atlas://app",
				"--dev-url", "sqlite://dev?mode=memory", "--latest", "1",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv("PTAH_ATLAS_REGISTRY", "")
			cmd := atlas.NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(test.args)

			err := cmd.Execute()

			c.Assert(err, qt.ErrorMatches, `.*PTAH_ATLAS_REGISTRY.*`)
		})
	}
}

// TestMigrateVerbs_RegistryDirRefusesAWrite is the other half: a verb that
// writes into the directory cannot run against one that came from a registry,
// and the refusal names the reference the operator typed rather than the
// resolved OCI one or a temporary path.
//
// It fires before the namespace is needed, which is what this row also pins:
// the answer does not depend on a registry being reachable.
func TestMigrateVerbs_RegistryDirRefusesAWrite(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "hash", args: []string{"migrate", "hash", "--dir", "atlas://app"}},
		{name: "new", args: []string{"migrate", "new", "addcol", "--dir", "atlas://app"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv("PTAH_ATLAS_REGISTRY", "")
			cmd := atlas.NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(test.args)

			err := cmd.Execute()

			c.Assert(err, qt.ErrorMatches,
				`.*writes to the migration directory, and atlas://app came from a registry.*`)
		})
	}
}
