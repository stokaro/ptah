package atlas_test

import (
	"bytes"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/internal/atlascompatpolicy"
)

func TestStrictCECommandTreeExposesOnlyCommunityCommandsInHelp(t *testing.T) {
	root := atlas.NewCompatCommandWithPolicy("atlas", atlascompatpolicy.StrictCE())

	schema, _, err := root.Find([]string{"schema"})
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, availableChildNames(schema), qt.DeepEquals,
		[]string{"apply", "clean", "diff", "fmt", "inspect"})

	migrate, _, err := root.Find([]string{"migrate"})
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, availableChildNames(migrate), qt.DeepEquals,
		[]string{"apply", "diff", "hash", "import", "lint", "new", "set", "status", "validate"})
}

func TestFullCompatibilityCommandTreeRetainsExtensions(t *testing.T) {
	root := atlas.NewCompatCommand("atlas")

	plan, _, err := root.Find([]string{"schema", "plan"})
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, plan.Hidden, qt.IsFalse)

	down, _, err := root.Find([]string{"migrate", "down"})
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, down.Hidden, qt.IsFalse)
}

func TestStrictCEFlagSurfaceMatchesCommunityInventory(t *testing.T) {
	tests := []struct {
		path []string
		want []string
	}{
		{
			path: []string{"schema", "clean"},
			want: []string{"auto-approve", "dry-run", "format", "url"},
		},
		{
			path: []string{"schema", "diff"},
			want: []string{"dev-url", "exclude", "format", "from", "include", "schema", "to"},
		},
		{
			path: []string{"schema", "inspect"},
			want: []string{"dev-url", "exclude", "format", "schema", "url"},
		},
		{
			path: []string{"schema", "apply"},
			want: []string{
				"auto-approve", "dev-url", "dry-run", "edit", "exclude", "format",
				"include", "lock-timeout", "plan", "schema", "to", "tx-mode", "url",
			},
		},
		{
			path: []string{"migrate", "apply"},
			want: []string{
				"allow-dirty", "baseline", "dir", "dry-run", "exec-order", "format",
				"lock-timeout", "revisions-schema", "tx-mode", "url",
			},
		},
		{
			path: []string{"migrate", "diff"},
			want: []string{
				"dev-url", "dir", "dir-format", "edit", "format", "lock-timeout",
				"qualifier", "schema", "to",
			},
		},
		{
			path: []string{"migrate", "hash"},
			want: []string{"dir", "dir-format"},
		},
		{
			path: []string{"migrate", "import"},
			want: []string{"dir-format", "from", "to"},
		},
		{
			path: []string{"migrate", "lint"},
			want: []string{
				"dev-url", "dir", "dir-format", "format", "git-base", "git-dir", "latest",
			},
		},
		{
			path: []string{"migrate", "new"},
			want: []string{"dir", "dir-format", "edit"},
		},
		{
			path: []string{"migrate", "set"},
			want: []string{"dir", "dir-format", "revisions-schema", "url"},
		},
		{
			path: []string{"migrate", "status"},
			want: []string{"dir", "dir-format", "format", "revisions-schema", "url"},
		},
		{
			path: []string{"migrate", "validate"},
			want: []string{"dev-url", "dir", "dir-format"},
		},
		{
			path: []string{"schema", "fmt"},
			want: []string{},
		},
	}

	for _, test := range tests {
		t.Run(strings.Join(test.path, " "), func(t *testing.T) {
			root := atlas.NewCompatCommandWithPolicy("atlas", atlascompatpolicy.StrictCE())
			cmd, _, err := root.Find(test.path)

			qt.Assert(t, err, qt.IsNil)
			qt.Assert(t, visibleLocalFlagNames(cmd), qt.DeepEquals, test.want)
		})
	}
}

func TestFullCompatibilityFlagSurfaceRetainsExtensions(t *testing.T) {
	tests := []struct {
		path []string
		want []string
	}{
		{path: []string{"schema", "clean"}, want: []string{"exclude", "include"}},
		{path: []string{"schema", "diff"}, want: []string{"export"}},
		{path: []string{"schema", "inspect"}, want: []string{"include", "output", "web"}},
		{path: []string{"schema", "apply"}, want: []string{"lock-name", "skip-lint", "skip-lock"}},
		{path: []string{"migrate", "apply"}, want: []string{"lock-name", "skip-lock", "to-version"}},
	}

	for _, test := range tests {
		t.Run(strings.Join(test.path, " "), func(t *testing.T) {
			root := atlas.NewCompatCommand("atlas")
			cmd, _, err := root.Find(test.path)

			qt.Assert(t, err, qt.IsNil)
			for _, name := range test.want {
				qt.Assert(t, cmd.Flags().Lookup(name), qt.IsNotNil, qt.Commentf("flag %s", name))
			}
		})
	}
}

func TestStrictCEHelpOmitsPtahEnvironmentBindings(t *testing.T) {
	strictHelp := executeAtlasHelp(t,
		atlas.NewCompatCommandWithPolicy("atlas", atlascompatpolicy.StrictCE()),
		"schema", "inspect",
	)
	fullHelp := executeAtlasHelp(t, atlas.NewCompatCommand("atlas"), "schema", "inspect")

	qt.Assert(t, strictHelp, qt.Not(qt.Contains), "[env: PTAH_")
	qt.Assert(t, fullHelp, qt.Contains, "[env: PTAH_URL]")
}

func TestStrictCEHelpDoesNotAdvertiseOmittedExtensions(t *testing.T) {
	tests := []struct {
		path      []string
		fragments []string
	}{
		{path: []string{"schema", "clean"}, fragments: []string{"--include", "--exclude"}},
		{path: []string{"schema", "diff"}, fragments: []string{"--export"}},
		{path: []string{"schema", "inspect"}, fragments: []string{"--include", "--output", "--web"}},
		{path: []string{"schema", "apply"}, fragments: []string{"--lock-name", "--skip-lint", "--skip-lock"}},
	}

	for _, test := range tests {
		t.Run(strings.Join(test.path, " "), func(t *testing.T) {
			strictHelp := executeAtlasHelp(t,
				atlas.NewCompatCommandWithPolicy("atlas", atlascompatpolicy.StrictCE()),
				test.path...,
			)
			fullHelp := executeAtlasHelp(t, atlas.NewCompatCommand("atlas"), test.path...)

			for _, fragment := range test.fragments {
				qt.Assert(t, strictHelp, qt.Not(qt.Contains), fragment)
				qt.Assert(t, fullHelp, qt.Contains, fragment)
			}
		})
	}
}

func TestStrictCEGateUsesPtahDiagnostic(t *testing.T) {
	const help = `'ptah-compat schema plan' is unavailable while PTAH_ATLAS_STRICT_COMPAT is enabled.

Unset PTAH_ATLAS_STRICT_COMPAT to use Ptah's full compatibility surface.

`
	tests := []struct {
		name       string
		args       []string
		wantStdout string
		wantStderr string
		wantCode   int
	}{
		{
			name:       "execution aborts",
			args:       []string{"schema", "plan"},
			wantStderr: "Abort: " + help,
			wantCode:   1,
		},
		{
			name:       "help explains the gate",
			args:       []string{"schema", "plan", "--help"},
			wantStdout: help,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := atlas.NewCompatCommandWithPolicy("atlas", atlascompatpolicy.StrictCE())
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			root.SetOut(&stdout)
			root.SetErr(&stderr)
			root.SetArgs(test.args)

			_, err := root.ExecuteC()

			qt.Assert(t, exitcode.Code(err, 0), qt.Equals, test.wantCode)
			qt.Assert(t, stdout.String(), qt.Equals, test.wantStdout)
			qt.Assert(t, stderr.String(), qt.Equals, test.wantStderr)
		})
	}
}

func TestStrictCEGatedFlagsUsePtahDiagnostic(t *testing.T) {
	tests := []struct {
		name string
		args []string
		path string
	}{
		{
			name: "schema diff include",
			args: []string{
				"schema", "diff",
				"--from", "sqlite://from.db",
				"--to", "sqlite://to.db",
				"--include", "table.users",
			},
			path: "ptah-compat schema diff --include",
		},
		{
			name: "schema apply include",
			args: []string{
				"schema", "apply",
				"--url", "sqlite://target.db",
				"--to", "sqlite://desired.db",
				"--include", "table.users",
			},
			path: "ptah-compat schema apply --include",
		},
		{
			name: "schema clean dry run",
			args: []string{
				"schema", "clean",
				"--url", "sqlite://target.db",
				"--dry-run",
			},
			path: "ptah-compat schema clean --dry-run",
		},
		{
			name: "schema clean format",
			args: []string{
				"schema", "clean",
				"--url", "sqlite://target.db",
				"--format", "{{ sql . }}",
			},
			path: "ptah-compat schema clean --format",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := atlas.NewCompatCommandWithPolicy("atlas", atlascompatpolicy.StrictCE())
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			root.SetOut(&stdout)
			root.SetErr(&stderr)
			root.SetArgs(test.args)

			_, err := root.ExecuteC()

			qt.Assert(t, exitcode.Code(err, 0), qt.Equals, 1)
			qt.Assert(t, stdout.String(), qt.Equals, "")
			qt.Assert(t, stderr.String(), qt.Equals, atlasStrictCompatGateStderr(test.path))
		})
	}
}

func atlasStrictCompatGateStderr(path string) string {
	return "Abort: '" + path + `' is unavailable while PTAH_ATLAS_STRICT_COMPAT is enabled.

Unset PTAH_ATLAS_STRICT_COMPAT to use Ptah's full compatibility surface.

`
}

func TestStrictCEGateKeepsUnknownFlagParsing(t *testing.T) {
	root := atlas.NewCompatCommandWithPolicy("atlas", atlascompatpolicy.StrictCE())
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	root.SetOut(&stdout)
	root.SetErr(&stderr)
	root.SetArgs([]string{"migrate", "checkpoint", "--dir", "file://migrations"})

	_, err := root.ExecuteC()

	qt.Assert(t, exitcode.Code(err, 0), qt.Equals, 2)
	qt.Assert(t, stdout.String(), qt.Equals, "")
	qt.Assert(t, stderr.String(), qt.Equals, "Error: unknown flag: --dir\n")
}

func TestStrictCERejectsNonCommunityDialectBeforeCommandWork(t *testing.T) {
	root := atlas.NewCompatCommandWithPolicy("atlas", atlascompatpolicy.StrictCE())
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	root.SetOut(&stdout)
	root.SetErr(&stderr)
	root.SetArgs([]string{"schema", "inspect", "--url", "clickhouse://localhost/app"})

	_, err := root.ExecuteC()

	qt.Assert(t, exitcode.Code(err, 0), qt.Equals, 1)
	qt.Assert(t, stdout.String(), qt.Equals, "")
	qt.Assert(t, stderr.String(), qt.Equals,
		"Error: Atlas Community Edition strict compatibility does not support database dialect \"clickhouse\"\n")
}

func availableChildNames(parent *cobra.Command) []string {
	names := make([]string, 0, len(parent.Commands()))
	for _, child := range parent.Commands() {
		if !child.Hidden {
			names = append(names, child.Name())
		}
	}
	slices.Sort(names)
	return names
}

func visibleLocalFlagNames(cmd *cobra.Command) []string {
	names := []string{}
	cmd.LocalNonPersistentFlags().VisitAll(func(flag *pflag.Flag) {
		if !flag.Hidden && flag.Name != "help" {
			names = append(names, flag.Name)
		}
	})
	slices.Sort(names)
	return names
}

func executeAtlasHelp(t *testing.T, root *cobra.Command, path ...string) string {
	t.Helper()
	var stdout bytes.Buffer
	root.SetOut(&stdout)
	root.SetArgs(append(path, "--help"))

	_, err := root.ExecuteC()

	qt.Assert(t, err, qt.IsNil)
	return stdout.String()
}
