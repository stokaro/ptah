package mcp_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/mcp"
	"ptah.run/internal/cli/root"
)

// missingProviderManifest writes a manifest whose one provider serves the
// fixture engine from an executable that does not exist. A provider this
// manifest selects fails to start, and says so, which is how the cases below
// tell a selection from none without starting a process.
func missingProviderManifest(c *qt.C) string {
	c.Helper()
	executable := filepath.Join(c.TempDir(), "missing-provider")
	manifest := `{"format_version":2,"providers":[{"executable":` + strconv.Quote(executable) + `,"sha256":"` + strings.Repeat("a", 64) +
		`","provider_id":"example.org/fixture","engines":[{"id":"fixture"}]}]}`
	path := filepath.Join(c.TempDir(), "providers.json")
	c.Assert(os.WriteFile(path, []byte(manifest), 0o600), qt.IsNil)
	return path
}

// runMCP runs `ptah mcp` with args and returns what it printed on stderr. Every
// case here fails before the transport opens, so nothing reads stdin.
func runMCP(c *qt.C, args ...string) (string, error) {
	c.Helper()
	cmd := mcp.NewCommand()
	var out, diagnostics bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&diagnostics)
	cmd.SetIn(strings.NewReader(""))
	cmd.SetArgs(args)
	err := cmd.ExecuteContext(c.Context())
	return diagnostics.String(), err
}

// The provider serves the configured database. With none configured, a
// provider flag would start a process no tool uses, so it is refused by name
// before anything is read or started.
func TestProviderFlags_FailurePath_NoDatabase(t *testing.T) {
	c := qt.New(t)
	manifest := missingProviderManifest(c)
	cases := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "a manifest",
			args: []string{"--provider-config", manifest},
			want: `--provider-config needs --database-url: a provider serves the configured database, and none is configured`,
		},
		{
			name: "an engine",
			args: []string{"--provider-engine", "fixture"},
			want: `--provider-engine needs --database-url: a provider serves the configured database, and none is configured`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			diagnostics, err := runMCP(c, tc.args...)
			c.Assert(err, qt.ErrorMatches, tc.want)
			c.Assert(diagnostics, qt.Contains, tc.want)
		})
	}
}

// The configured database is what selects the provider, under the flag that
// carried it. A fixture database selects the manifest's provider, which then
// fails to start because its executable is missing; a PostgreSQL database is
// not the fixture engine's, so an explicit fixture engine refuses it before
// anything starts and names --database-url.
func TestProviderFlags_FailurePath_TheDatabaseIsTheTarget(t *testing.T) {
	c := qt.New(t)
	manifest := missingProviderManifest(c)
	database := "fixture:///" + strings.TrimPrefix(filepath.ToSlash(filepath.Join(c.TempDir(), "database.db")), "/")
	cases := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "a database the manifest's engine serves starts its provider",
			args: []string{"--provider-config", manifest, "--database-url", database},
			want: `(?s)The configured provider executable was not found.*`,
		},
		{
			name: "a database of another engine is not the selected provider's",
			args: []string{"--provider-config", manifest, "--provider-engine", "fixture", "--database-url", "postgres://u@127.0.0.1:1/app"},
			want: `(?s).*The --database-url target is not owned by the selected provider engine.*`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := runMCP(c, tc.args...)
			c.Assert(err, qt.ErrorMatches, tc.want)
		})
	}
}

// A provider is the operator's to name on the command line. The flags bind no
// environment variable, as on every native command: a variable exported once
// in a client's configuration is not a decision about this binary. The
// database flag beside them does bind one, which is what shows the help would
// print the marker if a provider flag had it.
func TestProviderFlags_HappyPath_NoEnvironmentBinding(t *testing.T) {
	c := qt.New(t)
	cmd := root.NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)

	code := root.RunContext(c.Context(), cmd, "mcp", "--help")

	c.Assert(code, qt.Equals, 0)
	help := out.String()
	c.Assert(help, qt.Matches, `(?s).*--database-url[^\n]*\[env: PTAH_DATABASE_URL\].*`)
	c.Assert(help, qt.Matches, `(?s).*--provider-config[^\n]*\n.*`)
	c.Assert(help, qt.Matches, `(?s).*--provider-engine[^\n]*\n.*`)
	c.Assert(help, qt.Not(qt.Matches), `(?s).*--provider-(config|engine)[^\n]*\[env:.*`)
}
