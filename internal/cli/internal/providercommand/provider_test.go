package providercommand_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/providercommand"
)

func TestProviderManifestRefusesInvalidConfigurationBeforeCommand(t *testing.T) {
	for _, test := range []struct{ name, document, want string }{
		{"unknown field", `{"format_version":1,"providers":[],"extra":true}`, "unknown field"},
		{"multiple documents", `{"format_version":1,"providers":[]} {}`, "one JSON document"},
		{"wrong version", `{"format_version":2,"providers":[{}]}`, "format_version 1"},
		{"missing providers", `{"format_version":1}`, "at least one provider"},
		{"oversize whitespace", `{"format_version":1,"providers":[]}` + strings.Repeat(" ", 1024*1024), "exceeds 1 MiB"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(c.TempDir(), "providers.json")
			c.Assert(os.WriteFile(path, []byte(test.document), 0o600), qt.IsNil)
			calls := 0
			cmd := providercommand.Configure(&cobra.Command{Use: "test", RunE: func(*cobra.Command, []string) error { calls++; return nil }})
			var diagnostics bytes.Buffer
			cmd.SetErr(&diagnostics)
			cmd.SetOut(&diagnostics)
			cmd.SetArgs([]string{"--provider-config", path})
			err := cmd.ExecuteContext(c.Context())
			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, test.want)
			c.Assert(calls, qt.Equals, 0)
		})
	}
}

func TestProviderSelectionRequiresAnExplicitFlag(t *testing.T) {
	c := qt.New(t)
	c.Setenv("PTAH_PROVIDER_CONFIG", "missing.json")
	calls := 0
	cmd := providercommand.Configure(&cobra.Command{Use: "test", RunE: func(*cobra.Command, []string) error { calls++; return nil }})
	cmd.SetArgs(nil)
	c.Assert(cmd.ExecuteContext(c.Context()), qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	cmd.SetArgs([]string{"--provider-config="})
	c.Assert(cmd.ExecuteContext(c.Context()), qt.IsNotNil)
	c.Assert(calls, qt.Equals, 1)
}
