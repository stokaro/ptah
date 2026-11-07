//go:build integration

package providercli_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/providerhost"
)

func TestProviderReportsItsReleaseLines(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "devdb")
	c.Assert(host.ReleaseLines("fixture"), qt.DeepEquals, []providerhost.ReleaseLine{
		{Line: "2.0", Label: "Fixture 2", Support: capability.Certified, Image: "example.org/fixture:2.0"},
		{Line: "1.0", Support: capability.BestEffort, Image: "example.org/fixture:1.0", Unprobed: "the vendor ended support"},
	})
	c.Assert(host.ReleaseLines("other"), qt.HasLen, 0)
}

func TestProviderCheckListsReleaseLines(t *testing.T) {
	c := qt.New(t)
	f := newProviderCLI(c, migrationProviderCommandSpec(c, "devdb"))
	out, stderr, code := executeProviderCLI(c.Context(), []string{"providers", "check", "--provider-config", f.manifestPath, "--provider-engine", "fixture", "--json"})
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	var report struct {
		ReleaseLines []providerhost.ReleaseLine `json:"release_lines"`
	}
	c.Assert(json.Unmarshal([]byte(out), &report), qt.IsNil)
	c.Assert(report.ReleaseLines, qt.HasLen, 2)
	c.Assert(report.ReleaseLines[1].Unprobed, qt.Equals, "the vendor ended support")
}

// An engine linked into its provider certifies its line by the version it
// links, and the check reports the version as the line's evidence.
func TestProviderCheckListsALinkedReleaseLine(t *testing.T) {
	c := qt.New(t)
	spec := migrationProviderCommandSpec(c, "devdb")
	spec.Environment = append(spec.Environment, "FIXTURE_RELEASE_LINES=linked")
	f := newProviderCLI(c, spec)
	out, stderr, code := executeProviderCLI(c.Context(), []string{"providers", "check", "--provider-config", f.manifestPath, "--provider-engine", "fixture", "--json"})
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	var report struct {
		ReleaseLines []providerhost.ReleaseLine `json:"release_lines"`
	}
	c.Assert(json.Unmarshal([]byte(out), &report), qt.IsNil)
	c.Assert(report.ReleaseLines, qt.DeepEquals, []providerhost.ReleaseLine{
		{Line: "3.0", Support: capability.Certified, Linked: "3.0.1", Note: "The fixture's tests run the SQLite it links"},
		{Line: "2.0", Support: capability.BestEffort, Unprobed: "the fixture links 3.0"},
	})
}
