package projectconfig_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
)

// The go-yaml strict decoder reports failures against the Go type it decodes
// into. These tests hold every ptah.yaml diagnostic to the rule that it names
// something the user wrote, never a symbol from this package.

func chdirWith(tb testing.TB, files map[string]string) {
	c := qt.New(tb)
	dir := c.TempDir()
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	original, err := os.Getwd()
	c.Assert(err, qt.IsNil)
	c.Assert(os.Chdir(dir), qt.IsNil)
	c.Cleanup(func() {
		c.Assert(os.Chdir(original), qt.IsNil)
	})
}

// TestLoadExplicitConfigOverHCLBlamesTheFlag covers pointing --config at an
// atlas.hcl. The real problem is what --config accepts, not which YAML node
// kind the decoder found.
func TestLoadExplicitConfigOverHCLBlamesTheFlag(t *testing.T) {
	c := qt.New(t)
	chdirWith(c.TB, map[string]string{"atlas.hcl": "env \"local\" {\n  url = \"sqlite://file.db\"\n}\n"})

	_, err := projectconfig.Load(projectconfig.LoadOptions{PtahPath: "atlas.hcl", EnvName: "local"})

	c.Assert(err, qt.ErrorMatches, `--config takes a ptah\.yaml file, and atlas\.hcl is not a YAML mapping \(line 1: found !!str\); an Atlas project config is discovered as \./atlas\.hcl and selected with --env`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "projectconfig")
	c.Assert(err.Error(), qt.Not(qt.Contains), "unmarshal")
}

// TestLoadDiscoveredPtahFileDoesNotBlameTheFlag is the control for the case
// above: a conventional ./ptah.yaml that is not a mapping was not selected by
// --config, so the diagnostic must not blame it.
func TestLoadDiscoveredPtahFileDoesNotBlameTheFlag(t *testing.T) {
	c := qt.New(t)
	chdirWith(c.TB, map[string]string{projectconfig.PtahFileName: "- not-a-mapping\n"})

	_, err := projectconfig.Load(projectconfig.LoadOptions{})

	c.Assert(err, qt.ErrorMatches, `failed to parse ptah config ptah\.yaml: line 1: expected a YAML mapping of ptah\.yaml keys, found !!seq`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "--config")
	c.Assert(err.Error(), qt.Not(qt.Contains), "projectconfig")
}

// TestLoadExplicitValidConfigStillLoads is the positive control: the
// explicit-path branch above must not have made --config refuse real files.
func TestLoadExplicitValidConfigStillLoads(t *testing.T) {
	c := qt.New(t)
	chdirWith(c.TB, map[string]string{"custom.yaml": "url: \"sqlite://file.db\"\n"})

	cfg, err := projectconfig.Load(projectconfig.LoadOptions{PtahPath: "custom.yaml"})

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, "sqlite://file.db")
}

// TestParsePtahTypeMismatchNamesTheBlock covers a mismatch reported against a
// nested type rather than the document type.
func TestParsePtahTypeMismatchNamesTheBlock(t *testing.T) {
	c := qt.New(t)

	_, err := projectconfig.ParsePtah([]byte("migration: not-a-block\n"), "ptah.yaml", "")

	c.Assert(err, qt.ErrorMatches, `failed to parse ptah config ptah\.yaml: line 1: cannot read !!str as the ptah\.yaml migration block`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "projectconfig")
}

// TestParsePtahReportsEveryUnknownKey pins the multi-error rendering: a file
// with two mistakes reports both, each naming its own key and line.
func TestParsePtahReportsEveryUnknownKey(t *testing.T) {
	c := qt.New(t)

	_, err := projectconfig.ParsePtah([]byte("urll: a\nsrc: [b]\n"), "ptah.yaml", "")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `line 1: unknown ptah.yaml key "urll"`)
	c.Assert(err.Error(), qt.Contains, `line 2: unknown ptah.yaml key "src"`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "projectconfig")
}

// TestParsePtahSyntaxErrorSurvivesTranslation is the non-interference control:
// a plain YAML syntax error carries no Go type name and must reach the user
// unchanged rather than being rewritten into an unknown-key report.
func TestParsePtahSyntaxErrorSurvivesTranslation(t *testing.T) {
	c := qt.New(t)

	_, err := projectconfig.ParsePtah([]byte("url: \"unterminated\n"), "ptah.yaml", "")

	c.Assert(err, qt.ErrorMatches, `(?s)failed to parse ptah config ptah\.yaml: yaml: .*`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "unknown ptah.yaml key")
}
