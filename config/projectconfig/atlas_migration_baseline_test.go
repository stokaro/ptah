package projectconfig_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
)

// TestParseAtlasMigrationBaseline pins the decode half of stokaro/ptah#934
// item 5a. The attribute reached [projectconfig.Config] through the
// unknown-name tolerance until now: it was type-checked, recorded as ignored,
// warned about, and dropped.
//
// Measured on the pinned Atlas community binary v1.3.0 with
// `migrate apply --env local --dry-run` against a hashed two-migration
// directory, exit codes read directly from unpiped invocations:
//
//	baseline = "20260719010000"  -> 0  "from 20260719010000 (1 migrations in
//	                                    total)"
//	no baseline                  -> 0  "(2 migrations in total)"  (control)
//	baseline = null              -> 0  "(2 migrations in total)"
//	baseline = ""                -> 0  "(2 migrations in total)"
//
// The null and empty rows are why the parse uses a null-tolerant helper: the
// eight names that go through the ordinary decoded-value path all refuse a
// null, which is exit 1 where this binary exits 0, and a name added after the
// fact should not inherit that.
func TestParseAtlasMigrationBaseline(t *testing.T) {
	tests := []struct {
		name         string
		raw          string
		wantBaseline string
		wantPresent  bool
	}{
		{
			name: "a version is decoded",
			raw: `env "local" {
  url = "sqlite://file.db"
  migration {
    dir      = "file://migrations"
    baseline = "20260719010000"
  }
}
`,
			wantBaseline: "20260719010000",
			wantPresent:  true,
		},
		{
			name: "an absent attribute leaves no baseline",
			raw: `env "local" {
  url = "sqlite://file.db"
  migration {
    dir = "file://migrations"
  }
}
`,
			wantBaseline: "",
			wantPresent:  false,
		},
		{
			// null is "no value given" on the pinned binary, not an error, and
			// not a present empty value either: a present empty value would
			// clear a ptah.yaml baseline that nothing asked to clear.
			name: "a null attribute leaves no baseline",
			raw: `env "local" {
  url = "sqlite://file.db"
  migration {
    dir      = "file://migrations"
    baseline = null
  }
}
`,
			wantBaseline: "",
			wantPresent:  false,
		},
		{
			// An empty string IS written, so it is present and empty. The
			// command reads an empty operand as "no baseline", which is what the
			// pinned binary does with it.
			name: "an empty string is present and selects nothing",
			raw: `env "local" {
  url = "sqlite://file.db"
  migration {
    dir      = "file://migrations"
    baseline = ""
  }
}
`,
			wantBaseline: "",
			wantPresent:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cfg, err := projectconfig.ParseAtlas([]byte(test.raw), "atlas.hcl", "local")

			c.Assert(err, qt.IsNil)
			c.Check(cfg.Migration.Baseline, qt.Equals, test.wantBaseline)
			value := cfg.StringValue(projectconfig.StringMigrationBaseline)
			c.Check(value.Value, qt.Equals, test.wantBaseline)
			c.Check(value.Present, qt.Equals, test.wantPresent)
			// A decoded attribute must not also be reported as having no
			// effect. The warning was the whole symptom of item 5a.
			c.Check(atlasIgnoredNames(cfg), qt.Not(qt.Contains), "baseline")
		})
	}
}

// TestParseAtlasMigrationBaselineFailurePath keeps the malformed arm at exit 1
// with the message the tolerance table produced before the name was decoded.
// Measured: `migration { baseline = [1,2] }` is exit 1 on the pinned binary with
// `value of attr "baseline" cannot be read as string: string value is required`.
func TestParseAtlasMigrationBaselineFailurePath(t *testing.T) {
	c := qt.New(t)
	raw := `env "local" {
  url = "sqlite://file.db"
  migration {
    dir      = "file://migrations"
    baseline = [1, 2]
  }
}
`

	_, err := projectconfig.ParseAtlas([]byte(raw), "atlas.hcl", "local")

	c.Assert(err, qt.ErrorMatches, `atlas.hcl "baseline" at atlas.hcl:5 must be a string`)
}

// atlasIgnoredNames lists the names a parse reported as accepted-and-ignored.
func atlasIgnoredNames(cfg projectconfig.Config) []string {
	names := make([]string, 0, len(cfg.IgnoredConstructs))
	for _, ignored := range cfg.IgnoredConstructs {
		names = append(names, ignored.Name)
	}
	return names
}
