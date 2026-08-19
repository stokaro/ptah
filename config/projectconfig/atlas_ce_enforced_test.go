package projectconfig_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
)

// The three constructs covered here -- lint.condrop, diff.skip.drop_schema and
// env.schema.repo -- used to be refused outright: parsing any config that
// mentioned one exited 1 with `unsupported atlas.hcl construct`, even for
// commands that never consult the block. The pinned community binary accepts
// all three, so every assertion below prints an `unsupported atlas.hcl
// construct` error instead of its expected value if the parser arms are
// reverted.
//
// What the community binary was measured to DO with each is recorded next to
// the corresponding IR field: DiffSkipConfig.DropSchema and SchemaRepoConfig in
// config.go, and the condrop case in atlas.go.

func TestParseAtlasLintCondropSeverity(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		// want is the severity every code in codes must carry.
		want  string
		codes []string
		// absent lists rule selectors the block must not touch. Empty means the
		// row asserts only the positive side.
		absent []string
	}{
		{
			// condrop escalates the constraint-deletion family and nothing
			// else. The absent list is the negative control: on the pinned
			// community binary `destructive { error = true }` left the CD101
			// diagnostic a warning while `condrop { error = true }` made it an
			// error, so pointing condrop at DS or DD would be aiming a
			// constraint-drop policy at an unrelated check.
			name: "error true escalates the constraint family only",
			raw: `lint {
  condrop {
    error = true
  }
}
`,
			want:   "error",
			codes:  []string{"CD", "DS105"},
			absent: []string{"DS", "DD", "BC", "TX201", "PG101", "PG103"},
		},
		{
			name: "error false downgrades the constraint family only",
			raw: `lint {
  condrop {
    error = false
  }
}
`,
			want:   "warning",
			codes:  []string{"CD", "DS105"},
			absent: []string{"DS", "DD", "BC", "TX201", "PG101", "PG103"},
		},
		{
			// Same block inside env, because the parser reaches it by a
			// different path there: the env structure validator runs before the
			// lint parser and refused the name on its own.
			name: "env scoped block is decoded too",
			raw: `env "local" {
  url = "sqlite://s.db"
  lint {
    condrop {
      error = true
    }
  }
}
`,
			want:  "error",
			codes: []string{"CD", "DS105"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			cfg, err := projectconfig.ParseAtlas([]byte(tt.raw), "atlas.hcl", "local")

			c.Assert(err, qt.IsNil)
			for _, code := range tt.codes {
				c.Assert(cfg.Lint.RuleConfigs[code].Severity, qt.Equals, tt.want,
					qt.Commentf("severity for %q", code))
			}
			for _, code := range tt.absent {
				_, present := cfg.Lint.RuleConfigs[code]
				c.Assert(present, qt.IsFalse, qt.Commentf("selector %q must be untouched", code))
			}
		})
	}
}

// TestParseAtlasLintCondropRejectsNonBool pins the half of the contract that
// stays a refusal. The community binary decodes condrop's `error` as a bool and
// fails the run when it cannot -- measured under `migrate lint`, which is a
// command that reads the lint block:
//
//	lint { condrop { error = "x" } } -> exit 1, parsing datadepend check
//	                                    options: set field "error"
//
// so accepting a string here would be looser than the binary Ptah is matched
// against.
func TestParseAtlasLintCondropRejectsNonBool(t *testing.T) {
	c := qt.New(t)

	raw := `lint {
  condrop {
    error = "x"
  }
}
`

	_, err := projectconfig.ParseAtlas([]byte(raw), "atlas.hcl", "local")

	c.Assert(err, qt.ErrorMatches, `atlas\.hcl "error" at atlas\.hcl:3 must be a bool`)
}

func TestParseAtlasDiffSkipDropSchema(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want projectconfig.ConfigBool
	}{
		{
			name: "global true",
			raw: `diff {
  skip {
    drop_schema = true
  }
}
`,
			want: projectconfig.ConfigBool{Value: true, Set: true},
		},
		{
			// An explicit false is not the same as absence: the tri-state is
			// what lets an env override a global true back off.
			name: "global false is recorded as set",
			raw: `diff {
  skip {
    drop_schema = false
  }
}
`,
			want: projectconfig.ConfigBool{Value: false, Set: true},
		},
		{
			name: "env scoped",
			raw: `env "local" {
  url = "sqlite://s.db"
  diff {
    skip {
      drop_schema = true
    }
  }
}
`,
			want: projectconfig.ConfigBool{Value: true, Set: true},
		},
		{
			// The env value wins, which is the whole reason the field is a
			// tri-state rather than a bool.
			name: "env false overrides global true",
			raw: `diff {
  skip {
    drop_schema = true
  }
}

env "local" {
  url = "sqlite://s.db"
  diff {
    skip {
      drop_schema = false
    }
  }
}
`,
			want: projectconfig.ConfigBool{Value: false, Set: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			cfg, err := projectconfig.ParseAtlas([]byte(tt.raw), "atlas.hcl", "local")

			c.Assert(err, qt.IsNil)
			c.Assert(cfg.Diff.Skip.DropSchema, qt.Equals, tt.want)
			// Nothing new joins the planner vocabulary. Ptah's schema diff has
			// no removed-schema list and no code path renders DROP SCHEMA, so
			// there is no change kind for this suppression to omit -- see the
			// measurement recorded on DiffSkipConfig.DropSchema.
			c.Assert(cfg.Diff.SkipChangeKinds(), qt.HasLen, 0)
		})
	}
}

// TestParseAtlasDiffSkipDropSchemaRejectsNonBool mirrors the community binary's
// eager decode: `drop_schema = "x"` is refused there even by `schema inspect`,
// a command that never plans a diff, with `value of attr "drop_schema" cannot
// be read as bool`.
func TestParseAtlasDiffSkipDropSchemaRejectsNonBool(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{
			name: "global",
			raw: `diff {
  skip {
    drop_schema = "x"
  }
}
`,
		},
		{
			name: "env scoped",
			raw: `env "local" {
  url = "sqlite://s.db"
  diff {
    skip {
      drop_schema = "x"
    }
  }
}
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := projectconfig.ParseAtlas([]byte(tt.raw), "atlas.hcl", "local")

			c.Assert(err, qt.ErrorMatches, `atlas\.hcl "drop_schema" at atlas\.hcl:\d+ must be a bool`)
		})
	}
}

func TestParseAtlasSchemaRepo(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
		// wantSources is the schema src list the same block must still yield,
		// and wantIgnored the atlas.hcl names accepted and not acted on. Both
		// are asserted on every row, because the failure the repo arm risks is
		// shadowing a sibling attribute rather than misreading its own.
		wantSources []string
		wantIgnored []string
	}{
		{
			name: "name is decoded",
			raw: `env "local" {
  url = "sqlite://s.db"
  schema {
    repo {
      name = "myapp"
    }
  }
}
`,
			want:        "myapp",
			wantSources: nil,
			wantIgnored: make([]string, 0),
		},
		{
			// The community binary accepts an empty repo block -- measured at
			// exit 0 on `schema inspect` -- so an absent name is not an error.
			name: "empty block leaves the name empty",
			raw: `env "local" {
  url = "sqlite://s.db"
  schema {
    repo {
    }
  }
}
`,
			want:        "",
			wantSources: nil,
			wantIgnored: make([]string, 0),
		},
		{
			// src still parses beside it; adding the repo arm must not shadow
			// the attribute that shares the block.
			name: "coexists with schema src",
			raw: `env "local" {
  url = "sqlite://s.db"
  schema {
    src = "file://schema.hcl"
    repo {
      name = "myapp"
    }
  }
}
`,
			want:        "myapp",
			wantSources: []string{"file://schema.hcl"},
			wantIgnored: make([]string, 0),
		},
		{
			// An unrecognized attribute inside repo is tolerated, matching the
			// community binary's measured exit 0 for `repo { name = "myapp"
			// frobnicate = 1 }`.
			name: "unknown attribute inside repo is tolerated",
			raw: `env "local" {
  url = "sqlite://s.db"
  schema {
    repo {
      name       = "myapp"
      frobnicate = 1
    }
  }
}
`,
			want:        "myapp",
			wantSources: nil,
			wantIgnored: []string{"frobnicate"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			cfg, err := projectconfig.ParseAtlas([]byte(tt.raw), "atlas.hcl", "local")

			c.Assert(err, qt.IsNil)
			c.Assert(cfg.Schema.Repo.Name, qt.Equals, tt.want)
			c.Assert(cfg.SchemaSources, qt.DeepEquals, tt.wantSources)
			c.Assert(ignoredConstructNames(cfg), qt.DeepEquals, tt.wantIgnored)
		})
	}
}

// TestParseAtlasSchemaRepoRejectsNonString is the refusal half for repo. The
// community binary decodes the name eagerly -- `repo { name = 1 }` exits 1 on
// `schema inspect` with `value of attr "name" cannot be read as string` -- so
// accepting a number would be looser than the binary Ptah is matched against.
func TestParseAtlasSchemaRepoRejectsNonString(t *testing.T) {
	c := qt.New(t)

	raw := `env "local" {
  url = "sqlite://s.db"
  schema {
    repo {
      name = 1
    }
  }
}
`

	_, err := projectconfig.ParseAtlas([]byte(raw), "atlas.hcl", "local")

	c.Assert(err, qt.ErrorMatches, `atlas\.hcl "name" at atlas\.hcl:5 must be a string`)
}
