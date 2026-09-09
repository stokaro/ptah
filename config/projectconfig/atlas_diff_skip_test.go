package projectconfig_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
	"ptah.run/migration/diffpolicy"
)

// skipRow is one atlas.hcl body and the change kinds its diff policy must
// produce.
type skipRow struct {
	name string
	raw  string
	want []diffpolicy.ChangeKind
}

// TestParseAtlasDiffSkipReachesTheDiffPolicy_HappyPath pins that a diff.skip
// name Ptah's own policy models reaches that policy from atlas.hcl.
//
// `migration/diffpolicy` modeled drop_column and drop_index and `ptah.yaml`
// already reached both, while atlas.hcl type-checked them and sent them to the
// tolerance path. The compatibility surface therefore honored less of the same
// policy than the native spelling of it (stokaro/ptah#3111).
//
// The env-scoped and top-level spellings are both here because they are read
// through separate declarations, and only the top-level one was fixed first.
func TestParseAtlasDiffSkipReachesTheDiffPolicy_HappyPath(t *testing.T) {
	rows := []skipRow{
		{
			name: "top-level drop_column",
			raw: `diff {
  skip {
    drop_column = true
  }
}

env "local" {
  url = "sqlite://s.db"
}
`,
			want: []diffpolicy.ChangeKind{diffpolicy.DropColumn},
		},
		{
			name: "env-scoped drop_column",
			raw: `env "local" {
  url = "sqlite://s.db"
  diff {
    skip {
      drop_column = true
    }
  }
}
`,
			want: []diffpolicy.ChangeKind{diffpolicy.DropColumn},
		},
		{
			name: "env-scoped drop_index",
			raw: `env "local" {
  url = "sqlite://s.db"
  diff {
    skip {
      drop_index = true
    }
  }
}
`,
			want: []diffpolicy.ChangeKind{diffpolicy.DropIndex},
		},
		{
			name: "every modeled name together",
			raw: `env "local" {
  url = "sqlite://s.db"
  diff {
    skip {
      drop_table  = true
      drop_column = true
      drop_index  = true
    }
  }
}
`,
			want: []diffpolicy.ChangeKind{diffpolicy.DropTable, diffpolicy.DropColumn, diffpolicy.DropIndex},
		},
		{
			name: "false is decoded and skips nothing",
			raw: `env "local" {
  url = "sqlite://s.db"
  diff {
    skip {
      drop_column = false
    }
  }
}
`,
			want: nil,
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			cfg, err := projectconfig.ParseAtlas([]byte(row.raw), "atlas.hcl", "local")

			c.Assert(err, qt.IsNil)
			c.Assert(cfg.Diff.SkipChangeKinds(), qt.DeepEquals, row.want)
			// A decoded name must not also be reported as having no effect.
			// The two are declared separately -- parseDiffSkip decodes, the
			// structure table reports -- so a name can be acted on and called
			// useless in the same run, which is what the env-scoped spelling
			// did.
			c.Assert(ignoredAtlasNames(cfg), qt.HasLen, 0)
		})
	}
}

// TestParseAtlasDiffSkipReportsAnUnmodeledName_HappyPath is the control for the
// test above.
//
// The community binary decodes fifteen diff.skip names and Ptah's policy models
// a subset. A name outside that subset has to stay visible: reporting it is how
// a project author learns the setting does nothing here, and accepting it
// silently is the direction the compatibility policy forbids. Without this row,
// "decode every name" would satisfy every case above.
func TestParseAtlasDiffSkipReportsAnUnmodeledName_HappyPath(t *testing.T) {
	rows := []struct {
		name string
		attr string
	}{
		{name: "add_column", attr: "add_column"},
		{name: "modify_table", attr: "modify_table"},
		{name: "drop_foreign_key", attr: "drop_foreign_key"},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			raw := `env "local" {
  url = "sqlite://s.db"
  diff {
    skip {
      ` + row.attr + ` = true
    }
  }
}
`

			cfg, err := projectconfig.ParseAtlas([]byte(raw), "atlas.hcl", "local")

			c.Assert(err, qt.IsNil)
			c.Assert(cfg.Diff.SkipChangeKinds(), qt.HasLen, 0)
			c.Assert(ignoredAtlasNames(cfg), qt.Contains, row.attr)
		})
	}
}
