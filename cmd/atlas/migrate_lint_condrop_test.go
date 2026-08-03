package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/exitcode"
)

// atlasCondropProjectConfig gives one env per analyzer knob over the same
// migration directory, so a single fixture separates condrop from every
// neighbouring analyzer.
//
// On the pinned community binary, over a migration that drops a foreign key,
// the same separation was measured directly:
//
//	no lint block          -> exit 0, "1 version with warnings"
//	condrop     error=true -> exit 1, "1 version with errors"
//	destructive error=true -> exit 0, "1 version with warnings"
//
// condrop moves the constraint-deletion diagnostic and destructive does not.
// Ptah's default severity for its own CD101 is error rather than warning, so
// the knob is exercised in the other direction here -- error = false -- but the
// separation being pinned is the same one: condrop moves CD101, its neighbours
// leave it alone.
const atlasCondropProjectConfig = `env "baseline" {
  lint {
    latest = 1
  }
}

env "condrop_warn" {
  lint {
    latest = 1
    condrop {
      error = false
    }
  }
}

env "destructive_warn" {
  lint {
    latest = 1
    destructive {
      error = false
    }
  }
}

env "data_depend_warn" {
  lint {
    latest = 1
    data_depend {
      error = false
    }
  }
}
`

// TestCompatCommand_MigrateLintCondropSeverity is the end-to-end half of
// stokaro/ptah#1048's condrop arm.
//
// Three different mistakes are pinned, and they fail differently.
//
// Reverting the parser arm makes every row -- including the two that name no
// condrop block -- print `unsupported atlas.hcl construct "condrop" at
// atlas.hcl:10` and exit 1, because env structures are validated up front and
// all four envs share one file. Measured on the pre-change binary over this
// exact config; no row renders a lint report at all.
//
// Aiming condrop at the wrong family leaves CD101 an error, so the condrop row
// stays at exit 1 while the controls stay put. Measured by substituting DD for
// the constraint family: only "condrop downgrades the foreign-key drop" failed.
//
// Widening a neighbour to cover the constraint family instead drops that
// neighbour's control row to exit 0. Measured by adding CD to destructive: only
// "destructive leaves the foreign-key drop alone" failed. That is why the
// controls share this table rather than sitting in a separate test -- an exit
// code that moves on the wrong row is the only evidence that the selector is
// narrow enough.
func TestCompatCommand_MigrateLintCondropSeverity(t *testing.T) {
	tests := []struct {
		name    string
		env     string
		code    int
		summary string
	}{
		{
			// CD101 is an error by default, so the run fails without a policy.
			name:    "baseline reports the foreign-key drop as an error",
			env:     "baseline",
			code:    1,
			summary: "  -- 1 version with errors\n",
		},
		{
			// The discriminator: only this row's exit code and summary move.
			name:    "condrop downgrades the foreign-key drop",
			env:     "condrop_warn",
			code:    0,
			summary: "  -- 1 version with warnings\n",
		},
		{
			// Negative control. Atlas's destructive analyzer does not own the
			// constraint-deletion diagnostic -- measured -- so pointing condrop
			// at Ptah's destructive family would make this row move too.
			name:    "destructive leaves the foreign-key drop alone",
			env:     "destructive_warn",
			code:    1,
			summary: "  -- 1 version with errors\n",
		},
		{
			// Second negative control, aimed at the specific wrong turn
			// stokaro/ptah#1048 warned about: the community binary reports
			// condrop's decode failure under datadepend's option struct, which
			// made condrop look like an alias for data_depend. It is not.
			name:    "data_depend leaves the foreign-key drop alone",
			env:     "data_depend_warn",
			code:    1,
			summary: "  -- 1 version with errors\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			writeAtlasLintFile(c, filepath.Join(dir, "migrations"), "1.sql",
				"CREATE TABLE pets (id int PRIMARY KEY);\n")
			writeAtlasLintFile(c, filepath.Join(dir, "migrations"), "2.sql",
				"ALTER TABLE pets DROP FOREIGN KEY fk_pets_owner;\n")
			c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"),
				[]byte(atlasCondropProjectConfig), 0o600), qt.IsNil)
			t.Chdir(dir)

			stdout, _, err := runAtlasMigrateLint(c, "migrate", "lint", "--dir", "migrations", "--env", tt.env)

			c.Assert(exitcode.Code(err, 0), qt.Equals, tt.code)
			// The report body is identical in all four rows -- the diagnostic
			// is found either way -- so the summary line is the only byte-level
			// evidence of the severity, and asserting the whole report would
			// hide which half of it actually moved.
			c.Assert(stdout, qt.Contains, "-- L1 [CD101]: dropping a foreign key removes referential-integrity enforcement")
			c.Assert(stdout, qt.Contains, tt.summary)
		})
	}
}

// atlasDropSchemaProjectConfig sets the one diff.skip knob Ptah's planner has
// no statement to omit. See DiffSkipConfig.DropSchema for what the community
// binary does with it, and why accepting it cannot make Ptah emit a schema drop
// the binary would have withheld.
const atlasDropSchemaProjectConfig = `env "skip_drop_schema" {
  diff {
    skip {
      drop_schema = true
    }
  }
  lint {
    latest = 1
  }
}
`

// TestCompatCommand_MigrateLintRunsWithDropSchemaConfigured pins the divergence
// stokaro/ptah#1048 opened with: a config carrying diff.skip.drop_schema made
// ptah-compat exit 1 on commands that never consult a diff policy, while the
// community binary ran them normally.
//
// Reverting the parser arm makes this print
// `unsupported atlas.hcl construct "drop_schema" at atlas.hcl:4` and exit 1
// before any analysis happens, so the report assertion below never renders.
func TestCompatCommand_MigrateLintRunsWithDropSchemaConfigured(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeAtlasLintFile(c, filepath.Join(dir, "migrations"), "1.sql", "CREATE TABLE users (id int);\n")
	writeAtlasLintFile(c, filepath.Join(dir, "migrations"), "2.sql", "CREATE TABLE pets (id int);\n")
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"),
		[]byte(atlasDropSchemaProjectConfig), 0o600), qt.IsNil)
	t.Chdir(dir)

	stdout, _, err := runAtlasMigrateLint(c, "migrate", "lint", "--dir", "migrations", "--env", "skip_drop_schema")

	c.Assert(exitcode.Code(err, 0), qt.Equals, 0)
	c.Assert(redactAtlasLintDurations(stdout), qt.Equals,
		"Analyzing changes from version 1 to 2 (1 migration in total):\n"+
			"\n"+
			"  -- analyzing version 2\n"+
			"    -- no diagnostics found\n"+
			"  -- ok (DUR)\n"+
			"\n"+
			"  -------------------------\n"+
			"  -- DUR\n"+
			"  -- 1 version ok\n"+
			"  -- 1 schema change\n")
}

// atlasSchemaRepoProjectConfig names a schema repository. The community binary
// type-checks the name and then does nothing with it on every schema verb it
// has -- see SchemaRepoConfig for the runs that establish that.
const atlasSchemaRepoProjectConfig = `env "repo" {
  schema {
    repo {
      name = "myapp"
    }
  }
  lint {
    latest = 1
  }
}
`

// TestCompatCommand_MigrateLintRunsWithSchemaRepoConfigured is the schema.repo
// counterpart: reverting the parser arm makes this print
// `unsupported atlas.hcl construct "repo" at atlas.hcl:3` and exit 1 instead of
// the report asserted below.
func TestCompatCommand_MigrateLintRunsWithSchemaRepoConfigured(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeAtlasLintFile(c, filepath.Join(dir, "migrations"), "1.sql", "CREATE TABLE users (id int);\n")
	writeAtlasLintFile(c, filepath.Join(dir, "migrations"), "2.sql", "CREATE TABLE pets (id int);\n")
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"),
		[]byte(atlasSchemaRepoProjectConfig), 0o600), qt.IsNil)
	t.Chdir(dir)

	stdout, _, err := runAtlasMigrateLint(c, "migrate", "lint", "--dir", "migrations", "--env", "repo")

	c.Assert(exitcode.Code(err, 0), qt.Equals, 0)
	c.Assert(redactAtlasLintDurations(stdout), qt.Equals,
		"Analyzing changes from version 1 to 2 (1 migration in total):\n"+
			"\n"+
			"  -- analyzing version 2\n"+
			"    -- no diagnostics found\n"+
			"  -- ok (DUR)\n"+
			"\n"+
			"  -------------------------\n"+
			"  -- DUR\n"+
			"  -- 1 version ok\n"+
			"  -- 1 schema change\n")
}
