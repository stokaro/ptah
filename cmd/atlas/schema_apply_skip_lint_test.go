package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

// destructiveLintPolicy is an atlas.hcl that rates a destructive change as an
// error. It is the only thing that turns the lint pass on.
const destructiveLintPolicy = `lint {
  destructive {
    error = true
  }
}
`

// warningLintPolicy declares the same analyzer at warning severity, so the pass
// runs and finds the same statement without blocking.
const warningLintPolicy = `lint {
  destructive {
    error = false
  }
}
`

// schemaApplySkipLintFixture builds a working directory holding the optional
// atlas.hcl, a live SQLite database with one table, and a desired schema that
// drops it. Applying the desired state therefore plans a DROP TABLE, which is
// what the destructive analyzer reports.
func schemaApplySkipLintFixture(tb testing.TB, policy string) (dir, dbPath string) {
	c := qt.New(tb)
	c.Helper()
	dir = c.TempDir()
	dbPath = filepath.Join(dir, "target.db")
	createSQLiteSchemaCleanTable(c.TB, dbPath, "users")
	c.Assert(os.WriteFile(
		filepath.Join(dir, "schema.sql"),
		[]byte("CREATE TABLE keep (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	if policy != "" {
		c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(policy), 0o600), qt.IsNil)
	}
	return dir, dbPath
}

// runSchemaApplyInDir runs `atlas schema apply --dry-run` with dir as the
// working directory, so the optional atlas.hcl beside it is the one that loads.
func runSchemaApplyInDir(tb testing.TB, dir, dbPath string, extra ...string) (string, error) {
	c := qt.New(tb)
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	args := append([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "file://schema.sql",
		"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
		"--dry-run",
	}, extra...)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// TestSchemaApplySkipLint pins what the flag changes.
//
// Reverted, the --skip-lint rows fail with `unknown flag: --skip-lint` and the
// "policy refuses" row fails because nothing lints the plan, so a destructive
// apply that the project rated as an error succeeds silently.
func TestSchemaApplySkipLint(t *testing.T) {
	tests := []struct {
		name    string
		policy  string
		extra   []string
		wantErr bool
		want    string
		absent  string
	}{
		{
			name:    "error policy refuses the destructive plan",
			policy:  destructiveLintPolicy,
			wantErr: true,
			want:    "DS101",
		},
		{
			name:    "skip-lint applies the same plan",
			policy:  destructiveLintPolicy,
			extra:   []string{"--skip-lint"},
			wantErr: false,
			want:    `DROP TABLE IF EXISTS "users"`,
			absent:  "DS101",
		},
		{
			name:    "warning policy does not block",
			policy:  warningLintPolicy,
			wantErr: false,
			want:    `DROP TABLE IF EXISTS "users"`,
			absent:  "DS101",
		},
		{
			name:    "no policy is no lint pass",
			policy:  "",
			wantErr: false,
			want:    `DROP TABLE IF EXISTS "users"`,
			absent:  "DS101",
		},
		{
			name:    "skip-lint without a policy changes nothing",
			policy:  "",
			extra:   []string{"--skip-lint"},
			wantErr: false,
			want:    `DROP TABLE IF EXISTS "users"`,
			absent:  "DS101",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir, dbPath := schemaApplySkipLintFixture(c.TB, test.policy)
			// The atlas.hcl lookup is relative to the working directory, and
			// t.Chdir restores it and rejects a parallel test outright.
			t.Chdir(dir)

			out, err := runSchemaApplyInDir(c.TB, dir, dbPath, test.extra...)

			c.Assert(err != nil, qt.Equals, test.wantErr, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, test.want)
			c.Assert(schemaApplyOutputExcludes(out, test.absent), qt.IsTrue, qt.Commentf("%s", out))
		})
	}
}

// schemaApplyOutputExcludes reports whether out omits absent. An empty absent
// is vacuously satisfied, which keeps the table rows free of conditionals.
func schemaApplyOutputExcludes(out, absent string) bool {
	return absent == "" || !bytes.Contains([]byte(out), []byte(absent))
}
