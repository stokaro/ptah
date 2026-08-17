//go:build integration

package integration_test

import (
	"context"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/dbtarget"
)

// planE2ESearchPath returns dbURL carrying a `search_path` query parameter,
// which is how a dev URL names the one schema a desired-state file is loaded
// into.
func planE2ESearchPath(c *qt.C, dbURL, schema string) string {
	c.Helper()
	parsed, err := url.Parse(dbURL)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// TestAtlasSchemaPlanDevURLE2E covers stokaro/ptah#1635 against a live
// PostgreSQL: `schema plan` registered --dev-url, validated its dialect, and
// then handed it to nothing.
//
// The observable half of that is the schema scope. A dev URL carrying
// `search_path` says which schema an unqualified desired-state file belongs to,
// and a file declaring more than one schema block cannot be loaded into one --
// which is exactly the refusal `schema apply` gives for the same pair. Before
// this fix the plan path dropped the value on the floor, so the same pair was
// planned without complaint.
//
// The rows separate every operand:
//
//   - the refusal row is the finding, and it must name `dev-url` rather than
//     `url`, because the target URL carries no search_path and a message
//     naming it would mean the scope came from somewhere else;
//   - the no-dev-url row is the control that fails if the two-schema file
//     starts being refused on its own, which would make the first row pass for
//     the wrong reason;
//   - the single-schema row is the control that fails if the scope starts
//     refusing everything it is applied to.
func TestAtlasSchemaPlanDevURLE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)

	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	repoRoot := e2eRepoRoot(t)
	binaryPath := filepath.Join(t.TempDir(), "ptah-compat")
	buildPtahCompat(c, ctx, repoRoot, binaryPath)

	const twoSchemas = `schema "public" {
}

schema "app" {
}

table "users" {
  schema = schema.public
  column "id" {
    type = int
  }
}
`
	const oneSchema = `schema "public" {
}

table "users" {
  schema = schema.public
  column "id" {
    type = int
  }
}
`

	tests := []struct {
		name        string
		desired     string
		searchPath  string
		wantRefusal string
	}{
		{
			name:        "a dev URL limited to one schema refuses a two-schema file",
			desired:     twoSchemas,
			searchPath:  "public",
			wantRefusal: `is limited to schema "public"`,
		},
		{
			name:    "the same file is planned when no dev URL limits it",
			desired: twoSchemas,
		},
		{
			name:       "a one-schema file is planned under the same limit",
			desired:    oneSchema,
			searchPath: "public",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			work := t.TempDir()
			desiredPath := filepath.Join(work, "desired.hcl")
			c.Assert(os.WriteFile(desiredPath, []byte(test.desired), 0o600), qt.IsNil)

			args := []string{
				"schema", "plan",
				"--from", dbURL,
				"--to", "file://" + desiredPath,
				"--name", "p",
				"--auto-approve",
				"--dry-run",
			}
			args = append(args, planE2EDevURLArgs(c, dbURL, test.searchPath)...)

			cmd := exec.CommandContext(ctx, binaryPath, args...)
			cmd.Dir = work
			output, err := cmd.CombinedOutput()

			c.Assert(string(output), qt.Contains, test.wantRefusal)
			// The arithmetic half: qt.Contains with "" passes on any output, so
			// without this the two control rows would assert nothing.
			c.Assert(err != nil, qt.Equals, test.wantRefusal != "",
				qt.Commentf("output:\n%s", string(output)))
		})
	}
}

// planE2EDevURLArgs returns the --dev-url pair, or nothing when the row is the
// control that passes no dev URL at all.
func planE2EDevURLArgs(c *qt.C, dbURL, searchPath string) []string {
	c.Helper()
	if searchPath == "" {
		return nil
	}
	return []string{"--dev-url", planE2ESearchPath(c, dbURL, searchPath)}
}
