//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// TestSchemaApplyAnswersForAGrantOnADroppedViewE2E applies a source that stops
// declaring a view and still grants on it. The plan refuses it before anything
// runs, and selecting the view's schema changes none of that. Dropped from the
// selection, the grant leaves the plan: the view is gone, the run succeeds, and
// the next one says the schema is synced.
func TestSchemaApplyAnswersForAGrantOnADroppedViewE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	t.Cleanup(cancel)
	binaries := buildDiffSkipBinaries(c, ctx, e2eRepoRoot(t))

	const (
		current = "CREATE TABLE t (id integer PRIMARY KEY);\nCREATE VIEW v AS SELECT id FROM t;\n"
		viewV   = "SELECT count(*) FROM pg_views WHERE schemaname = 'public' AND viewname = 'v'"
	)
	for _, row := range []struct {
		name, grant string
		scope       []string
		want        string
	}{
		{name: "unscoped", grant: "v",
			want: "granting SELECT on view v to PUBLIC: the plan drops the view; declare it, or leave the grant out"},
		{name: "with its schema selected", grant: "v", scope: []string{"--schemas", "public"},
			want: "granting SELECT on view v to PUBLIC: the plan drops the view; declare it, or leave the grant out"},
		{name: "named with its schema", grant: "public.v", scope: []string{"--schemas", "public"},
			want: "the plan drops the view; declare it, or leave the grant out"},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			desired := "CREATE TABLE t (id integer PRIMARY KEY);\nGRANT SELECT ON " + row.grant + " TO PUBLIC;\n"
			c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(desired), 0o600), qt.IsNil)
			url := databaseBuiltFrom(c, current)
			args := append([]string{"schema", "apply", "--db-url", url, "--schema-file", "schema.sql", "--auto-approve"}, row.scope...)

			stdout, stderr, err := runCLIProcess(ctx, dir, binaries.native, args...)

			c.Assert(err, qt.IsNotNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stderr, qt.Contains, row.want)
			c.Assert(countIn(c, url, viewV), qt.Equals, 1)
		})
	}
}
