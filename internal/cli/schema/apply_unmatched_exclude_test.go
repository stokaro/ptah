package schema_test

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/shadow"
)

// TestSchemaApplyUnmatchedExclude_FailurePath refuses an --exclude selector
// that names nothing, and the refusal is the error an embedder of
// shadow.PlanDynamicRollback matches: one error under errors.Is, whichever
// verb executes the derived plan.
func TestSchemaApplyUnmatchedExclude_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	dbPath := filepath.Join(dir, "apply.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte("CREATE TABLE apply_keep (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

	stdout, _, err := runSchemaStreams("apply",
		"--db-url", "sqlite://"+dbPath,
		"--schema-file", schemaPath,
		"--exclude", "nosuch_object",
		"--auto-approve",
	)

	c.Assert(err, qt.ErrorIs, shadow.ErrUnmatchedExclude)
	c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(`the --exclude selection matched no objects: "nosuch_object"; `+
		`schema apply refuses a selection that protects nothing, `+
		`set PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE=1 to keep the permissive behavior`))
	c.Assert(stdout, qt.Not(qt.Contains), "apply_keep")
}
