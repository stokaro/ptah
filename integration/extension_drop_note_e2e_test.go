//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// extensionDropNote is the sentence a plan writes above DROP EXTENSION. The
// control and the refusal below measure each half of it on the server.
const extensionDropNote = "DROP EXTENSION removes the objects the extension owns, and the server refuses it while another object depends on them"

// extensionPresent counts the installed extension the rows below remove.
const extensionPresent = "SELECT count(*) FROM pg_extension WHERE extname = 'pg_trgm'"

// TestSchemaApplyExtensionDropE2E_HappyPath removes an extension nothing
// depends on. The server takes the objects the extension owns with it, and the
// next plan finds nothing to do.
func TestSchemaApplyExtensionDropE2E_HappyPath(t *testing.T) {
	c := qt.New(t)

	target, run := runExactApply(c,
		"CREATE EXTENSION pg_trgm;\nCREATE TABLE keep (id bigint PRIMARY KEY);\n",
		"CREATE TABLE keep (id bigint PRIMARY KEY);\n")

	c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	c.Assert(run.Stdout, qt.Contains, extensionDropNote)
	c.Assert(run.Stdout, qt.Contains, `DROP EXTENSION "pg_trgm";`)
	c.Assert(run.Stdout, qt.Not(qt.Contains), "cascade")
	c.Assert(readInt(c, target, extensionPresent), qt.Equals, 0)
	c.Assert(readInt(c, target, "SELECT count(*) FROM pg_proc WHERE proname = 'similarity'"), qt.Equals, 0)
}

// TestSchemaApplyExtensionDropE2E_ADependentStopsTheDrop holds the note to its
// second half: an index in a schema the run leaves out uses what the extension
// supplies, so the server refuses the drop and the index keeps working. A note
// that said the removal may cascade would have told the operator the opposite.
func TestSchemaApplyExtensionDropE2E_ADependentStopsTheDrop(t *testing.T) {
	c := qt.New(t)

	target, run := runExactApply(c,
		"CREATE EXTENSION pg_trgm;\nCREATE TABLE keep (id bigint PRIMARY KEY);\n"+
			"CREATE SCHEMA reporting;\nCREATE TABLE reporting.docs (id bigint PRIMARY KEY, body text);\n"+
			"CREATE INDEX docs_trgm ON reporting.docs USING gin (body gin_trgm_ops);\n",
		"CREATE TABLE keep (id bigint PRIMARY KEY);\n")

	c.Assert(run.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	c.Assert(run.Stdout, qt.Contains, extensionDropNote)
	c.Assert(run.Stderr, qt.Contains, "cannot drop extension pg_trgm because other objects depend on it")
	c.Assert(readInt(c, target, extensionPresent), qt.Equals, 1)
	c.Assert(readInt(c, target, "SELECT count(*) FROM pg_class WHERE relname = 'docs_trgm'"), qt.Equals, 1)
}
