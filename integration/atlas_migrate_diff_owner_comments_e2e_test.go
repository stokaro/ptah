//go:build integration

package integration_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// ownerCommentsStep is one desired state, the comments its two files carry, and
// what the database holds once the forward file ran.
type ownerCommentsStep struct {
	schema   string
	wantUp   []string
	wantDown []string
	wantHeld string
}

// ownerCommentsFixture walks each owner's object through a creation, a change
// and a removal.
var ownerCommentsFixture = []ownerCommentsStep{
	{
		schema: `CREATE EXTENSION IF NOT EXISTS hstore;
CREATE DOMAIN posint AS integer CHECK (VALUE > 0);
COMMENT ON DOMAIN posint IS 'pos';
CREATE TYPE wrap AS (a int, b text);
CREATE TYPE span AS RANGE (subtype = int4);
CREATE TABLE t (id int PRIMARY KEY, a int);
ALTER TABLE t ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON t USING (a > 0);
COMMENT ON POLICY p ON t IS 'pp';
`,
		wantUp: []string{
			`create "hstore" extension`,
			`create "posint" domain`,
			`set comment to domain: "posint"`,
			`create "span" range type`,
			`create "wrap" composite type`,
			`create "t" table`,
			`create "p" policy`,
			`set comment to policy: "p"`,
		},
		wantDown: []string{
			`reverse: create "p" policy`,
			`reverse: create "t" table`,
			`reverse: create "posint" domain`,
			`reverse: create "wrap" composite type`,
			`reverse: create "span" range type`,
			`reverse: create "hstore" extension`,
		},
		wantHeld: "domain:posint extension:hstore policy:p range:span rowtype:wrap",
	},
	{
		schema: `CREATE DOMAIN posint AS integer CHECK (VALUE > 1);
CREATE TYPE wrap AS (a int, b text, c text);
CREATE TABLE t (id int PRIMARY KEY, a int);
ALTER TABLE t ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON t USING (a > 1);
`,
		wantUp: []string{
			`modify "posint" domain`,
			`modify "wrap" composite type`,
			`modify "p" policy`,
			`set comment to policy: "p"`,
			`set comment to domain: "posint"`,
			`drop "span" range type`,
			`drop "hstore" extension`,
		},
		wantDown: []string{
			`Recovery limit: "Restoring the previous row-security definition does not undo access the forward plan granted or withheld while it stood."`,
			`reverse: drop "hstore" extension`,
			`reverse: set comment to extension: "hstore"`,
			`reverse: drop "span" range type`,
			`reverse: modify "posint" domain`,
			`reverse: modify "wrap" composite type`,
			`reverse: modify "p" policy`,
			`reverse: set comment to policy: "p"`,
			`reverse: set comment to domain: "posint"`,
		},
		wantHeld: "domain:posint policy:p rowtype:wrap",
	},
	{
		schema: "CREATE TABLE t (id int PRIMARY KEY, a int);\n",
		wantUp: []string{
			`drop "p" policy`,
			`modify "t" table`,
			`drop "posint" domain`,
			`drop "wrap" composite type`,
		},
		wantDown: []string{
			`Recovery limit: "Restoring the previous row-security definition does not undo access the forward plan granted or withheld while it stood."`,
			`Recovery limit: "Restoring the previous row-security definition does not undo access the forward plan granted or withheld while it stood."`,
			`reverse: drop "posint" domain`,
			`reverse: drop "wrap" composite type`,
			`reverse: modify "t" table`,
			`reverse: drop "p" policy`,
		},
		wantHeld: "",
	},
}

// ownerObjectsHeld names the objects of the owners that public holds.
const ownerObjectsHeld = `SELECT coalesce(string_agg(object, ' ' ORDER BY object), '') FROM (
  SELECT 'extension:' || extname AS object FROM pg_extension WHERE extname <> 'plpgsql'
  UNION ALL SELECT 'policy:' || policyname FROM pg_policies WHERE schemaname = 'public'
  UNION ALL SELECT CASE typtype WHEN 'd' THEN 'domain:' WHEN 'r' THEN 'range:' ELSE 'rowtype:' END || typname
    FROM pg_type WHERE typnamespace = 'public'::regnamespace AND (typtype IN ('d', 'r') OR typname = 'wrap')
) AS objects`

// TestCompatMigrateDiffOwnerCommentsE2E_NameEachOperation diffs an extension, a
// domain, a composite type, a range type and a row-security policy through their
// life, holds both files of each step to the comments they carry, and runs the
// files on the server forward and back, reading the objects after each. What
// only describes the operation is gone: the warnings above a removal and the
// `Rollback of` lines. What the rollback cannot undo stays.
func TestCompatMigrateDiffOwnerCommentsE2E_NameEachOperation(t *testing.T) {
	c := qt.New(t)
	schemas := make([]string, 0, len(ownerCommentsFixture))
	for _, step := range ownerCommentsFixture {
		schemas = append(schemas, step.schema)
	}

	pairs := readMigrationPairs(c, diffStatementStyleSteps(c, exactSteps(schemas...)))
	target := statementStyleTarget(c)

	c.Assert(pairs, qt.HasLen, len(ownerCommentsFixture))
	for i, step := range ownerCommentsFixture {
		c.Assert(ownerFileComments(pairs[i].Up), qt.DeepEquals, step.wantUp, qt.Commentf("%s", pairs[i].Up))
		c.Assert(ownerFileComments(pairs[i].Down), qt.DeepEquals, step.wantDown, qt.Commentf("%s", pairs[i].Down))
		_, err := target.ExecContext(c.Context(), pairs[i].Up)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", pairs[i].Up))
		var held string
		c.Assert(target.QueryRowContext(c.Context(), ownerObjectsHeld).Scan(&held), qt.IsNil)
		c.Assert(held, qt.Equals, step.wantHeld)
	}
	for i := len(pairs) - 1; i > 0; i-- {
		_, err := target.ExecContext(c.Context(), pairs[i].Down)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", pairs[i].Down))
		var held string
		c.Assert(target.QueryRowContext(c.Context(), ownerObjectsHeld).Scan(&held), qt.IsNil)
		c.Assert(held, qt.Equals, ownerCommentsFixture[i-1].wantHeld)
	}
}

// ownerFileComments is the comment lines of a migration file, in order.
func ownerFileComments(file string) []string {
	comments := make([]string, 0, 8)
	for line := range strings.Lines(file) {
		if text, ok := strings.CutPrefix(line, "-- "); ok {
			comments = append(comments, strings.TrimSpace(text))
		}
	}
	return comments
}
