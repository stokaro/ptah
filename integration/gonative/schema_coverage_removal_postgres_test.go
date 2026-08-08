//go:build integration

// The genuine-removal control for stokaro/ptah#1276.
//
// The boundary guard next door asserts that applying a compatibility document
// back to the database it came from plans nothing. On its own that assertion is
// satisfied by a comparator that can no longer plan a removal at all, which is a
// worse defect than the destructive plan it replaced: an operator who deletes an
// extension from their desired state would get exit 0 and no change.
//
// So this file runs the pair. One document, one database, one difference: the
// three coverage directives in the document's header. With them the plan is
// empty; with them removed the same document against the same database drops
// all three objects. Nothing else about the two documents differs, which is what
// makes the pair discriminating rather than merely two measurements.

package gonative_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/dbschema"
)

// coverageRemovalSeed holds one object of each block type the compatibility
// surface omits, so the control covers every kind the header can name rather
// than the one that was reported.
var coverageRemovalSeed = []string{
	"CREATE EXTENSION pgcrypto",
	"CREATE SEQUENCE order_seq",
	"CREATE TABLE guarded (id integer PRIMARY KEY, owner text NOT NULL)",
	"ALTER TABLE guarded ENABLE ROW LEVEL SECURITY",
	"CREATE POLICY p ON guarded FOR SELECT USING (true)",
}

func TestPostgreSQLCoverageStillPlansAGenuineRemovalIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)

	c := qt.New(t)
	dbURL := newBoundaryDatabase(c, dsn, boundaryCase{
		name: "coverage_removal",
		seed: coverageRemovalSeed,
	})
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	inspected := boundaryInspect(c, dbURL, true)
	authored := coverageStripDirectives(inspected)

	c.Run("the document declares the three kinds it does not describe", func(c *qt.C) {
		declared, decodeErr := coverage.DecodeHeader(inspected)
		c.Assert(decodeErr, qt.IsNil)
		c.Assert(declared, qt.DeepEquals, coverage.Set{}.WithKind(
			coverage.Extension, coverage.Policy, coverage.Sequence,
		))
	})

	c.Run("as inspected, applying it back plans nothing", func(c *qt.C) {
		c.Assert(boundaryApplyBack(c, conn, inspected, true), qt.HasLen, 0)
	})

	c.Run("with the header removed, the same document drops all three", func(c *qt.C) {
		c.Assert(boundaryApplyBack(c, conn, authored, true), qt.DeepEquals, []string{
			`DROP POLICY IF EXISTS "p" ON "guarded"`,
			`DROP SEQUENCE IF EXISTS "order_seq"`,
			`DROP EXTENSION IF EXISTS "pgcrypto"`,
		})
	})

	c.Run("the two documents differ in the header alone", func(c *qt.C) {
		c.Assert(coverageDirectiveLines(inspected), qt.HasLen, 3)
		c.Assert(coverageDirectiveLines(authored), qt.HasLen, 0)
		c.Assert(coverageStripDirectives(inspected), qt.Equals, authored)
	})
}

// coverageStripDirectives is the edit an operator makes when they mean the
// omission: delete the line that says the document did not describe the thing.
func coverageStripDirectives(document string) string {
	var kept []string
	for line := range strings.SplitSeq(document, "\n") {
		if !strings.Contains(line, coverage.DirectiveMarker) {
			kept = append(kept, line)
		}
	}
	return strings.Join(kept, "\n")
}

func coverageDirectiveLines(document string) []string {
	var lines []string
	for line := range strings.SplitSeq(document, "\n") {
		if strings.Contains(line, coverage.DirectiveMarker) {
			lines = append(lines, line)
		}
	}
	return lines
}
