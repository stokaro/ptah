//go:build integration

// The split/write half of the round trip for stokaro/ptah#1276.
//
// `schema inspect` has two HCL output modes. The single-document mode is what
// the boundary guard next door measures; this file measures the other one,
// which applies the SAME block suppression and, until the commit this file
// arrived with, wrote no coverage record anywhere:
//
//	inspect --format '{{ hcl . | split "schema" | write "out" }}'   exit 0
//	grep -rn ptah:not-described out                                 no match
//	schema apply --url <the same database> --to file://out/public.hcl --auto-approve
//	                                                                exit 0
//	pg_extension pgcrypto        1 -> 0
//	pg_class     order_seq       1 -> 0
//	pg_policy    p               1 -> 0
//
// measured on PostgreSQL 17.10. Splitting rebuilds every member from a parsed
// block and a leading comment belongs to no block, so the record was thrown
// away one pipe stage after it was written -- and the destructive round trip
// the single-document guard closes was open the whole time, one `--format`
// away.
//
// Every member is checked, not the first one: `write` puts the members on a
// filesystem and the next process is handed ONE of them by path, so a record
// carried by a sibling is a record that is not there.

package gonative_test

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasschema"
)

func TestPostgreSQLCoverageSurvivesSplitWriteIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)

	c := qt.New(t)
	dbURL := newBoundaryDatabase(c, dsn, boundaryCase{
		name: "coverage_split",
		seed: coverageRemovalSeed,
	})
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	wantRecord := coverage.Set{}.WithKind(coverage.Extension, coverage.Policy, coverage.Sequence)
	wantDrops := []string{
		`DROP POLICY IF EXISTS "p" ON "guarded"`,
		`DROP SEQUENCE IF EXISTS "order_seq"`,
		`DROP EXTENSION IF EXISTS "pgcrypto"`,
	}

	modes := []struct {
		name  string
		split string
	}{
		{name: "split by object, the default", split: `split`},
		{name: "split by schema", split: `split "schema"`},
		{name: "split by type", split: `split "type"`},
	}

	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			c := qt.New(t)
			members := coverageSplitInspect(c, dbURL, mode.split)
			c.Assert(len(members) > 0, qt.IsTrue)

			for _, member := range members {
				t.Run(filepath.Base(member), func(t *testing.T) {
					c := qt.New(t)
					document, readErr := os.ReadFile(member)
					c.Assert(readErr, qt.IsNil)

					declared, decodeErr := coverage.DecodeHeader(string(document))
					c.Assert(decodeErr, qt.IsNil)
					c.Assert(declared, qt.DeepEquals, wantRecord)

					// The member is applied back to the database it came from.
					// A member describes only part of the schema, so it can
					// legitimately plan to drop what a sibling holds; what it
					// must never plan is a drop of the three kinds it declared
					// it does not describe.
					planned := boundaryApplyBack(c, conn, string(document), true)
					for _, drop := range wantDrops {
						c.Assert(planned, qt.Not(qt.Contains), drop)
					}
				})
			}
		})
	}

	// The control. A member that reaches the comparator with its header deleted
	// is the edit an operator makes when they MEAN the omission, and it must
	// still plan every one of the three drops -- otherwise the assertions above
	// are satisfied by a comparator that can no longer remove anything, which is
	// the worse defect.
	t.Run("with the header removed, a split member drops all three", func(t *testing.T) {
		c := qt.New(t)
		members := coverageSplitInspect(c, dbURL, `split "schema"`)
		c.Assert(members, qt.HasLen, 1)
		document, readErr := os.ReadFile(members[0])
		c.Assert(readErr, qt.IsNil)

		c.Assert(coverageDirectiveLines(string(document)), qt.HasLen, 3)
		stripped := coverageStripDirectives(string(document))
		c.Assert(coverageDirectiveLines(stripped), qt.HasLen, 0)

		c.Assert(boundaryApplyBack(c, conn, stripped, true), qt.DeepEquals, wantDrops)
	})
}

// coverageSplitInspect runs `schema inspect` through the split/write export and
// returns the absolute path of every file it wrote, sorted.
func coverageSplitInspect(c *qt.C, dbURL, split string) []string {
	c.Helper()

	root := c.TempDir()
	_, err := atlasschema.InspectSource(c.Context(), atlasschema.InspectSourceOptions{
		URL:                    dbURL,
		Format:                 fmt.Sprintf(`{{ hcl . | %s | write %q }}`, split, root),
		Diagnostics:            nil,
		OmitAtlasRefusedBlocks: true,
		IgnoreUnknownHCLNames:  true,
	})
	c.Assert(err, qt.IsNil)

	var members []string
	c.Assert(filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() {
			members = append(members, path)
		}
		return nil
	}), qt.IsNil)
	slices.Sort(members)
	return members
}
